import asyncio
from collections import defaultdict
import json
from pathlib import Path
import re
from typing import List, Optional
from urllib.parse import urlencode
#from scrapfly import ScrapflyClient, ScrapeConfig, ScrapeApiResponse
from parsel import Selector
from httpx import AsyncClient
from datetime import date

#scrapfly = ScrapflyClient(key="YOUR SCRAPFLY KEY")

today_date = date.today()

async def request_hotels_page(
    query,
    session: AsyncClient,
    checkin: str = "",
    checkout: str = "",
    number_of_rooms=1,
    offset: int = 0,
):
    """scrapes a single hotel search page of booking.com"""
    checkin_year, checking_month, checking_day = checkin.split("-") if checkin else "", "", ""
    checkout_year, checkout_month, checkout_day = checkout.split("-") if checkout else "", "", ""

    url = "https://www.booking.com/searchresults.html"
    url += "?" + urlencode(
        {
            "ss": query,
            "checkin_year": checkin_year,
            "checkin_month": checking_month,
            "checkin_monthday": checking_day,
            "checkout_year": checkout_year,
            "checkout_month": checkout_month,
            "checkout_monthday": checkout_day,
            "no_rooms": number_of_rooms,
            "offset": offset,
        }
    )
    return await session.get(url, follow_redirects=True)

# Example use:
# first we need to immitate web browser headers to not get blocked instantly
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.94 Safari/537.36",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
    "Connection": "keep-alive",
    "Accept-Language": "en-US,en;q=0.9,lt;q=0.8,et;q=0.7,de;q=0.6",
}


def parse_search_total_results(html: str):
    sel = Selector(text=html)
    # parse total amount of pages from heading1 text:
    # e.g. "London: 1,232 properties found"
    total_results = int(sel.css("h1").re("([\d,]+) properties found")[0].replace(",", ""))
    return total_results


def parse_search_hotels(html: str):
    sel = Selector(text=html)

    hotel_previews = {}
    for hotel_box in sel.xpath('//div[@data-testid="property-card"]'):
        url = hotel_box.xpath('.//h3/a[@data-testid="title-link"]/@href').get("").split("?")[0]
        hotel_previews[url] = {
            "name": hotel_box.xpath('.//h3/a[@data-testid="title-link"]/div/text()').get(""),
            "location": hotel_box.xpath('.//span[@data-testid="address"]/text()').get(""),
            "score": hotel_box.xpath('.//div[@data-testid="review-score"]/div/text()').get(""),
            "review_count": hotel_box.xpath('.//div[@data-testid="review-score"]/div[2]/div[2]/text()').get(""),
            "stars": len(hotel_box.xpath('.//div[@data-testid="rating-stars"]/span').getall()),
            "image": hotel_box.xpath('.//img[@data-testid="image"]/@src').get(),
        }
    return hotel_previews


async def scrape_search(
    query,
    checkin: str = "",
    checkout: str = "",
    number_of_rooms=1,
    max_results: Optional[int] = None,
):
    first_page = await request_hotels_page(
        query=query, checkin=checkin, checkout=checkout, number_of_rooms=number_of_rooms
    )
    hotel_previews = parse_search_hotels(first_page.content)
    total_results = parse_search_total_results(first_page.content)
    if max_results and total_results > max_results:
        total_results = max_results
    other_pages = await asyncio.gather(
        *[
            request_hotels_page(
                query=query,
                checkin=checkin,
                checkout=checkout,
                number_of_rooms=number_of_rooms,
                offset=offset,
            )
            for offset in range(25, total_results, 25)
        ]
    )
    for result in other_pages:
        hotel_previews.update(parse_search_hotels(result.content))
    return hotel_previews


def parse_hotel(html: str):
    sel = Selector(text=html)
    css = lambda selector, sep="": sep.join(sel.css(selector).getall()).strip()
    css_first = lambda selector: sel.css(selector).get("")
    lat, lng = css_first(".show_map_hp_link::attr(data-atlas-latlng)").split(",")
    features = defaultdict(list)
    for feat_box in sel.css("[data-capla-component*=PropertyFacilitiesBlock]>div>div>div"):
        type_ = feat_box.xpath('.//span[contains(@data-testid, "facility-group-icon")]/../text()').get()
        feats = [f.strip() for f in feat_box.css("li ::text").getall() if f.strip()]
        features[type_] = feats
    data = {
        "title": css("h2#hp_hotel_name::text"),
        "description": css("div#property_description_content ::text", "\n"),
        "address": css(".hp_address_subtitle::text"),
        "lat": lat,
        "lng": lng,
        "features": dict(features),
        "id": re.findall(r"b_hotel_id:\s*'(.+?)'", html)[0],
    }
    return data


async def scrape_hotels(urls: List[str], session: AsyncClient, price_start_dt: str, price_n_days=60):
    async def scrape_hotel(url: str):
        resp = await session.get(url)
        hotel = parse_hotel(resp.text)
        hotel["url"] = str(resp.url)
        # for background requests we need to find cross-site-reference token
        csrf_token = re.findall(r"b_csrf_token:\s*'(.+?)'", resp.text)[0]
        hotel['price'] = await scrape_prices(csrf_token=csrf_token, hotel_id=hotel['id'])
        return hotel

    async def scrape_prices(hotel_id, csrf_token):
        data = {
            "name": "hotel.availability_calendar",
            "result_format": "price_histogram",
            "hotel_id": hotel_id,
            "search_config": json.dumps({
                # we can adjust pricing configuration here but this is the default
                "b_adults_total": 2,
                "b_nr_rooms_needed": 1,
                "b_children_total": 0,
                "b_children_ages_total": [],
                "b_is_group_search": 0,
                "b_pets_total": 0,
                "b_rooms": [{"b_adults": 2, "b_room_order": 1}],
            }),
            "checkin": price_start_dt,
            "n_days": price_n_days,
            "respect_min_los_restriction": 1,
            "los": 1,
        }
        resp = await session.post(
            "https://www.booking.com/fragment.json?cur_currency=eur",
            headers={**session.headers, "X-Booking-CSRF": csrf_token},
            data=data,
        )
        return resp.json()["data"]

    hotels = await asyncio.gather(*[scrape_hotel(url) for url in urls])
    return hotels

async def run():
    async with AsyncClient(headers=HEADERS) as session:
        hotels = await scrape_hotels(["https://www.booking.com/hotel/nl/gttjaarda.html"], session, " " )
        print(json.dumps(hotels, indent=2))

if __name__ == "__main__":
    asyncio.run(run())