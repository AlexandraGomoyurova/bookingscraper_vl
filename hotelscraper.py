import asyncio
from collections import defaultdict
import json

import re
from typing import List, Optional

from parsel import Selector
from httpx import AsyncClient
from bs4 import BeautifulSoup

import requests

# Set up your Bright Data API credentials
api_key = "cce451bb-80e4-4ca0-b07a-d7eb6ba3409c"
proxy_server = "zproxy.lum-superproxy.io"
proxy_port = 22225

# Set up the URL of the website you want to scrape
url = "https://www.booking.com/hotel/nl/gttjaarda.html"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.94 Safari/537.36",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
    "Connection": "keep-alive",
    "Accept-Language": "en-US,en;q=0.9,lt;q=0.8,et;q=0.7,de;q=0.6",
}

# Set up the proxy for your request
proxy = {
    "http": f"http://brd-customer-hl_35d316d4-zone-zone1:t880fs1ka4in@zproxy.lum-superproxy.io:22225",
    "https": f"http://brd-customer-hl_35d316d4-zone-zone1:t880fs1ka4in@zproxy.lum-superproxy.io:22225",
}

# Send your request using the Bright Data API
response = requests.get(url, headers=HEADERS, proxies=proxy)


#print(response.text)

l = list()
g = list()
o = {}
k = {}
fac = []
fac_arr = []


soup = BeautifulSoup(response.text, 'html.parser')

o["name"] = soup.find("h2", {"class": "pp-header__title"}).text
o["address"] = soup.find("span", {"class": "hp_address_subtitle"}).text.strip("\n")
#o["rating"] = soup.find("div", {"class": "d10a6220b4"}).text


def parse_hotel(html: str):
    sel = Selector(response.text)
    css = lambda selector, sep="": sep.join(sel.css(selector).getall()).strip()
    css_first = lambda selector: sel.css(selector).get("")
    # get latitude and longitude of the hotel address:
    lat, lng = css_first(".show_map_hp_link::attr(data-atlas-latlng)").split(",")
    # get hotel features by type
    features = defaultdict(list)
    for feat_box in sel.css("[data-capla-component*=PropertyFacilitiesBlock]>div>div>div"):
        type_ = feat_box.xpath('.//span[contains(@data-testid, "facility-group-icon")]/../text()').get()
        feats = [f.strip() for f in feat_box.css("li ::text").getall() if f.strip()]
        features[type_] = feats
    data = {
        "title": soup.find("h2", {"class": "pp-header__title"}).text,
        "description": css("div#property_description_content ::text", "\n"),
        "address": css(".hp_address_subtitle::text"),
        "lat": lat,
        "lng": lng,
        "features": dict(features),
        "id": re.findall(r"b_hotel_id:\s*'(.+?)'", html)[0],
        "rating": soup.find("div", {"class": "d10a6220b4"}).text,
    }
    #print(data)

    return data

async def scrape_hotels(urls: List[str], session: AsyncClient):
    async def scrape_hotel(url: str):
        resp = await session.get(url)
        hotel = parse_hotel(resp.text)
        hotel["url"] = str(resp.url)
        return hotel

    hotels = await asyncio.gather(*[scrape_hotel(url) for url in urls])
    return hotels

async def run():
    async with AsyncClient(headers=HEADERS) as session:
        hotels = await scrape_hotels([url], session)
        json_hotel_object = json.dumps(hotels, indent=2)
        print(json.dumps(hotels, indent=2))
        with open("sample.json", "w") as outfile:
            outfile.write(json_hotel_object)

if __name__ == "__main__":
    asyncio.run(run())