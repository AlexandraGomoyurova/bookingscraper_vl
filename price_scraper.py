import asyncio
from collections import defaultdict
import json

import re
from typing import List, Optional

from pandas import read_csv
from parsel import Selector
from httpx import AsyncClient
from datetime import date
import pandas as pd

from bs4 import BeautifulSoup
import requests

import psycopg2

from sqlalchemy import *
from sqlalchemy.engine import create_engine

def convertTuple(tup):
    str = ''.join(tup)
    return str

#conn_string = 'postgresql://approval_tool:6XnhYFSUjkU8KAJAaPW4Q6Zk49K6CurnCeUv7Twu@payment-dev.cluster-calqwlcvf9fo.eu-west-1.rds.amazonaws.com/approval_tool'

#conn_string = 'postgresql://readonly:Dbn856XLc5ssRw8jBKZbnTUBRe5nVN22@pyllow-core-prod-instance-1-eu-west-1b.calqwlcvf9fo.eu-west-1.rds.amazonaws.com/postgres'


#db = create_engine(conn_string)
#conn = db.connect()



today_date = date.today()
today_date = today_date.strftime("%Y-%m-%d")

connection = psycopg2.connect(
                                host="pyllow-core-prod-instance-1-eu-west-1b.calqwlcvf9fo.eu-west-1.rds.amazonaws.com",
                                port="5432",
                                database="pyllow_production",
                                user="readonly",
                                password="Dbn856XLc5ssRw8jBKZbnTUBRe5nVN22"
)
cursor = connection.cursor()

postgreSQL_select_Query = "select admin_id, booking_com_url from hotels"
#postgreSQL_select_Query = "select url_b from vl_hotel_link"

cursor.execute(postgreSQL_select_Query)

booking_links_db = cursor.fetchall()
print(type(booking_links_db))
print(booking_links_db)

#for url in booking_links_db:
#    print((type(url)))
#   for suburl in url:
#       print(type(suburl))

first_element = [x[1] for x in booking_links_db]
print("Get first element in list of tuples:", first_element)
print(type(first_element))
for i in first_element:
    print(type(i))



#data = read_csv("booking_links.csv")

#links = data['url_b'].tolist()

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



connection = psycopg2.connect(
                                host="payment-dev.cluster-calqwlcvf9fo.eu-west-1.rds.amazonaws.com",
                                port="5432",
                                database="postgres",
                                user="approval_tool",
                                password="6XnhYFSUjkU8KAJAaPW4Q6Zk49K6CurnCeUv7Twu"
)
def parse_hotel(html: str):
    sel = Selector(text=html)
    css = lambda selector, sep="": sep.join(sel.css(selector).getall()).strip()
    css_first = lambda selector: sel.css(selector).get("")
    a = css_first(".show_map_hp_link::attr(data-atlas-latlng)").split(",")
    lat, lng = css_first(".show_map_hp_link::attr(data-atlas-latlng)").split(",")
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
                    # "b_children_total": 0,
                    # "b_children_ages_total": [],
                    # "b_is_group_search": 0,
                    # "b_pets_total": 0,
                    "b_rooms": [{"b_adults": 2, "b_room_order": 1}],
                }),
                "checkin": price_start_dt,
                "n_days": price_n_days,
                # "respect_min_los_restriction": 1,
                # "los": 1,
            }
            resp = await session.post(
                "https://www.booking.com/fragment.json?cur_currency=eur",
                headers={**session.headers, "X-Booking-CSRF": csrf_token},
                data=data,
            )
            return resp.json()["data"]

        hotels = await asyncio.gather(*[scrape_hotel(url) for url in urls])
        return hotels


for h in booking_links_db:
    print(h)
    url_one = str(h[1])
    print(url_one)
    #for suburl in url_one:
    try:
        url = convertTuple(url_one)
        print(url)
        response = requests.get(url, headers=HEADERS, proxies=proxy)

        #print(response.text)

        soup = BeautifulSoup(response.text, 'html.parser')



        async def run():
            print('1')
            async with AsyncClient(headers=HEADERS) as session:
                hotels = await scrape_hotels([url], session, today_date)
                first_elem = hotels[0]
                employee_data = first_elem['price']
                daysdata = employee_data['days']
                id_el = first_elem['id']
                title_el = first_elem['title']
                rows = []
                for each in employee_data['days']:
                    each.pop("b_length_of_stay")
                    each.pop("b_avg_price_pretty")
                    each.pop("b_avg_price_raw")
                    each.pop("b_min_length_of_stay")
                    each.pop("b_price_pretty")
                    each.pop("b_available")
                    rows.append(each)
                #print(rows)
                df = pd.DataFrame(rows)
                df['id'] = id_el
                df['title'] = title_el
                df['added_date'] = today_date
                df['admin_id'] = str(h[0])
                df = df[['checkin', 'b_price', 'title', 'id', 'added_date', 'admin_id']]
                print(df)
                #df.to_csv('file_prices4.csv', mode='a', index=False, header=False)

                conn_string = 'postgresql://approval_tool:6XnhYFSUjkU8KAJAaPW4Q6Zk49K6CurnCeUv7Twu@payment-dev.cluster-calqwlcvf9fo.eu-west-1.rds.amazonaws.com/approval_tool'

                db = create_engine(conn_string)
                conn = db.connect()

                df.to_sql('pricing_booking_2', con=conn, if_exists='append',
                          index=False)
                conn = psycopg2.connect(conn_string)
                conn.autocommit = True
                cursor = conn.cursor()

                sql1 = 'select * from pricing_booking_2;'
                cursor.execute(sql1)
                for i in cursor.fetchall():
                    print(i)

                # conn.commit()
                conn.close()


        if __name__ == "__main__":
            asyncio.run(run())

    except (ValueError, AttributeError, TypeError) as e:
        print("An exception occurred:", e)
        pass