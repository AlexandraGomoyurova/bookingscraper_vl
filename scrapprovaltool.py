import pandas as pd
import urllib.request
from urllib.parse import urlparse
from bs4 import BeautifulSoup
import advertools as adv

import asyncio
from collections import defaultdict
import json

import re
from typing import List, Optional

from parsel import Selector
from httpx import AsyncClient
from bs4 import BeautifulSoup

import requests
from io import StringIO

import psycopg2

df_result = pd.DataFrame()

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

conn = psycopg2.connect(
        host="payment-dev.cluster-calqwlcvf9fo.eu-west-1.rds.amazonaws.com",
        port="5432",
        database="approval_tool",
        user="approval_tool",
        password="6XnhYFSUjkU8KAJAaPW4Q6Zk49K6CurnCeUv7Twu"
    )

# Create a cursor object to execute SQL queries
cursor = conn.cursor()

# Execute a query to retrieve the desired column
cursor.execute('SELECT url_b FROM vl_hotel_link')

# Fetch all the rows from the result set
rows = cursor.fetchall()

# Extract the column values as a list
column_values = [row[0] for row in rows]

# Close the cursor and database connection
cursor.close()
conn.close()

# Display the column values
print(column_values)


for y in column_values:
    url = y
    response = requests.get(url, headers=HEADERS, proxies=proxy)
    l = list()
    g = list()
    o = {}
    k = {}
    fac = []
    fac_arr = []

    soup = BeautifulSoup(response.text, 'html.parser')

    value1 = url
    conn = psycopg2.connect(
        host="payment-dev.cluster-calqwlcvf9fo.eu-west-1.rds.amazonaws.com",
        port="5432",
        database="approval_tool",
        user="approval_tool",
        password="6XnhYFSUjkU8KAJAaPW4Q6Zk49K6CurnCeUv7Twu"
    )

    cursor = conn.cursor()

    cursor.execute('SELECT external_id FROM vl_hotel_link WHERE url_b = %s', (value1,))

    row = cursor.fetchone()
    # Extract the value
    if row:
        external_id = row[0]
    else:
        print('No matching value found.')

    # Close the cursor and database connection
    cursor.close()
    conn.close()



    def parse_hotel(html: str):
        sel = Selector(response.text)
        css = lambda selector, sep="": sep.join(sel.css(selector).getall()).strip()
        css_first = lambda selector: sel.css(selector).get("")
        # get latitude and longitude of the hotel address:
        try:
            lat, lng = css_first(".show_map_hp_link::attr(data-atlas-latlng)").split(",")
        except:
            val = css_first(".show_map_hp_link::attr(data-atlas-latlng)").split(",")
        # get hotel features by type
        features = defaultdict(list)
        try:
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
                "external_id": external_id,
            }
        except AttributeError:
            for feat_box in sel.css("[data-capla-component*=PropertyFacilitiesBlock]>div>div>div"):
                type_ = feat_box.xpath('.//span[contains(@data-testid, "facility-group-icon")]/../text()').get()
                feats = [f.strip() for f in feat_box.css("li ::text").getall() if f.strip()]
                features[type_] = feats
            data = {
                "title": "nan",
                "description": css("div#property_description_content ::text", "\n"),
                "address": css(".hp_address_subtitle::text"),
                "lat": "nan",
                "lng": "nan",
                "features": "nan",
                "id": "nan",
                "rating": "nan",
                "external_id": external_id,
            }
        # print(data)

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
        # df_result = pd.DataFrame()
        async with AsyncClient(headers=HEADERS) as session:
            hotels = await scrape_hotels([url], session)
            json_hotel_object = json.dumps(hotels, indent=2)
            # print(json.dumps(hotels, indent=2))
            print(json_hotel_object)
            df_json = pd.read_json(StringIO(json_hotel_object))
            #df_result2 = pd.concat([df_result2, df_json])
            #print(df_result2)
            df_json.to_csv('file_approval1.csv', mode='a', index=False, header=False)




    if __name__ == "__main__":
        #df_res = pd.DataFrame()
        asyncio.run(run())
