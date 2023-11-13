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

def get_sitemap(url):
    """Scrapes an XML sitemap from the provided URL and returns XML source.

    Args:
        url (string): Fully qualified URL pointing to XML sitemap.

    Returns:
        xml (string): XML source of scraped sitemap.
    """

    response = urllib.request.urlopen(url)
    xml = BeautifulSoup(response,
                         'lxml-xml',
                         from_encoding=response.info().get_param('charset'))

    return xml

def get_sitemap_type(xml):
    """Parse XML source and returns the type of sitemap.

    Args:
        xml (string): Source code of XML sitemap.

    Returns:
        sitemap_type (string): Type of sitemap (sitemap, sitemapindex, or None).
    """

    sitemapindex = xml.find_all('sitemapindex')
    sitemap = xml.find_all('urlset')

    if sitemapindex:
        return 'sitemapindex'
    elif sitemap:
        return 'urlset'
    else:
        return

def get_child_sitemaps(xml):
    """Return a list of child sitemaps present in a XML sitemap file.

    Args:
        xml (string): XML source of sitemap.

    Returns:
        sitemaps (list): Python list of XML sitemap URLs.
    """

    sitemaps = xml.find_all("sitemap")

    output = []

    for sitemap in sitemaps:
        output.append(sitemap.findNext("loc").text)
    return output

url = "https://www.booking.com/sitembk-hotel-index.xml"
xml = get_sitemap(url)

sitemap_type = get_sitemap_type(xml)

child_sitemaps = get_child_sitemaps(xml)

hotel_sitemap = adv.sitemap_to_df("https://www.booking.com/sitembk-hotel-index.xml")

url_df = adv.url_to_df(hotel_sitemap['sitemap'])

df = (hotel_sitemap[hotel_sitemap['sitemap']
 .str.contains('/sitembk-hotel-en-gb.')])
col_list = df.sitemap.values.tolist()

df_urls = pd.DataFrame()
# df_result = pd.DataFrame()
list_nl_hotels = []

for x in col_list:
  urls = pd.read_xml(x)
  df_urls = df_urls.append(urls)
  url_list = df_urls['loc'].tolist()

#print (url_list[0:10])

for url in url_list:
    fullstring = url
    substring = '/ae/'
    if substring in fullstring:
        list_nl_hotels.append(fullstring)

print(list_nl_hotels)
print(len(list_nl_hotels))
list_nl_hotels_done = list_nl_hotels[0:0]
res = [i for i in list_nl_hotels if i not in list_nl_hotels_done]
print(len(res))

for y in res:
    url = y
    response = requests.get(url, headers=HEADERS, proxies=proxy)
    l = list()
    g = list()
    o = {}
    k = {}
    fac = []
    fac_arr = []

    soup = BeautifulSoup(response.text, 'html.parser')


    #o["name"] = soup.find("h2", {"class": "pp-header__title"}).text
    #o["address"] = soup.find("span", {"class": "hp_address_subtitle"}).text.strip("\n")
    #o["rating"] = soup.find("div", {"class": "d10a6220b4"}).text



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
                #"title": soup.find("h2", {"class": "pp-header__title"}).text,
                #"description": css("div#property_description_content ::text", "\n"),
                #"address": css(".hp_address_subtitle::text"),
                #"lat": lat,
                #"lng": lng,
                #"features": dict(features),
                #"id": re.findall(r"b_hotel_id:\s*'(.+?)'", html)[0],
                #"rating": soup.find("div", {"class": "d10a6220b4"}).text,

                "title": soup.find("h2",{"class":"pp-header__title"}).text,
                "description": css("div#property_description_content ::text", "\n"),
                "address": css(".hp_address_subtitle::text"),
                "lat": lat,
                "lng": lng,
                "features": dict(features),
                "id": re.findall(r"b_hotel_id:\s*'(.+?)'", html)[0],
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
            df_json.to_csv('file_uae1.csv', mode='a', index=False, header=False)

            #df_json = pd.read_json(StringIO(json_hotel_object))
            #df_result = pd.concat([df_result, df_json])
            #print(df_result)
            #df_result.to_csv('file.csv')

            # print(json.dumps(hotels, indent=2))
            # with open("sample.json", "w") as outfile:
            #    outfile.write(json_hotel_object)Starting pgAdmin 4...

    # print(json.dumps(hotels, indent=2))



    if __name__ == "__main__":
        #df_res = pd.DataFrame()
        asyncio.run(run())
