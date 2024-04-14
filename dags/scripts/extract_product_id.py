import requests
import pandas as pd
import requests
from sqlalchemy import create_engine
import psycopg2
import csv
import time
import random


url = "https://tiki.vn/api/personalish/v1/blocks/listings"
headers = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "en-US,en;q=0.5",
    "Connection": "keep-alive",
    "Host": "tiki.vn",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "cross-site",
    "Upgrade-Insecure-Requests": "1",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:122.0) Gecko/20100101 Firefox/122.0",
}
params = {
    "limit": "40",
    "include": "advertisement",
    "aggregations": "2",
    "version": "home-persionalized",
    "trackity_id": "f6aad9cb-889e-bc89-2280-f76da33082dd",
    "category": "316",
    "page": "1",
    "urlKey": "sach-truyen-tieng-viet",
}
last_page = (
    requests.get(url, headers=headers, params=params)
    .json()
    .get("paging")
    .get("last_page")
)

product_id = []


def main():
    for i in range(1, 3 + 1):
        params["page"] = i
        print(i)
        response = requests.get(url, headers=headers, params=params)
        for record in response.json().get("data"):
            name = record.get("name")
            if "combo" in name.lower():
                continue
            product_id.append({"id": record.get("id")})

    df = pd.DataFrame(product_id)
    df.to_csv("product_id.csv", index=False)


if __name__ == "__main__":
    main()
