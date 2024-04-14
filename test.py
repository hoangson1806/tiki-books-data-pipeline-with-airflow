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
drop_table = """DROP TABLE IF EXISTS PRODUCT_ID_TABLE2;"""
create_product_id_table = """
        CREATE TABLE PRODUCT_ID_TABLE2(ID CHARACTER VARYING);
                """
insert_id = """ INSERT INTO PRODUCT_ID_TABLE2 VALUES(%s); """


def main():
    alchemyEngine = create_engine(
        "postgresql+psycopg2://hoangson:11111@localhost/airflow"
    )
    conn = psycopg2.connect(
        database="airflow",
        user="hoangson",
        password="11111",
        host="localhost",
        port="5432",
    )
    cur = conn.cursor()
    cur.execute(create_product_id_table)
    for i in range(1, 1 + 1):
        params["page"] = i
        print(i)
        response = requests.get(url, headers=headers, params=params)
        for record in response.json().get("data"):
            name = record.get("name")

            if "combo" in name.lower():
                continue
            id = record.get("id")
            product_id.append({"id": id})
            cur.execute(insert_id % id)
    conn.commit()
    conn.close()
    cur.close()

    df = pd.DataFrame(product_id)
    print(df)


if __name__ == "__main__":
    main()
