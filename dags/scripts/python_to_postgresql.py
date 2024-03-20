import psycopg2
import requests
import time
import random
import boto3


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


def main():

    res = requests.get(url, headers=headers, params=params)
    last_page = res.json()["paging"]["last_page"]
    conn = psycopg2.connect(
        database="airflow",
        user="hoangson",
        password="11111",
        host="localhost",
        port="5432",
    )
    cur = conn.cursor()
    drop_table = """DROP TABLE IF EXISTS product;"""
    cur.execute(drop_table)
    create_product_id_table = """
    CREATE TABLE IF NOT EXISTS product(
        id  SERIAL NOT NULL PRIMARY KEY,
        product_id int not null
    );"""
    cur.execute(create_product_id_table)
    insert_into_id_table = """INSERT INTO product (product_id) VALUES(%s)"""
    count = 0
    for i in range(1, last_page + 1):
        params["page"] = i
        print(i)
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 200:
            try:
                for record in response.json().get("data"):
                    name = record.get("name")
                    if "combo" in name.lower():
                        continue
                    cur.execute(insert_into_id_table, [record["id"]])
            except Exception as e:
                print(e)
                print("ERRORS HAPPEN")
        time.sleep(random.randrange(1, 2))

    print(count)

    conn.commit()
    conn.close()
    cur.close()


if __name__ == "__main__":
    main()
