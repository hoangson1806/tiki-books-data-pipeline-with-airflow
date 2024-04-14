import requests
import pandas as pd
from sqlalchemy import create_engine
import psycopg2
import csv
import time
import random

cookies = {
    "_trackity": "9669e6a9-153e-a40a-5d5b-58fdc7a8689b",
    "TOKENS": "{%22access_token%22:%22JfBAO6thqnc3j4xuFKdPEWUSMT91GyHR%22}",
    "tiki_client_id": "",
    "delivery_zone": "Vk4wMzkwMDYwMDE=",
}

url = "https://tiki.vn/api/v2/products/{}"

headers = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:122.0) Gecko/20100101 Firefox/122.0",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-GB,en;q=0.5",
    "x-guest-token": "JfBAO6thqnc3j4xuFKdPEWUSMT91GyHR",
    "TE": "trailers",
}
params = (("platform", "web"), ("spid", 205576404))


def get_attr_value(attributes, field):
    ls = [attr.get("value") for attr in attributes if attr.get("code") == field]
    return ls[0] if len(ls) > 0 else None


def parser_product(json):
    d = dict()
    product_id = json.get("id")
    sku = json.get("sku")
    name = json.get("name")

    price = json.get("price")
    original_price = json.get("original_price")

    discount = json.get("discount")
    discount_rate = json.get("discount_rate")

    quantity_sold = (
        json.get("quantity_sold").get("value")
        if json.get("quantity_sold") != None
        else 0
    )

    author = json.get("authors")
    author = author[0].get("name") if author != None else None

    categories = json.get("breadcrumbs")[2].get("name")
    categories_id = json.get("breadcrumbs")[2].get("category_id")

    attributes = json.get("specifications")[0].get("attributes")

    publisher = get_attr_value(attributes, "publisher_vn")
    manufacturer = get_attr_value(attributes, "manufacturer")

    record = (
        product_id,
        sku,
        name,
        price,
        original_price,
        discount,
        discount_rate,
        quantity_sold,
        author,
        categories,
        categories_id,
        publisher,
        manufacturer,
    )

    return record


drop_table = """DROP TABLE IF EXISTS  product_data_info;"""
create_data_table = """CREATE TABLE product_data_info(
                        id bigserial NOT NULL,
                        product_id character varying NOT NULL,
                        sku character varying NOT NULL,
                        name character varying NOT NULL,
                        price double precision NOT NULL,
                        orginal_price double precision NOT NULL,
                        discount double precision NOT NULL,
                        discount_rate double precision,
                        quantity_sold character varying,
                        author character varying,
                        categories_name character varying,
                        categories_id integer, 
                        publisher character varying,
                        manufacturer character varying,
                        PRIMARY KEY (id)

);"""
insert_data_toTable = """INSERT INTO product_data_info values(DEFAULT,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ;"""

product_data = []
fail_id = []

df_id = pd.read_csv("product_id.csv")
p_ids = df_id.id.to_list()


def main():
    alchemyEngine = create_engine(
        "postgresql+psycopg2://hoangson:11111@localhost/airflow"
    )
    dbConnection = alchemyEngine.connect()

    conn = psycopg2.connect(
        database="airflow",
        user="hoangson",
        password="11111",
        host="localhost",
        port="5432",
    )
    cur = conn.cursor()
    cur.execute(drop_table)
    cur.execute(create_data_table)
    book_id = pd.read_sql(" SELECT * FROM product_id_table", dbConnection)
    id_list = book_id.id.to_list()
    print(len(id_list))

    id_list = list(dict.fromkeys(id_list))  # remove duplicate product_id
    print("the second: ", len(id_list))
    while len(id_list) > 0:
        for id in id_list:
            response = requests.get(
                url=url.format(id), headers=headers, params=params, cookies=cookies
            )
            if response.status_code == 200:
                try:
                    prod = parser_product(response.json())
                    cur.execute(insert_data_toTable, prod)
                    print(prod)
                    id_list.remove(id)
                except Exception as e:
                    print(e)

    time.sleep(random.randrange(1, 3))

    conn.commit()
    conn.close()
    cur.close()


if __name__ == "__main__":
    main()
