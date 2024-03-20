import pandas as pd
from sqlalchemy import create_engine
import psycopg2


def transform(df_book_products):

    df_book_products["category_id"] = df_book_products["category_id"]
    df_fact = df_book_products[
        [
            "product_id",
            "category_id",
            "sku",
            "quantity_sold",
            "price",
            "original_price",
            "discount",
            "discount_rate",
        ]
    ]
    df_fact = df_fact.drop_duplicates()
    return df_fact


def main():

    alchemyEngine = create_engine(
        "postgresql+psycopg2://hoangson:11111@localhost/airflow"
    )
    dbConnect = alchemyEngine.connect()
    conn = psycopg2.connect(
        database="airflow",
        user="hoangson",
        password="11111",
        host="localhost",
        port=5432,
    )
    cur = conn.cursor()
    df_product = transform(pd.read_sql("select * from PRODUCT_DATA_DATA", dbConnect))
    print(df_product)
    df_product.to_sql("fact_table", alchemyEngine, if_exists="append", index=False)

    conn.commit()
    conn.close()
    cur.close()


if __name__ == "__main__":
    main()
