import pandas as pd
from sqlalchemy import create_engine
import psycopg2


def transform(df_product):

    df_product["author"] = df_product["author"].map(
        lambda x: str(x or "Unknown").strip().title()
    )
    df_product["publisher"] = df_product["publisher"].map(
        lambda x: str(x or "Unknown").strip().title()
    )
    df_product["manufacturer"] = df_product["manufacturer"].map(
        lambda x: str(x or "Unknown").strip().title()
    )

    df_book = df_product[["product_id", "name", "author", "publisher", "manufacturer"]]
    return df_book


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
    df_product.to_sql("dim_book_table", alchemyEngine, if_exists="append", index=False)

    conn.commit()
    conn.close()
    cur.close()


if __name__ == "__main__":
    main()
