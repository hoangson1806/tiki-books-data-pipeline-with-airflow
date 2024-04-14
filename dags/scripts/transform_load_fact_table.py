import pandas as pd
from sqlalchemy import create_engine
import psycopg2
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas


def transform(df_book_products):

    df_fact = df_book_products[
        [
            "product_id",
            "categories_id",
            "sku",
            "quantity_sold",
            "price",
            "orginal_price",
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
    conn = snowflake.connector.connect(
        user="HOANGSONSNOWFLAKE",
        account="mjmpxrl-ai52284",
        password="Hoangson123@#",
        warehouse="COMPUTE_WH",
        database="MY_DB",
        schema="PUBLIC",
    )

    cur = conn.cursor()
    df_product = transform(pd.read_sql("select * from PRODUCT_DATA_INFO", dbConnect))
    print(df_product)
    write_pandas(conn, df_product, table_name="FACT_TABLE", quote_identifiers=False)

    conn.commit()
    conn.close()
    cur.close()


if __name__ == "__main__":
    main()
