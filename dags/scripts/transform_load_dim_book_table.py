import pandas as pd
from sqlalchemy import create_engine
import psycopg2
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas


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
    conn = snowflake.connector.connect(
        user="HOANGSONSNOWFLAKE",
        password="Hoangson123@#",
        account="mjmpxrl-ai52284",
        warehouse="COMPUTE_WH",
        database="MY_DB",
        schema="PUBLIC",
    )
    cur = conn.cursor()

    df_product = pd.read_sql("select * from PRODUCT_DATA_INFO", dbConnect)
    print(df_product)
    print("after")
    print(transform(df_product))
    t = transform(df_product)
    write_pandas(conn, t, table_name="DIM_BOOK_TABLE", quote_identifiers=False)
    # df_product.to_sql("dim_book_table", alchemyEngine, if_exists="append", index=False)

    conn.commit()
    conn.close()
    cur.close()


if __name__ == "__main__":
    main()
