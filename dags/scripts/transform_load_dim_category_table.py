import pandas as pd
from sqlalchemy import create_engine
import psycopg2
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas


def transform(df_book_products):
    df_book_products["category"] = df_book_products["categories_name"].map(
        lambda x: str(x or "").strip().capitalize()
    )
    # df_book_products["catagory_id"] = df_book_products["categories_id"]

    df_category = df_book_products[["categories_id", "category"]]
    df_category = df_category.drop_duplicates()
    return df_category


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
    df_product = transform(pd.read_sql("select * from PRODUCT_DATA_INFO", dbConnect))
    print(df_product)
    write_pandas(
        conn, df_product, table_name="DIM_CATEGORY_TABLE", quote_identifiers=False
    )
    conn.commit()
    conn.close()
    cur.close()


if __name__ == "__main__":
    main()
