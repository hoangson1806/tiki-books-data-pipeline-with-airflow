import pandas as pd
from sqlalchemy import create_engine
import snowflake.connector


drop_dim_table = """
            DROP TABLE IF EXISTS DIM_BOOK_TABLE;
"""
create_dim_book_table = """
    CREATE TABLE DIM_BOOK_TABLE (
        product_id VARCHAR(1000) NOT NULL PRIMARY KEY,
        name VARCHAR(1000),
        author VARCHAR(1000),
        publisher VARCHAR(1000),
        manufacturer VARCHAR(1000)
    );
"""


def main():

    conn = snowflake.connector.connect(
        user="HOANGSONSNOWFLAKE",
        password="Hoangson123@#",
        account="mjmpxrl-ai52284",
        warehouse="COMPUTE_WH",
        database="MY_DB",
        schema="PUBLIC",
    )
    cur = conn.cursor()
    cur.execute(drop_dim_table)
    cur.execute(create_dim_book_table)

    conn.commit()
    conn.close()
    cur.close()


if __name__ == "__main__":
    main()
