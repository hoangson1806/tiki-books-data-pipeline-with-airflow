import pandas as pd
from sqlalchemy import create_engine
import psycopg2


drop_dim_table = """
            DROP TABLE IF EXISTS DIM_BOOK_TABLE;
"""
create_dim_book_table = """
    CREATE TABLE DIM_BOOK_TABLE (
        product_id VARCHAR(20) NOT NULL PRIMARY KEY,
        name VARCHAR(1000),
        author VARCHAR(1000),
        publisher VARCHAR(1000),
        manufacturer VARCHAR(1000)
    );
"""


def main():
    alchemyEngine = create_engine(
        "postgresql+psycopg2://hoangson:11111@localhost/airflow"
    )
    dbConnection = alchemyEngine.connect()
    conn = psycopg2.connect(
        database="airflow",
        user="hoangson",
        password="1111",
        host="localhost",
        port="5432",
    )
    cur = conn.cursor()
    cur.execute(drop_dim_table)
    cur.execute(create_dim_book_table)

    conn.commit()
    conn.close()
    cur.close()


if __name__ == "__main__":
    main()
