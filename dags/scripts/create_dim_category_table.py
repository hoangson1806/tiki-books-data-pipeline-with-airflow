import pandas as pd
from sqlalchemy import create_engine
import psycopg2


drop_dim_table = """
            DROP TABLE IF EXISTS DIM_CATEGORY_TABLE;
"""
create_dim_category_table = """
    CREATE TABLE IF NOT EXISTS DIM_CATEGORY_TABLE (
        category_id INT NOT NULL, 
        category VARCHAR(1000),
        PRIMARY KEY(category_id)
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
    cur.execute(create_dim_category_table)

    conn.commit()
    conn.close()
    cur.close()


if __name__ == "__main__":
    main()
