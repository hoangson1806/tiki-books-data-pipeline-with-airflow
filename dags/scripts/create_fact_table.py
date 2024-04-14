import snowflake.connector

drop_fact_table = """
            DROP TABLE IF EXISTS FACT_TABLE;
"""
create_fact_table = """
    CREATE TABLE FACT_TABLE (
        product_id VARCHAR(20), -- REFERENCES DIMBOOK(product_id), 
        categories_id INT, -- REFERENCES DIMCATEGORY(categories_id),
        sku VARCHAR(50),
        quantity_sold INT,
        price REAL,
        orginal_price REAL,
        discount REAL,
        discount_rate REAL
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
    cur.execute(drop_fact_table)
    cur.execute(create_fact_table)
    conn.commit()
    conn.close()
    cur.close()


if __name__ == "__main__":
    main()
