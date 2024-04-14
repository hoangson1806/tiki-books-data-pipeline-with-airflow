import snowflake.connector

drop_dim_table = """
            DROP TABLE IF EXISTS DIM_CATEGORY_TABLE;
"""
create_dim_category_table = """
    CREATE TABLE IF NOT EXISTS DIM_CATEGORY_TABLE (
        categories_id INT NOT NULL, 
        category VARCHAR(1000),
        PRIMARY KEY(categories_id)
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
    cur.execute(create_dim_category_table)

    conn.commit()
    conn.close()
    cur.close()


if __name__ == "__main__":
    main()
