import psycopg2

from utils.logging import log


def execute_query(query: str, fetch_one: bool = True, commit: bool = False):
    # Replace with your database credentials
    host = "recalls_db"
    database = "recalls_db"
    user = "admin"
    password = "admin"
    port = 5432

    conn = None
    cur = None

    try:
        # Connect to the database
        conn = psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=password,
            port=port,
        )

        # Create a cursor object to execute queries
        cur = conn.cursor()

        log.info(f"Executing the following query\n{query}")

        # Execute the query
        cur.execute(query)

        if commit:
            conn.commit()

        if fetch_one:
            return cur.fetchone()

    finally:
        # Close the cursor and connection
        if cur:
            cur.close()
        if conn:
            conn.close()
