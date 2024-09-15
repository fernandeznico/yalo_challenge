"""
Get daily yesterday data from Iowa Liquor Sales (BigQuery -> S3 -> Postgres)

# Source
 - https://console.cloud.google.com/marketplace/details/iowa-department-of-commerce/iowa-liquor-sales

# Database Configuration

    CREATE SCHEMA staging;

    CREATE TABLE staging.iowa_liquor_sales(
        invoice_and_item_number TEXT, -- OPTIONS(description=""Concatenated invoice and line number associated with the liquor order. This provides a unique identifier for the individual liquor products included in the store order.""),
        date DATE, -- OPTIONS(description=""Date of order""),
        store_number TEXT, -- OPTIONS(description=""Unique number assigned to the store who ordered the liquor.""),
        store_name TEXT, -- OPTIONS(description=""Name of store who ordered the liquor.""),
        address TEXT, -- OPTIONS(description=""Address of store who ordered the liquor.""),
        city TEXT, -- OPTIONS(description=""City where the store who ordered the liquor is located""),
        zip_code TEXT, -- OPTIONS(description=""Zip code where the store who ordered the liquor is located""),
        store_location TEXT, -- OPTIONS(description=""Location of store who ordered the liquor. The Address, City, State and Zip Code are geocoded to provide geographic coordinates. Accuracy of geocoding is dependent on how well the address is interpreted and the completeness of the reference data used.""),
        county_number TEXT, -- OPTIONS(description=""Iowa county number for the county where store who ordered the liquor is located""),
        county TEXT, -- OPTIONS(description=""County where the store who ordered the liquor is located""),
        category TEXT, -- OPTIONS(description=""Category code associated with the liquor ordered""),
        category_name TEXT, -- OPTIONS(description=""Category of the liquor ordered.""),
        vendor_number TEXT, -- OPTIONS(description=""The vendor number of the company for the brand of liquor ordered""),
        vendor_name TEXT, -- OPTIONS(description=""The vendor name of the company for the brand of liquor ordered""),
        item_number TEXT, -- OPTIONS(description=""Item number for the individual liquor product ordered.""),
        item_description TEXT, -- OPTIONS(description=""Description of the individual liquor product ordered.""),
        pack BIGINT, -- OPTIONS(description=""The number of bottles in a case for the liquor ordered""),
        bottle_volume_ml BIGINT, -- OPTIONS(description=""Volume of each liquor bottle ordered in milliliters.""),
        state_bottle_cost DOUBLE PRECISION, -- OPTIONS(description=""The amount that Alcoholic Beverages Division paid for each bottle of liquor ordered""),
        state_bottle_retail DOUBLE PRECISION, -- OPTIONS(description=""The amount the store paid for each bottle of liquor ordered""),
        bottles_sold BIGINT, -- OPTIONS(description=""The number of bottles of liquor ordered by the store""),
        sale_dollars DOUBLE PRECISION, -- OPTIONS(description=""Total cost of liquor order (number of bottles multiplied by the state bottle retail)""),
        volume_sold_liters DOUBLE PRECISION, -- OPTIONS(description=""Total volume of liquor ordered in liters. (i.e. (Bottle Volume (ml) x Bottles Sold)/1,000)""),
        volume_sold_gallons DOUBLE PRECISION -- OPTIONS(description=""Total volume of liquor ordered in gallons. (i.e. (Bottle Volume (ml) x Bottles Sold)/3785.411784)"")
    );

    CREATE INDEX staging_iowa_liquor_sales_date ON staging.iowa_liquor_sales(date);

    CREATE SCHEMA analytics;

    CREATE TABLE analytics.iowa_liquor_daily_sales_by_store(
        store_number BIGINT NOT NULL,
        date DATE NOT NULL,
        variety_of_items INTEGER,
        bottles_sold INTEGER,
        liters INTEGER,
        sale_dollars INTEGER,

        PRIMARY KEY(store_number, date)
    );
"""
from pathlib import Path

from airflow import DAG
from airflow.decorators import task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from pendulum import datetime

from config import WORKSPACE_PATH
from utils.logging import log
from utils.postgres import execute_query
from utils.str import fmt_left


default_args = {
    "owner": "DE_TEAM",
    "depends_on_past": True,
    "catchup": True,
    "wait_for_downstream": True,
}


with DAG(
    dag_id=Path(__file__).stem,
    default_args=default_args,
    start_date=datetime(2024, 1, 1, tz="America/Chicago"),
    end_date=datetime(2024, 1, 5),
    template_searchpath=Path(__file__).parent.absolute().as_posix(),
    schedule_interval="0 3 * * *",
    tags=["BIGQUERY", "S3", "DAILY"],
) as dag:
    dag_output_absolute_path = WORKSPACE_PATH.joinpath(f"dag_{dag.dag_id}/{{ds}}.parquet").absolute().as_posix()

    @task(task_id="download_yesterday_sales")
    def download_yesterday_sales(**context):
        """Downloads daily data from a BigQuery table and saves it as a CSV file."""
        from os import environ

        from google.cloud import bigquery
        from pyarrow import Table
        from pyarrow.parquet import write_table as write_parquet_table

        ds = context["ds"]

        environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/opt/airflow/gcloud.json"
        client = bigquery.Client(project="quickstart-1556759099194")
        query = fmt_left(
            f"""
            SELECT *
            FROM `bigquery-public-data.iowa_liquor_sales.sales`
            WHERE date = '{ds}'
            """,
        )
        log.info(f"Executing the following query\n{query}")
        query_job = client.query(query)
        results = query_job.result()
        table_result: Table = results.to_arrow()
        log.info(f"Saving data into {dag_output_absolute_path}")
        Path(dag_output_absolute_path).parent.mkdir(parents=True, exist_ok=True)
        write_parquet_table(table=table_result, where=dag_output_absolute_path.format(ds=ds))

    @task(task_id="load_yesterday_sales_to_postgres")
    def load_yesterday_sales_to_postgres(**context):
        from pyarrow import parquet
        from sqlalchemy import create_engine

        ds = context["ds"]
        already_exists = execute_query(
            query=f"SELECT 1 FROM staging.iowa_liquor_sales WHERE date = '{ds}' LIMIT 1",
        )
        if already_exists:
            raise Exception(f"The data already exists for the date `{ds}`")  # Or delete and load

        df = parquet.read_table(dag_output_absolute_path.format(ds=ds)).to_pandas()
        engine = create_engine("postgresql://admin:admin@recalls_db:5432/recalls_db")
        with engine.connect() as conn:
            print(df.to_sql(schema="staging", name="iowa_liquor_sales", con=conn, if_exists="append", index=False))

    task_update_daily_sales_by_store_table = SQLExecuteQueryOperator(
        task_id="update_daily_sales_by_store_table",
        sql="update_daily_sales_by_store_table.sql",
        conn_id="recalls_db",
    )

    download_yesterday_sales() >> load_yesterday_sales_to_postgres() >> task_update_daily_sales_by_store_table
