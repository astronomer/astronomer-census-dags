from datetime import datetime, timedelta

from airflow.decorators import dag
from airflow.models.baseoperator import chain
from airflow.operators.dummy import DummyOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow_provider_census.operators.census import CensusOperator
from airflow_provider_census.sensors.census import CensusSensor


@dag(
    dag_id="census_sync_for_customer_reengagement",
    start_date=datetime(2021, 9, 24),
    schedule_interval="@daily",
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=3)},
    default_view="graph",
)
def census_sync_for_customer_reengagement():
    begin = DummyOperator(task_id="begin")

    # Inserts new rows into the ``customers_for_reengagement`` table for those customers last placed an order
    # 90 days ago (as configured) and have opted-in for to receive marketing emails.
    get_customers_for_reengagement = SnowflakeOperator(
        task_id="get_customers_for_reengagement",
        sql="""
        INSERT INTO marketing.campaigns.customers_for_reengagement
        SELECT
            customer_id,
            first_name,
            last_name,
            email_address
        FROM erp.sales.customers
        WHERE DATEDIFF(last_order_date, CURRENT_DATE()) = {{ var.json.campaigns.reengagement.days_since_last_order }}
          AND opt_in_marketing_emails = 'Y'
          AND NOT EXISTS (
              SELECT 1
              FROM marketing.campaigns.customers_for_reengagement reengage
              WHERE customers.customer_id = reengage.customer_id
          );
        """,
    )

    # This syncs the ``marketing.campaigns.customers_for_reengagement`` table data to the marketing platform.
    trigger_census_sync_to_marketing_platform = CensusOperator(
        task_id="trigger_census_sync_to_marketing_platform",
        sync_id=8290,
    )

    # Checks the status of the Census sync run for completion every 30 seconds.  This operator uses the
    # ``sync_run_id`` returned from the ``CensusOperator`` task as an XComArg.
    wait_for_census_sync = CensusSensor(
        task_id="wait_for_census_sync",
        sync_run_id=trigger_census_sync_to_marketing_platform.output,
        poke_interval=30,
    )

    end = DummyOperator(task_id="end")

    # Set task dependencies.
    chain(
        begin,
        get_customers_for_reengagement,
        trigger_census_sync_to_marketing_platform,
        wait_for_census_sync,
        end,
    )

dag = census_sync_for_customer_reengagement()
