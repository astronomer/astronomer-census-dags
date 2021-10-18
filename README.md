### Sync Customer Data from a Data Warehouse to Marketing Campaign Platform with Census

This reverse-ETL pipeline syncs customer data from a Snowflake data warehouse to a marketing platform using
Census for those customers who last ordered a configurable number of days before the current date. The
extracted customer information will be used to send personalized emails as part of an ongoing reengagement
campaign.

#### Configuration
For configuration purposes, the pipeline uses an Airflow Variable which contains the number of days since a
customer's last order was placed to send a reengagement email: ``days_since_last_order``. The Variable JSON is
structured in the following manner:

```
    {
        "campaigns": {
            "reengagement": {
                "days_since_last_order": 90
            }
        }
    }
```

#### Connections
The Airflow Connections used for Census and Snowflake are stored under the default connection IDs,
"census_default" and "snowflake_default", respectively.
