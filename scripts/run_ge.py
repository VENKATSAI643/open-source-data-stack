import os
import great_expectations as gx
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from dotenv import load_dotenv

def main():
    # Load .env file
    load_dotenv()
    
    # Set the working directory to the project root dynamically
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    os.chdir(project_root)
    
    # Get context
    context = gx.get_context()

    # Get ClickHouse credentials
    ch_user = os.getenv("CLICKHOUSE_USER", "default")
    ch_pass = os.getenv("CLICKHOUSE_PASSWORD", "")
    ch_host = "localhost:19000"
    
    # Add ClickHouse Datasource
    datasource_config = {
        "name": "clickhouse_datasource",
        "class_name": "Datasource",
        "execution_engine": {
            "class_name": "SqlAlchemyExecutionEngine",
            "connection_string": f"clickhouse+native://{ch_user}:{ch_pass}@{ch_host}/nyc_taxi_marts",
        },
        "data_connectors": {
            "default_configured_data_connector_name": {
                "class_name": "ConfiguredAssetSqlDataConnector",
                "assets": {
                    "fct_trips": {
                        "table_name": "fct_trips",
                        "schema_name": "nyc_taxi_marts"
                    }
                },
            },
        },
    }
    context.add_datasource(**datasource_config)
    print("Datasource added.")

    # Create Suite
    suite = context.add_or_update_expectation_suite(expectation_suite_name="trips_physics_suite")

    # Expect trip distance to be reasonable (< 200 miles)
    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_between",
            kwargs={
                "column": "trip_distance",
                "min_value": 0,
                "max_value": 200
            }
        )
    )

    # Expect trip duration to be positive and reasonable (< 500 mins)
    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_between",
            kwargs={
                "column": "trip_duration_min",
                "min_value": 0,
                "max_value": 500
            }
        )
    )

    # Save suite
    context.save_expectation_suite(suite, "trips_physics_suite")
    print("Suite saved.")

    # Create and run checkpoint
    checkpoint_config = {
        "name": "trips_checkpoint",
        "config_version": 1,
        "class_name": "Checkpoint",
        "validations": [
            {
                "batch_request": {
                    "datasource_name": "clickhouse_datasource",
                    "data_connector_name": "default_configured_data_connector_name",
                    "data_asset_name": "fct_trips",
                },
                "expectation_suite_name": "trips_physics_suite"
            }
        ]
    }
    context.add_or_update_checkpoint(**checkpoint_config)
    print("Running checkpoint...")
    result = context.run_checkpoint(checkpoint_name="trips_checkpoint")
    print("Success:", result.success)

    # Build data docs
    context.build_data_docs()
    print("Data Docs built.")

if __name__ == "__main__":
    main()
