import google.cloud.bigquery as bigquery
from geniusrise import BatchOutput, Spout, State


class BigQuery(Spout):
    def __init__(self, output: BatchOutput, state: State, **kwargs):
        r"""
        Initialize the BigQuery class.

        Args:
            output (BatchOutput): An instance of the BatchOutput class for saving the data.
            state (State): An instance of the State class for maintaining the state.
            **kwargs: Additional keyword arguments.

        ## Using geniusrise to invoke via command line
        ```bash
        genius BigQuery rise \
            batch \
                --output_folder /path/to/output \
                --bucket my_bucket \
                --s3_folder s3/folder \
            none \
            fetch \
                --project_id my_project_id dataset_id=my_dataset table_id=my_table
        ```

        ## Using geniusrise to invoke via YAML file
        ```yaml
        version: "1"
        spouts:
            my_bigquery_spout:
                name: "BigQuery"
                method: "fetch"
                args:
                    project_id: "my_project_id"
                    dataset_id: "my_dataset"
                    table_id: "my_table"
                output:
                    type: "batch"
                    args:
                        output_folder: "/path/to/output"
                        bucket: "my_bucket"
                        s3_folder: "s3/folder"
        ```
        """
        super().__init__(output, state)
        self.top_level_arguments = kwargs

    def fetch(self, project_id: str, dataset_id: str, table_id: str):
        """
        📖 Fetch data from a BigQuery table and save it in batch.

        Args:
            project_id (str): The Google Cloud project ID.
            dataset_id (str): The BigQuery dataset ID.
            table_id (str): The BigQuery table ID.

        Raises:
            Exception: If unable to connect to the BigQuery server or execute the query.
        """
        # Initialize BigQuery client
        client = bigquery.Client(project=project_id)

        # Execute the query and save the results to a file
        query = f"SELECT * FROM `{dataset_id}.{table_id}`"
        results = client.query(query).result()
        self.output.save(results)

        # Update the state
        current_state = self.state.get_state(self.id) or {
            "success_count": 0,
            "failure_count": 0,
            "processed_rows": 0,
        }
        current_state["success_count"] += 1
        current_state["processed_rows"] = len(results)
        self.state.set_state(self.id, current_state)
