import google.cloud.spanner
from geniusrise import BatchOutput, Spout, State


class Spanner(Spout):
    def __init__(self, output: BatchOutput, state: State, **kwargs):
        r"""
        Initialize the Spanner class.

        Args:
            output (BatchOutput): An instance of the BatchOutput class for saving the data.
            state (State): An instance of the State class for maintaining the state.
            **kwargs: Additional keyword arguments.

        ## Using geniusrise to invoke via command line
        ```bash
        genius Spanner rise \
            batch \
                --output_folder /path/to/output \
                --bucket my_bucket \
                --s3_folder s3/folder \
            none \
            fetch \
                --project_id my_project_id instance_id=my_instance database_id=my_database table_id=my_table
        ```

        ## Using geniusrise to invoke via YAML file
        ```yaml
        version: "1"
        spouts:
            my_spanner_spout:
                name: "Spanner"
                method: "fetch"
                args:
                    project_id: "my_project_id"
                    instance_id: "my_instance"
                    database_id: "my_database"
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

    def fetch(self, project_id: str, instance_id: str, database_id: str, table_id: str):
        """
        📖 Fetch data from a Spanner database and save it in batch.

        Args:
            project_id (str): The Google Cloud project ID.
            instance_id (str): The Spanner instance ID.
            database_id (str): The Spanner database ID.
            table_id (str): The Spanner table ID.

        Raises:
            Exception: If unable to connect to the Spanner database or execute the query.
        """
        # Initialize Spanner client
        client = google.cloud.spanner.Client(project=project_id)

        # Execute the query and save the results to a file
        with client.session(database=database_id) as session:
            results = session.execute(f"SELECT * FROM {table_id}")
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
