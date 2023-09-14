import boto3
import time
from geniusrise import BatchOutput, Spout, State


class Athena(Spout):
    def __init__(self, output: BatchOutput, state: State, **kwargs):
        r"""
        Initialize the Athena class.

        Args:
            output (BatchOutput): An instance of the BatchOutput class for saving the data.
            state (State): An instance of the State class for maintaining the state.
            **kwargs: Additional keyword arguments.

        ## Using geniusrise to invoke via command line
        ```bash
        genius Athena rise \
            batch \
                --output_folder /path/to/output \
                --bucket my_bucket \
                --s3_folder s3/folder \
            none \
            fetch \
                --args region_name=us-east-1 output_location=s3://mybucket/output query="SELECT * FROM mytable"
        ```

        ## Using geniusrise to invoke via YAML file
        ```yaml
        version: "1"
        spouts:
            my_athena_spout:
                name: "Athena"
                method: "fetch"
                args:
                    region_name: "us-east-1"
                    output_location: "s3://mybucket/output"
                    query: "SELECT * FROM mytable"
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

    def fetch(
        self,
        region_name: str,
        output_location: str,
        query: str,
    ):
        """
        📖 Fetch data from an AWS Athena table and save it in batch.

        Args:
            region_name (str): The AWS region name.
            output_location (str): The S3 output location for the query results.
            query (str): The SQL query to execute.

        Raises:
            Exception: If unable to connect to the AWS Athena service or execute the query.
        """
        # Initialize the Athena client
        athena = boto3.client("athena", region_name=region_name)

        # Perform the Athena operation
        try:
            result = athena.start_query_execution(
                QueryString=query,
                ResultConfiguration={
                    "OutputLocation": output_location,
                },
            )
            query_id = result["QueryExecutionId"]

            while True:
                result = athena.get_query_execution(QueryExecutionId=query_id)
                status = result["QueryExecution"]["Status"]["State"]
                if status in ["SUCCEEDED", "FAILED"]:
                    break

                # Wait for the query to complete
                self.log.info(f"Waiting for query to complete ({status})...")
                time.sleep(5)

            # Save the query results to a file
            self.output.save(result["ResultSet"])

            # Update the state
            current_state = self.state.get_state(self.id) or {
                "success_count": 0,
                "failure_count": 0,
            }
            current_state["success_count"] += 1
            self.state.set_state(self.id, current_state)

        except Exception as e:
            self.log.error(f"Error fetching data from Athena: {e}")

            # Update the state
            current_state = self.state.get_state(self.id) or {
                "success_count": 0,
                "failure_count": 0,
            }
            current_state["failure_count"] += 1
            self.state.set_state(self.id, current_state)
