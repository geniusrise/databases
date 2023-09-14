from typing import Any, Dict

import boto3
from geniusrise import BatchOutput, Spout, State


class DynamoDB(Spout):
    def __init__(self, output: BatchOutput, state: State, **kwargs: Any) -> None:
        r"""
        Initialize the DynamoDB class.

        Args:
            output (BatchOutput): An instance of the BatchOutput class for saving the data.
            state (State): An instance of the State class for maintaining the state.
            **kwargs: Additional keyword arguments.

        ## Using geniusrise to invoke via command line
        ```bash
        genius DynamoDB rise \
            batch \
                --output_folder /path/to/output \
                --bucket my_bucket \
                --s3_folder s3/folder \
            none \
            fetch \
                --args table_name=my_table page_size=100
        ```

        ## Using geniusrise to invoke via YAML file
        ```yaml
        version: "1"
        spouts:
            my_dynamodb_spout:
                name: "DynamoDB"
                method: "fetch"
                args:
                    table_name: "my_table"
                    page_size: 100
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

    def fetch(self, table_name: str, page_size: int = 100) -> None:
        """
        📖 Fetch data from a DynamoDB table and save it in batch.

        Args:
            table_name (str): The DynamoDB table name.
            page_size (int): The number of rows to fetch per page. Defaults to 100.

        Raises:
            Exception: If unable to connect to the DynamoDB or fetch the data.
        """
        # Initialize DynamoDB client
        dynamodb = boto3.resource("dynamodb")
        table = dynamodb.Table(table_name)

        # Initialize variables
        processed_rows = 0
        last_evaluated_key = None

        try:
            while True:
                if last_evaluated_key:
                    response = table.scan(Limit=page_size, ExclusiveStartKey=last_evaluated_key)
                else:
                    response = table.scan(Limit=page_size)

                items = response.get("Items", [])
                if not items:
                    break

                # Save the fetched rows to a file
                self.output.save(items)

                # Update the number of processed rows
                processed_rows += len(items)
                self.log.info(f"Total rows processed: {processed_rows}")

                last_evaluated_key = response.get("LastEvaluatedKey")
                if not last_evaluated_key:
                    break

            # Update the state
            current_state: Dict[str, Any] = self.state.get_state(self.id) or {
                "success_count": 0,
                "failure_count": 0,
                "processed_rows": 0,
            }
            current_state["success_count"] += 1
            current_state["processed_rows"] = processed_rows
            self.state.set_state(self.id, current_state)

            # Log the total number of rows processed
            self.log.info(f"Total rows processed: {processed_rows}")

        except Exception as e:
            self.log.error(f"Error fetching data from DynamoDB: {e}")

            # Update the state
            current_state = self.state.get_state(self.id) or {
                "success_count": 0,
                "failure_count": 0,
                "processed_rows": 0,
            }
            current_state["failure_count"] += 1
            self.state.set_state(self.id, current_state)
