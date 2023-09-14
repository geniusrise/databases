import os
import sqlite3
from typing import Any, Dict

import boto3
from geniusrise import BatchOutput, Spout, State


class SQLite(Spout):
    def __init__(self, output: BatchOutput, state: State, **kwargs: Any) -> None:
        r"""
        Initialize the SQLite class.

        Args:
            output (BatchOutput): An instance of the BatchOutput class for saving the data.
            state (State): An instance of the State class for maintaining the state.
            **kwargs: Additional keyword arguments.

        ## Using geniusrise to invoke via command line
        ```bash
        genius SQLite rise \
            batch \
                --output_folder /path/to/output \
                --bucket my_bucket \
                --s3_folder s3/folder \
            none \
            fetch \
                --args s3_bucket=my_s3_bucket s3_key=mydb.sqlite query="SELECT * FROM table" page_size=100
        ```

        ## Using geniusrise to invoke via YAML file
        ```yaml
        version: "1"
        spouts:
            my_sqlite_spout:
                name: "SQLite"
                method: "fetch"
                args:
                    s3_bucket: "my_s3_bucket"
                    s3_key: "mydb.sqlite"
                    query: "SELECT * FROM table"
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

    def fetch(self, s3_bucket: str, s3_key: str, query: str, page_size: int = 100) -> None:
        """
        📖 Fetch data from an SQLite database and save it in batch.

        Args:
            s3_bucket (str): The S3 bucket containing the SQLite database.
            s3_key (str): The S3 key for the SQLite database.
            query (str): The SQL query to execute.
            page_size (int): The number of rows to fetch per page. Defaults to 100.

        Raises:
            Exception: If unable to connect to the SQLite database or execute the query.
        """
        # Download SQLite database from S3
        local_path = f"/tmp/{os.path.basename(s3_key)}"
        s3 = boto3.client("s3")
        s3.download_file(s3_bucket, s3_key, local_path)

        # Initialize SQLite connection
        connection = sqlite3.connect(local_path)
        connection.row_factory = sqlite3.Row  # To get dict-like rows

        try:
            cursor = connection.cursor()
            cursor.execute(query)
            processed_rows = 0

            while True:
                rows = cursor.fetchmany(page_size)
                if not rows:
                    break

                # Convert rows to dictionaries
                rows = [dict(row) for row in rows]

                # Save the fetched rows to a file
                self.output.save(rows)

                # Update the number of processed rows
                processed_rows += len(rows)
                self.log.info(f"Processed rows: {processed_rows}")

            # Update the state
            current_state: Dict[str, Any] = self.state.get_state(self.id) or {
                "success_count": 0,
                "failure_count": 0,
                "processed_rows": 0,
            }
            current_state["success_count"] += 1
            current_state["processed_rows"] = processed_rows
            self.state.set_state(self.id, current_state)

        except Exception as e:
            self.log.error(f"Error fetching data from SQLite: {e}")

            # Update the state
            current_state = self.state.get_state(self.id) or {
                "success_count": 0,
                "failure_count": 0,
                "processed_rows": 0,
            }
            current_state["failure_count"] += 1
            self.state.set_state(self.id, current_state)

        finally:
            connection.close()
            os.remove(local_path)  # Remove the downloaded SQLite database
