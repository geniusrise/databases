import happybase
from geniusrise import BatchOutput, Spout, State


class HBase(Spout):
    def __init__(self, output: BatchOutput, state: State, **kwargs):
        r"""
        Initialize the HBase class.

        Args:
            output (BatchOutput): An instance of the BatchOutput class for saving the data.
            state (State): An instance of the State class for maintaining the state.
            **kwargs: Additional keyword arguments.

        ## Using geniusrise to invoke via command line
        ```bash
        genius HBase rise \
            batch \
                --output_folder /path/to/output \
                --bucket my_bucket \
                --s3_folder s3/folder \
            none \
            fetch \
                --args host=localhost table=my_table row_start=start row_stop=stop batch_size=100
        ```

        ## Using geniusrise to invoke via YAML file
        ```yaml
        version: "1"
        spouts:
            my_hbase_spout:
                name: "HBase"
                method: "fetch"
                args:
                    host: "localhost"
                    table: "my_table"
                    row_start: "start"
                    row_stop: "stop"
                    batch_size: 100
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
        host: str,
        table: str,
        row_start: str,
        row_stop: str,
        batch_size: int = 100,
    ):
        """
        📖 Fetch data from an HBase table and save it in batch.

        Args:
            host (str): The HBase host.
            table (str): The HBase table name.
            row_start (str): The row key to start scanning from.
            row_stop (str): The row key to stop scanning at.
            batch_size (int): The number of rows to fetch per batch. Defaults to 100.

        Raises:
            Exception: If unable to connect to the HBase server or execute the scan.
        """
        # Initialize HBase connection
        connection = happybase.Connection(host)
        hbase_table = connection.table(table)

        try:
            processed_rows = 0
            for row_key, data in hbase_table.scan(row_start=row_start, row_stop=row_stop, batch_size=batch_size):
                rows = [(row_key, data)]

                # Save the fetched rows to a file
                self.output.save(rows)

                # Update the number of processed rows
                processed_rows += 1
                self.log.info(f"Total rows processed: {processed_rows}")

            # Update the state
            current_state = self.state.get_state(self.id) or {
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
            self.log.error(f"Error fetching data from HBase: {e}")

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
