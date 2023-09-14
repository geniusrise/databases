import voltdb
from geniusrise import BatchOutput, Spout, State


class VoltDB(Spout):
    def __init__(self, output: BatchOutput, state: State, **kwargs):
        r"""
        Initialize the VoltDB class.

        Args:
            output (BatchOutput): An instance of the BatchOutput class for saving the data.
            state (State): An instance of the State class for maintaining the state.
            **kwargs: Additional keyword arguments.

        ## Using geniusrise to invoke via command line
        ```bash
        genius VoltDB rise \
            batch \
                --output_folder /path/to/output \
                --bucket my_bucket \
                --s3_folder s3/folder \
            none \
            fetch \
                --args host=localhost port=21212 username=myuser password=<PASSWORD>
        ```

        ## Using geniusrise to invoke via YAML file
        ```yaml
        version: "1"
        spouts:
            my_voltdb_spout:
                name: "VoltDB"
                method: "fetch"
                args:
                    host: "localhost"
                    port: 21212
                    username: "myuser"
                    password: "<PASSWORD>"
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
        port: int,
        username: str,
        password: str,
    ):
        """
        📖 Fetch data from a VoltDB database and save it in batch.

        Args:
            host (str): The VoltDB host.
            port (int): The VoltDB port.
            username (str): The VoltDB username.
            password (str): The VoltDB password.

        Raises:
            Exception: If unable to connect to the VoltDB server or execute the query.
        """
        # Initialize VoltDB client
        client = voltdb.Client(host=host, port=port)

        try:
            # Connect to the database
            client.create_session(username, password)

            # Get the number of tables in the database
            table_count = len(client.get_tables())

            # Iterate through each table in the database
            tables = client.get_tables()
            processed_tables = 0

            while True:
                # Get a batch of tables
                batch = tables[::100]

                # Check if there are any tables in the batch
                if not batch:
                    break

                # Save the batch of tables to a file
                self.output.save(batch)

                # Update the number of processed tables
                processed_tables += len(batch)
                self.log.info(f"Total tables processed: {processed_tables}/{table_count}")

            # Update the state
            current_state = self.state.get_state(self.id) or {
                "success_count": 0,
                "failure_count": 0,
                "processed_tables": 0,
            }
            current_state["success_count"] += 1
            current_state["processed_tables"] = processed_tables
            self.state.set_state(self.id, current_state)

            # Log the total number of tables processed
            self.log.info(f"Total tables processed: {processed_tables}/{table_count}")

        except Exception as e:
            self.log.error(f"Error fetching data from VoltDB: {e}")

            # Update the state
            current_state = self.state.get_state(self.id) or {
                "success_count": 0,
                "failure_count": 0,
                "processed_tables": 0,
            }
            current_state["failure_count"] += 1
            self.state.set_state(self.id, current_state)
