import influxdb
from geniusrise import BatchOutput, Spout, State


class InfluxDB(Spout):
    def __init__(self, output: BatchOutput, state: State, **kwargs):
        r"""
        Initialize the InfluxDB class.

        Args:
            output (BatchOutput): An instance of the BatchOutput class for saving the data.
            state (State): An instance of the State class for maintaining the state.
            **kwargs: Additional keyword arguments.

        ## Using geniusrise to invoke via command line
        ```bash
        genius InfluxDB rise \
            batch \
                --output_folder /path/to/output \
                --bucket my_bucket \
                --s3_folder s3/folder \
            none \
            fetch \
                --args host=localhost port=8086 username=myusername password=mypassword database=mydatabase
        ```

        ## Using geniusrise to invoke via YAML file
        ```yaml
        version: "1"
        spouts:
            my_influxdb_spout:
                name: "InfluxDB"
                method: "fetch"
                args:
                    host: "localhost"
                    port: 8086
                    username: "myusername"
                    password: "mypassword"
                    database: "mydatabase"
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
        database: str,
    ):
        """
        📖 Fetch data from an InfluxDB database and save it in batch.

        Args:
            host (str): The InfluxDB host.
            port (int): The InfluxDB port.
            username (str): The InfluxDB username.
            password (str): The InfluxDB password.
            database (str): The InfluxDB database name.

        Raises:
            Exception: If unable to connect to the InfluxDB server or execute the query.
        """
        # Initialize InfluxDB client
        client = influxdb.InfluxDBClient(host, port, username, password, database)

        try:
            # Connect to the database
            with client:
                # Get the number of measurements in the database
                measurement_count = len(client.get_list_measurements())

                # Iterate through each measurement in the database
                cursor = client.get_list_measurements()
                processed_measurements = 0

                while True:
                    # Get a batch of measurements
                    batch = list(cursor.batch(100))

                    # Check if there are any measurements in the batch
                    if not batch:
                        break

                    # Save the batch of measurements to a file
                    self.output.save(batch)

                    # Update the number of processed measurements
                    processed_measurements += len(batch)
                    self.log.info(f"Total measurements processed: {processed_measurements}/{measurement_count}")

                # Update the state
                current_state = self.state.get_state(self.id) or {
                    "success_count": 0,
                    "failure_count": 0,
                    "processed_measurements": 0,
                }
                current_state["success_count"] += 1
                current_state["processed_measurements"] = processed_measurements
                self.state.set_state(self.id, current_state)

            # Log the total number of measurements processed
            self.log.info(f"Total measurements processed: {processed_measurements}/{measurement_count}")

        except Exception as e:
            self.log.error(f"Error fetching data from InfluxDB: {e}")

            # Update the state
            current_state = self.state.get_state(self.id) or {
                "success_count": 0,
                "failure_count": 0,
                "processed_measurements": 0,
            }
            current_state["failure_count"] += 1
            self.state.set_state(self.id, current_state)
