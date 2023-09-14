import redis  # type: ignore
from geniusrise import BatchOutput, Spout, State


class Redis(Spout):
    def __init__(self, output: BatchOutput, state: State, **kwargs):
        r"""
        Initialize the Redis class.

        Args:
            output (BatchOutput): An instance of the BatchOutput class for saving the data.
            state (State): An instance of the State class for maintaining the state.
            **kwargs: Additional keyword arguments.

        ## Using geniusrise to invoke via command line
        ```bash
        genius Redis rise \
            batch \
                --output_folder /path/to/output \
                --bucket my_bucket \
                --s3_folder s3/folder \
            none \
            fetch \
                --args host=localhost port=6379 password=mypassword database=0
        ```

        ## Using geniusrise to invoke via YAML file
        ```yaml
        version: "1"
        spouts:
            my_redis_spout:
                name: "Redis"
                method: "fetch"
                args:
                    host: "localhost"
                    port: 6379
                    password: "mypassword"
                    database: 0
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
        password: str,
        database: int,
    ):
        """
        📖 Fetch data from a Redis database and save it in batch.

        Args:
            host (str): The Redis host.
            port (int): The Redis port.
            password (str): The Redis password.
            database (int): The Redis database number.

        Raises:
            Exception: If unable to connect to the Redis server or execute the command.
        """
        # Initialize Redis connection
        connection = redis.Redis(
            host=host,
            port=port,
            password=password,
            db=database,
        )

        try:
            # Get the number of keys in the database
            count = connection.dbsize()

            # Iterate through each key in the database
            cursor = connection.scan_iter()
            processed_rows = 0

            while True:
                # Get a batch of keys
                batch = list(cursor)

                # Check if there are any keys in the batch
                if not batch:
                    break

                # Get the values for each key in the batch
                values = connection.mget(batch)

                # Save the batch of key-value pairs to a file
                self.output.save(list(zip(batch, values)))

                # Update the number of processed rows
                processed_rows += len(batch)
                self.log.info(f"Total rows processed: {processed_rows}/{count}")

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
            self.log.info(f"Total rows processed: {processed_rows}/{count}")

        except Exception as e:
            self.log.error(f"Error fetching data from Redis: {e}")

            # Update the state
            current_state = self.state.get_state(self.id) or {
                "success_count": 0,
                "failure_count": 0,
                "processed_rows": 0,
            }
            current_state["failure_count"] += 1
            self.state.set_state(self.id, current_state)

        finally:
            # Close the Redis connection
            connection.close()
