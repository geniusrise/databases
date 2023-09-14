import riak
from geniusrise import BatchOutput, Spout, State


class Riak(Spout):
    def __init__(self, output: BatchOutput, state: State, **kwargs):
        r"""
        Initialize the Riak class.

        Args:
            output (BatchOutput): An instance of the BatchOutput class for saving the data.
            state (State): An instance of the State class for maintaining the state.
            **kwargs: Additional keyword arguments.

        ## Using geniusrise to invoke via command line
        ```bash
        genius Riak rise \
            batch \
                --output_folder /path/to/output \
                --bucket my_bucket \
                --s3_folder s3/folder \
            none \
            fetch \
                --args host=localhost port=8098
        ```

        ## Using geniusrise to invoke via YAML file
        ```yaml
        version: "1"
        spouts:
            my_riak_spout:
                name: "Riak"
                method: "fetch"
                args:
                    host: "localhost"
                    port: 8098
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
    ):
        """
        📖 Fetch data from a Riak database and save it in batch.

        Args:
            host (str): The Riak host.
            port (int): The Riak port.

        Raises:
            Exception: If unable to connect to the Riak server or execute the query.
        """
        # Initialize Riak client
        client = riak.RiakClient(host=host, port=port)

        try:
            # Connect to the database
            with client as bucket:
                # Get the number of objects in the database
                count = len(list(bucket.get_bucket().keys()))

                # Iterate through each object in the database
                cursor = bucket.get_bucket().get_keys()
                processed_objects = 0

                while True:
                    # Get a batch of objects
                    batch = list(cursor.batch_size(100))

                    # Check if there are any objects in the batch
                    if not batch:
                        break

                    # Save the batch of objects to a file
                    self.output.save(batch)

                    # Update the number of processed objects
                    processed_objects += len(batch)
                    self.log.info(f"Total objects processed: {processed_objects}/{count}")

                # Update the state
                current_state = self.state.get_state(self.id) or {
                    "success_count": 0,
                    "failure_count": 0,
                    "processed_objects": 0,
                }
                current_state["success_count"] += 1
                current_state["processed_objects"] = processed_objects
                self.state.set_state(self.id, current_state)

            # Log the total number of objects processed
            self.log.info(f"Total objects processed: {processed_objects}/{count}")

        except Exception as e:
            self.log.error(f"Error fetching data from Riak: {e}")

            # Update the state
            current_state = self.state.get_state(self.id) or {
                "success_count": 0,
                "failure_count": 0,
                "processed_objects": 0,
            }
            current_state["failure_count"] += 1
            self.state.set_state(self.id, current_state)
