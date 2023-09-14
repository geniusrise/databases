import arango
from geniusrise import BatchOutput, Spout, State


class ArangoDB(Spout):
    def __init__(self, output: BatchOutput, state: State, **kwargs):
        r"""
        Initialize the Arango class.

        Args:
            output (BatchOutput): An instance of the BatchOutput class for saving the data.
            state (State): An instance of the State class for maintaining the state.
            **kwargs: Additional keyword arguments.

        ## Using geniusrise to invoke via command line
        ```bash
        genius Arango rise \
            batch \
                --output_folder /path/to/output \
                --bucket my_bucket \
                --s3_folder s3/folder \
            none \
            fetch \
                --args host=myarangodb.example.com username=myusername password=mypassword database=mydb collection=mycollection
        ```

        ## Using geniusrise to invoke via YAML file
        ```yaml
        version: "1"
        spouts:
            my_arangodb_spout:
                name: "Arango"
                method: "fetch"
                args:
                    host: "myarangodb.example.com"
                    username: "myusername"
                    password: "mypassword"
                    database: "mydb"
                    collection: "mycollection"
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
        username: str,
        password: str,
        database: str,
        collection: str,
    ):
        """
        📖 Fetch data from an ArangoDB collection and save it in batch.

        Args:
            host (str): The ArangoDB host.
            username (str): The ArangoDB username.
            password (str): The ArangoDB password.
            database (str): The ArangoDB database name.
            collection (str): The name of the ArangoDB collection.

        Raises:
            Exception: If unable to connect to the ArangoDB server or execute the command.
        """
        # Initialize the ArangoDB connection
        try:
            conn = arango.Connection(host=host, username=username, password=password)
            db = conn.db(database)
        except Exception as e:
            self.log.error(f"Error connecting to ArangoDB server: {e}")
            return

        # Perform the ArangoDB operation
        try:
            cursor = db[collection].fetch()

            while True:
                doc = cursor.next()
                if not doc:
                    break

                # Save the fetched document to a file
                self.output.save(doc)

            # Update the state
            current_state = self.state.get_state(self.id) or {
                "success_count": 0,
                "failure_count": 0,
            }
            current_state["success_count"] += 1
            self.state.set_state(self.id, current_state)

        except Exception as e:
            self.log.error(f"Error fetching data from ArangoDB: {e}")

            # Update the state
            current_state = self.state.get_state(self.id) or {
                "success_count": 0,
                "failure_count": 0,
            }
            current_state["failure_count"] += 1
            self.state.set_state(self.id, current_state)

        finally:
            conn.close()
