import pymongo
from geniusrise import BatchOutput, Spout, State


class MongoDB(Spout):
    def __init__(self, output: BatchOutput, state: State, **kwargs):
        r"""
        Initialize the MongoDB class.

        Args:
            output (BatchOutput): An instance of the BatchOutput class for saving the data.
            state (State): An instance of the State class for maintaining the state.
            **kwargs: Additional keyword arguments.

        ## Using geniusrise to invoke via command line
        ```bash
        genius MongoDB rise \
            batch \
                --output_folder /path/to/output \
                --bucket my_bucket \
                --s3_folder s3/folder \
            none \
            fetch \
                --args host=localhost port=27017 username=myusername password=mypassword database=mydatabase collection=mycollection
        ```

        ## Using geniusrise to invoke via YAML file
        ```yaml
        version: "1"
        spouts:
            my_mongodb_spout:
                name: "MongoDB"
                method: "fetch"
                args:
                    host: "localhost"
                    port: 27017
                    username: "myusername"
                    password: "mypassword"
                    database: "mydatabase"
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
        port: int,
        username: str,
        password: str,
        database: str,
        collection: str,
    ):
        """
        📖 Fetch data from a MongoDB database and save it in batch.

        Args:
            host (str): The MongoDB host.
            port (int): The MongoDB port.
            username (str): The MongoDB username.
            password (str): The MongoDB password.
            database (str): The MongoDB database name.
            collection (str): The MongoDB collection name.

        Raises:
            Exception: If unable to connect to the MongoDB server or execute the query.
        """
        # Initialize MongoDB client
        client = pymongo.MongoClient(host=host, port=port)  # type: ignore

        try:
            # Connect to the database
            db = client[database]
            collection = db[collection]  # type: ignore

            # Get the number of documents in the collection
            count = collection.count_documents({})  # type: ignore

            # Iterate through each document in the collection
            cursor = collection.find({})  # type: ignore
            processed_rows = 0

            while True:
                # Get a batch of documents
                batch = list(cursor.batch_size(100))  # type: ignore

                # Check if there are any documents in the batch
                if not batch:
                    break

                # Save the batch of documents to a file
                self.output.save(batch)

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
            self.log.error(f"Error fetching data from MongoDB: {e}")

            # Update the state
            current_state = self.state.get_state(self.id) or {
                "success_count": 0,
                "failure_count": 0,
                "processed_rows": 0,
            }
            current_state["failure_count"] += 1
            self.state.set_state(self.id, current_state)

        finally:
            # Close the MongoDB client
            client.close()
