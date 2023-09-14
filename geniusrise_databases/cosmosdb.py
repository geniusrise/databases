import azure.cosmos.cosmos_client as cosmos_client
from geniusrise import BatchOutput, Spout, State


class CosmosDB(Spout):
    def __init__(self, output: BatchOutput, state: State, **kwargs):
        r"""
        Initialize the Cosmos DB class.

        Args:
            output (BatchOutput): An instance of the BatchOutput class for saving the data.
            state (State): An instance of the State class for maintaining the state.
            **kwargs: Additional keyword arguments.

        ## Using geniusrise to invoke via command line
        ```bash
        genius CosmosDB rise \
            batch \
                --output_folder /path/to/output \
                --bucket my_bucket \
                --s3_folder s3/folder \
            none \
            fetch \
                --args endpoint=https://mycosmosdb.documents.azure.com:443/ my_database my_collection
        ```

        ## Using geniusrise to invoke via YAML file
        ```yaml
        version: "1"
        spouts:
            my_cosmosdb_spout:
                name: "CosmosDB"
                method: "fetch"
                args:
                    endpoint: "https://mycosmosdb.documents.azure.com:443/"
                    database: "my_database"
                    collection: "my_collection"
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
        endpoint: str,
        database: str,
        collection: str,
    ):
        """
        📖 Fetch data from a Cosmos DB collection and save it in batch.

        Args:
            endpoint (str): The Cosmos DB endpoint URL.
            database (str): The Cosmos DB database name.
            collection (str): The Cosmos DB collection name.

        Raises:
            Exception: If unable to connect to the Cosmos DB server or execute the query.
        """
        # Initialize Cosmos DB client
        client = cosmos_client.CosmosClient(endpoint, {"masterKey": "my_master_key"})

        try:
            # Connect to the database and collection
            database = client.get_database_client(database)  # type: ignore
            collection = database.get_container_client(collection)  # type: ignore

            # Get the number of documents in the collection
            document_count = collection.get_item_count()  # type: ignore

            # Iterate through each document in the collection
            processed_documents = 0
            continuation_token = None

            while True:
                # Get a batch of documents
                options = {"partitionKey": "my_partition_key"}
                if continuation_token:
                    options["continuation"] = continuation_token
                response = collection.query_items(query="SELECT * FROM c", options=options)  # type: ignore

                # Check if there are any documents in the batch
                if not response:
                    break

                # Save the batch of documents to a file
                self.output.save(response)

                # Update the number of processed documents
                processed_documents += len(response)
                self.log.info(f"Total documents processed: {processed_documents}/{document_count}")

                # Get the continuation token
                continuation_token = response["_continuation_token"]

            # Update the state
            current_state = self.state.get_state(self.id) or {
                "success_count": 0,
                "failure_count": 0,
                "processed_documents": 0,
            }
            current_state["success_count"] += 1
            current_state["processed_documents"] = processed_documents
            self.state.set_state(self.id, current_state)

            # Log the total number of documents processed
            self.log.info(f"Total documents processed: {processed_documents}/{document_count}")

        except Exception as e:
            self.log.error(f"Error fetching data from Cosmos DB: {e}")

            # Update the state
            current_state = self.state.get_state(self.id) or {
                "success_count": 0,
                "failure_count": 0,
                "processed_documents": 0,
            }
            current_state["failure_count"] += 1
            self.state.set_state(self.id, current_state)
