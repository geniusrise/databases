import google.cloud.firestore_v1
from geniusrise import BatchOutput, Spout, State


class Firestore(Spout):
    def __init__(self, output: BatchOutput, state: State, **kwargs):
        r"""
        Initialize the Firestore class.

        Args:
            output (BatchOutput): An instance of the BatchOutput class for saving the data.
            state (State): An instance of the State class for maintaining the state.
            **kwargs: Additional keyword arguments.

        ## Using geniusrise to invoke via command line
        ```bash
        genius Firestore rise \
            batch \
                --output_folder /path/to/output \
                --bucket my_bucket \
                --s3_folder s3/folder \
            none \
            fetch \
                --args project_id=my-project collection_id=my-collection
        ```

        ## Using geniusrise to invoke via YAML file
        ```yaml
        version: "1"
        spouts:
            my_firestore_spout:
                name: "Firestore"
                method: "fetch"
                args:
                    project_id: "my-project"
                    collection_id: "my-collection"
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
        project_id: str,
        collection_id: str,
    ):
        """
        📖 Fetch data from a Firestore collection and save it in batch.

        Args:
            project_id (str): The Google Cloud project ID.
            collection_id (str): The Firestore collection ID.

        Raises:
            Exception: If unable to connect to the Firestore server or execute the query.
        """
        # Initialize Firestore client
        client = google.cloud.firestore_v1.Client(project=project_id)

        try:
            # Connect to the collection
            collection = client.collection(collection_id)

            # Get the number of documents in the collection
            document_count = len(list(collection.stream()))

            # Iterate through each document in the collection
            documents = list(collection.stream())
            processed_documents = 0

            while True:
                # Get a batch of documents
                batch = list(documents[::100])

                # Check if there are any documents in the batch
                if not batch:
                    break

                # Save the batch of documents to a file
                self.output.save(batch)

                # Update the number of processed documents
                processed_documents += len(batch)
                self.log.info(f"Total documents processed: {processed_documents}/{document_count}")

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
            self.log.error(f"Error fetching data from Firestore: {e}")

            # Update the state
            current_state = self.state.get_state(self.id) or {
                "success_count": 0,
                "failure_count": 0,
                "processed_documents": 0,
            }
            current_state["failure_count"] += 1
            self.state.set_state(self.id, current_state)
