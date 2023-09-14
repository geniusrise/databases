from geniusrise import BatchOutput, Spout, State
from pymongo import MongoClient


class DocumentDB(Spout):
    def __init__(self, output: BatchOutput, state: State, **kwargs):
        r"""
        Initialize the DocumentDB class.

        Args:
            output (BatchOutput): An instance of the BatchOutput class for saving the data.
            state (State): An instance of the State class for maintaining the state.
            **kwargs: Additional keyword arguments.

        ## Using geniusrise to invoke via command line
        ```bash
        genius DocumentDB rise \
            batch \
                --output_folder /path/to/output \
                --bucket my_bucket \
                --s3_folder s3/folder \
            none \
            fetch \
                --args host=localhost port=27017 user=myuser password=mypassword database=mydb collection=mycollection query="{}" page_size=100
        ```

        ## Using geniusrise to invoke via YAML file
        ```yaml
        version: "1"
        spouts:
            my_documentdb_spout:
                name: "DocumentDB"
                method: "fetch"
                args:
                    host: "localhost"
                    port: 27017
                    user: "myuser"
                    password: "mypassword"
                    database: "mydb"
                    collection: "mycollection"
                    query: "{}"
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

    def fetch(
        self,
        host: str,
        port: int,
        user: str,
        password: str,
        database: str,
        collection: str,
        query: str,
        page_size: int = 100,
    ):
        """
        📖 Fetch data from a DocumentDB database and save it in batch.

        Args:
            host (str): The DocumentDB host.
            port (int): The DocumentDB port.
            user (str): The DocumentDB user.
            password (str): The DocumentDB password.
            database (str): The DocumentDB database name.
            collection (str): The DocumentDB collection name.
            query (str): The query to execute.
            page_size (int): The number of documents to fetch per page. Defaults to 100.

        Raises:
            Exception: If unable to connect to the DocumentDB server or execute the query.
        """
        # Initialize DocumentDB connection
        connection = MongoClient(host, port, username=user, password=password)
        db = connection[database]
        coll = db[collection]

        try:
            cursor = coll.find(eval(query)).limit(page_size)
            processed_docs = 0

            for doc in cursor:
                # Save the fetched document to a file
                self.output.save([doc])

                # Update the number of processed documents
                processed_docs += 1
                self.log.info(f"Total documents processed: {processed_docs}")

            # Update the state
            current_state = self.state.get_state(self.id) or {
                "success_count": 0,
                "failure_count": 0,
                "processed_docs": 0,
            }
            current_state["success_count"] += 1
            current_state["processed_docs"] = processed_docs
            self.state.set_state(self.id, current_state)

            # Log the total number of documents processed
            self.log.info(f"Total documents processed: {processed_docs}")

        except Exception as e:
            self.log.error(f"Error fetching data from DocumentDB: {e}")

            # Update the state
            current_state = self.state.get_state(self.id) or {
                "success_count": 0,
                "failure_count": 0,
                "processed_docs": 0,
            }
            current_state["failure_count"] += 1
            self.state.set_state(self.id, current_state)

        finally:
            connection.close()
