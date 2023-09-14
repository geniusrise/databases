from typing import Any, Dict

from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster, ClusterOptions
from geniusrise import BatchOutput, Spout, State


class Couchbase(Spout):
    def __init__(self, output: BatchOutput, state: State, **kwargs: Any) -> None:
        r"""
        Initialize the CouchbaseSpout class.

        Args:
            output (BatchOutput): An instance of the BatchOutput class for saving the data.
            state (State): An instance of the State class for maintaining the state.
            **kwargs: Additional keyword arguments.

        ## Using geniusrise to invoke via command line
        ```bash
        genius CouchbaseSpout rise \
            batch \
                --output_folder /path/to/output \
                --bucket my_bucket \
                --s3_folder s3/folder \
            none \
            fetch \
                --args host=localhost username=admin password=password bucket_name=my_bucket query="SELECT * FROM my_bucket" page_size=100
        ```

        ## Using geniusrise to invoke via YAML file
        ```yaml
        version: "1"
        spouts:
            my_couchbase_spout:
                name: "CouchbaseSpout"
                method: "fetch"
                args:
                    host: "localhost"
                    username: "admin"
                    password: "password"
                    bucket_name: "my_bucket"
                    query: "SELECT * FROM my_bucket"
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
        username: str,
        password: str,
        bucket_name: str,
        query: str,
        page_size: int = 100,
    ) -> None:
        """
        📖 Fetch data from a Couchbase bucket and save it in batch.

        Args:
            host (str): The Couchbase host.
            username (str): The Couchbase username.
            password (str): The Couchbase password.
            bucket_name (str): The Couchbase bucket name.
            query (str): The N1QL query to execute.
            page_size (int): The number of documents to fetch per page. Defaults to 100.

        Raises:
            Exception: If unable to connect to the Couchbase cluster or execute the query.
        """
        # Initialize Couchbase connection
        cluster = Cluster(
            f"couchbase://{host}",
            ClusterOptions(PasswordAuthenticator(username, password)),
        )
        bucket = cluster.bucket(bucket_name)
        collection = bucket.default_collection()

        try:
            # Execute the query
            result = cluster.query(query)
            processed_docs = 0

            for row in result.rows():
                # Save the fetched document to a file
                self.output.save(row)

                # Update the number of processed documents
                processed_docs += 1
                self.log.debug(f"Processed document {processed_docs}")

            # Update the state
            current_state: Dict[str, Any] = self.state.get_state(self.id) or {
                "success_count": 0,
                "failure_count": 0,
                "processed_docs": 0,
            }
            current_state["success_count"] += 1
            current_state["processed_docs"] = processed_docs
            self.state.set_state(self.id, current_state)

            self.log.info(f"Total documents processed: {processed_docs}")

        except Exception as e:
            self.log.error(f"Error fetching data from Couchbase: {e}")

            # Update the state
            current_state = self.state.get_state(self.id) or {
                "success_count": 0,
                "failure_count": 0,
                "processed_docs": 0,
            }
            current_state["failure_count"] += 1
            self.state.set_state(self.id, current_state)
