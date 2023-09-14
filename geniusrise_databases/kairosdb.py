import requests
from geniusrise import BatchOutput, Spout, State


class KairosDB(Spout):
    def __init__(self, output: BatchOutput, state: State, **kwargs):
        r"""
        Initialize the KairosDB class.

        Args:
            output (BatchOutput): An instance of the BatchOutput class for saving the data.
            state (State): An instance of the State class for maintaining the state.
            **kwargs: Additional keyword arguments.

        ## Using geniusrise to invoke via command line
        ```bash
        genius KairosDB rise \
            batch \
                --output_folder /path/to/output \
                --bucket my_bucket \
                --s3_folder s3/folder \
            none \
            fetch \
                --args url=http://mykairosdbhost:8080/api/v1/datapoints query="SELECT * FROM mymetric"
        ```

        ## Using geniusrise to invoke via YAML file
        ```yaml
        version: "1"
        spouts:
            my_kairosdb_spout:
                name: "KairosDB"
                method: "fetch"
                args:
                    url: "http://mykairosdbhost:8080/api/v1/datapoints"
                    query: "SELECT * FROM mymetric"
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
        url: str,
        query: str,
    ):
        """
        📖 Fetch data from a KairosDB metric and save it in batch.

        Args:
            url (str): The URL of the KairosDB API endpoint.
            query (str): The SQL query to execute.

        Raises:
            Exception: If unable to connect to the KairosDB server or execute the query.
        """
        # Perform the KairosDB operation
        try:
            response = requests.get(url, params={"query": query})
            data = response.json()["results"]

            # Save the query results to a file
            self.output.save(data)

            # Update the state
            current_state = self.state.get_state(self.id) or {
                "success_count": 0,
                "failure_count": 0,
            }
            current_state["success_count"] += 1
            self.state.set_state(self.id, current_state)

        except Exception as e:
            self.log.error(f"Error fetching data from KairosDB: {e}")

            # Update the state
            current_state = self.state.get_state(self.id) or {
                "success_count": 0,
                "failure_count": 0,
            }
            current_state["failure_count"] += 1
            self.state.set_state(self.id, current_state)
