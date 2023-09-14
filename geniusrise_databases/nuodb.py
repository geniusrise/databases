import nuodb
from geniusrise import BatchOutput, Spout, State


class NuoDB(Spout):
    def __init__(self, output: BatchOutput, state: State, **kwargs):
        r"""
        Initialize the NuoDB class.

        Args:
            output (BatchOutput): An instance of the BatchOutput class for saving the data.
            state (State): An instance of the State class for maintaining the state.
            **kwargs: Additional keyword arguments.

        ## Using geniusrise to invoke via command line
        ```bash
        genius NuoDB rise \
            batch \
                --output_folder /path/to/output \
                --bucket my_bucket \
                --s3_folder s3/folder \
            none \
            fetch \
                --args url=http://mynuodbhost:8080/v1/statement query="SELECT * FROM mytable"
        ```

        ## Using geniusrise to invoke via YAML file
        ```yaml
        version: "1"
        spouts:
            my_nuodb_spout:
                name: "NuoDB"
                method: "fetch"
                args:
                    url: "http://mynuodbhost:8080/v1/statement"
                    query: "SELECT * FROM mytable"
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
        📖 Fetch data from a NuoDB table and save it in batch.

        Args:
            url (str): The URL of the NuoDB API endpoint.
            query (str): The SQL query to execute.

        Raises:
            Exception: If unable to connect to the NuoDB server or execute the query.
        """
        # Perform the NuoDB operation
        try:
            session = nuodb.Session(url)
            cursor = session.cursor()
            cursor.execute(query)
            data = cursor.fetchall()

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
            self.log.error(f"Error fetching data from NuoDB: {e}")

            # Update the state
            current_state = self.state.get_state(self.id) or {
                "success_count": 0,
                "failure_count": 0,
            }
            current_state["failure_count"] += 1
            self.state.set_state(self.id, current_state)
