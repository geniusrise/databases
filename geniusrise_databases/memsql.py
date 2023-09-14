import memsql
from geniusrise import BatchOutput, Spout, State


class MemSQL(Spout):
    def __init__(self, output: BatchOutput, state: State, **kwargs):
        r"""
        Initialize the MemSQL class.

        Args:
            output (BatchOutput): An instance of the BatchOutput class for saving the data.
            state (State): An instance of the State class for maintaining the state.
            **kwargs: Additional keyword arguments.

        ## Using geniusrise to invoke via command line
        ```bash
        genius MemSQL rise \
            batch \
                --output_folder /path/to/output \
                --bucket my_bucket \
                --s3_folder s3/folder \
            none \
            fetch \
                --args host=mymemsqlhost user=myuser password=<PASSWORD> database=mydatabase query="SELECT * FROM mytable"
        ```

        ## Using geniusrise to invoke via YAML file
        ```yaml
        version: "1"
        spouts:
            my_memsql_spout:
                name: "MemSQL"
                method: "fetch"
                args:
                    host: "mymemsqlhost"
                    user: "myuser"
                    password: "<PASSWORD>"
                    database: "mydatabase"
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
        host: str,
        user: str,
        password: str,
        database: str,
        query: str,
    ):
        """
        📖 Fetch data from a MemSQL database and save it in batch.

        Args:
            host (str): The MemSQL host.
            user (str): The MemSQL user.
            password (str): The MemSQL password.
            database (str): The MemSQL database name.
            query (str): The SQL query to execute.

        Raises:
            Exception: If unable to connect to the MemSQL server or execute the query.
        """
        # Initialize MemSQL connection
        connection = memsql.connect(
            host=host,
            user=user,
            password=password,
            database=database,
        )

        try:
            with connection.cursor() as cursor:
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
            self.log.error(f"Error fetching data from MemSQL: {e}")

            # Update the state
            current_state = self.state.get_state(self.id) or {
                "success_count": 0,
                "failure_count": 0,
            }
            current_state["failure_count"] += 1
            self.state.set_state(self.id, current_state)
