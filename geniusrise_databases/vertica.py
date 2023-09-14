import vertica_python
from geniusrise import BatchOutput, Spout, State


class Vertica(Spout):
    def __init__(self, output: BatchOutput, state: State, **kwargs):
        r"""
        Initialize the Vertica class.

        Args:
            output (BatchOutput): An instance of the BatchOutput class for saving the data.
            state (State): An instance of the State class for maintaining the state.
            **kwargs: Additional keyword arguments.

        ## Using geniusrise to invoke via command line
        ```bash
        genius Vertica rise \
            batch \
                --output_folder /path/to/output \
                --bucket my_bucket \
                --s3_folder s3/folder \
            none \
            fetch \
                --host my_host port=5433 user=my_user password=my_password database=my_database query="SELECT * FROM my_table"
        ```

        ## Using geniusrise to invoke via YAML file
        ```yaml
        version: "1"
        spouts:
            my_vertica_spout:
                name: "Vertica"
                method: "fetch"
                args:
                    host: "my_host"
                    port: 5433
                    user: "my_user"
                    password: "my_password"
                    database: "my_database"
                    query: "SELECT * FROM my_table"
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

    def fetch(self, host: str, port: int, user: str, password: str, database: str, query: str):
        """
        📖 Fetch data from a Vertica database and save it in batch.

        Args:
            host (str): The Vertica host.
            port (int): The Vertica port.
            user (str): The Vertica user.
            password (str): The Vertica password.
            database (str): The Vertica database name.
            query (str): The SQL query to execute.

        Raises:
            Exception: If unable to connect to the Vertica server or execute the query.
        """
        # Initialize Vertica connection
        connection = vertica_python.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
        )

        try:
            # Execute the query and save the results to a file
            with connection.cursor() as cursor:
                cursor.execute(query)
                results = cursor.fetchall()
                self.output.save(results)

                # Update the state
                current_state = self.state.get_state(self.id) or {
                    "success_count": 0,
                    "failure_count": 0,
                    "processed_rows": 0,
                }
                current_state["success_count"] += 1
                current_state["processed_rows"] = len(results)
                self.state.set_state(self.id, current_state)

        except Exception as e:
            self.log.error(f"Error fetching data from Vertica: {e}")

            # Update the state
            current_state = self.state.get_state(self.id) or {
                "success_count": 0,
                "failure_count": 0,
                "processed_rows": 0,
            }
            current_state["failure_count"] += 1
            self.state.set_state(self.id, current_state)

        finally:
            connection.close()
