import pymssql
from geniusrise import BatchOutput, Spout, State


class Sybase(Spout):
    def __init__(self, output: BatchOutput, state: State, **kwargs):
        r"""
        Initialize the Sybase class.

        Args:
            output (BatchOutput): An instance of the BatchOutput class for saving the data.
            state (State): An instance of the State class for maintaining the state.
            **kwargs: Additional keyword arguments.

        ## Using geniusrise to invoke via command line
        ```bash
        genius Sybase rise \
            batch \
                --output_folder /path/to/output \
                --bucket my_bucket \
                --s3_folder s3/folder \
            none \
            fetch \
                --args host=localhost port=5000 user=sa password=secret database=mydb query="SELECT * FROM table" page_size=100
        ```

        ## Using geniusrise to invoke via YAML file
        ```yaml
        version: "1"
        spouts:
            my_sybase_spout:
                name: "Sybase"
                method: "fetch"
                args:
                    host: "localhost"
                    port: 5000
                    user: "sa"
                    password: "secret"
                    database: "mydb"
                    query: "SELECT * FROM table"
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
        query: str,
        page_size: int = 100,
    ):
        """
        📖 Fetch data from a Sybase database and save it in batch.

        Args:
            host (str): The Sybase host.
            port (int): The Sybase port.
            user (str): The Sybase user.
            password (str): The Sybase password.
            database (str): The Sybase database name.
            query (str): The SQL query to execute.
            page_size (int): The number of rows to fetch per page. Defaults to 100.

        Raises:
            Exception: If unable to connect to the Sybase server or execute the query.
        """
        # Initialize Sybase connection
        connection = pymssql.connect(server=host, port=port, user=user, password=password, database=database)

        try:
            with connection.cursor() as cursor:
                cursor.execute(query)
                total_rows = cursor.rowcount
                processed_rows = 0

                while True:
                    rows = cursor.fetchmany(page_size)
                    if not rows:
                        break

                    # Save the fetched rows to a file
                    self.output.save(rows)

                    # Update the number of processed rows
                    processed_rows += len(rows)
                    self.log.info(f"Total rows processed: {processed_rows}/{total_rows}")

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
            self.log.info(f"Total rows processed: {processed_rows}/{total_rows}")

        except Exception as e:
            self.log.error(f"Error fetching data from Sybase: {e}")

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
