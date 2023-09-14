import pyodbc
from geniusrise import BatchOutput, Spout, State


class SQLServer(Spout):
    def __init__(self, output: BatchOutput, state: State, **kwargs):
        r"""
        Initialize the SQLServer class.

        Args:
            output (BatchOutput): An instance of the BatchOutput class for saving the data.
            state (State): An instance of the State class for maintaining the state.
            **kwargs: Additional keyword arguments.

        ## Using geniusrise to invoke via command line
        ```bash
        genius SQLServer rise \
            batch \
                --output_folder /path/to/output \
                --bucket my_bucket \
                --s3_folder s3/folder \
            none \
            fetch \
                --args server=localhost port=1433 user=myuser password=mypassword database=mydatabase query="SELECT * FROM mytable"
        ```

        ## Using geniusrise to invoke via YAML file
        ```yaml
        version: "1"
        spouts:
            my_sqlserver_spout:
                name: "SQLServer"
                method: "fetch"
                args:
                    server: "localhost"
                    port: 1433
                    user: "myuser"
                    password: "mypassword"
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
        server: str,
        port: int,
        user: str,
        password: str,
        database: str,
        query: str,
    ):
        """
        📖 Fetch data from a SQL Server database and save it in batch.

        Args:
            server (str): The SQL Server host.
            port (int): The SQL Server port.
            user (str): The SQL Server user.
            password (str): The SQL Server password.
            database (str): The SQL Server database name.
            query (str): The SQL query to execute.

        Raises:
            Exception: If unable to connect to the SQL Server server or execute the query.
        """
        # Initialize SQL Server connection
        connection_string = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};PORT={port};DATABASE={database};UID={user};PWD={password}"
        connection = pyodbc.connect(connection_string)

        try:
            with connection.cursor() as cursor:
                cursor.execute(query)
                total_rows = cursor.rowcount
                processed_rows = 0

                while True:
                    rows = cursor.fetchmany()
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
            self.log.error(f"Error fetching data from SQL Server: {e}")

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
