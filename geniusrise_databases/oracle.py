import cx_Oracle
from geniusrise import BatchOutput, Spout, State


class Oracle(Spout):
    def __init__(self, output: BatchOutput, state: State, **kwargs):
        r"""
        Initialize the OracleSQL class.

        Args:
            output (BatchOutput): An instance of the BatchOutput class for saving the data.
            state (State): An instance of the State class for maintaining the state.
            **kwargs: Additional keyword arguments.

        ## Using geniusrise to invoke via command line
        ```bash
        genius OracleSQL rise \
            batch \
                --output_folder /path/to/output \
                --bucket my_bucket \
                --s3_folder s3/folder \
            none \
            fetch \
                --args server=localhost port=1521 service_name=myservice user=myuser password=mypassword
        ```

        ## Using geniusrise to invoke via YAML file
        ```yaml
        version: "1"
        spouts:
            my_oraclesql_spout:
                name: "OracleSQL"
                method: "fetch"
                args:
                    server: "localhost"
                    port: 1521
                    service_name: "myservice"
                    user: "myuser"
                    password: "mypassword"
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
        service_name: str,
        user: str,
        password: str,
        query: str,
    ):
        """
        📖 Fetch data from an Oracle SQL database and save it in batch.

        Args:
            server (str): The Oracle SQL server.
            port (int): The Oracle SQL port.
            service_name (str): The Oracle service name.
            user (str): The Oracle user.
            password (str): The Oracle password.
            query (str): The SQL query to execute.

        Raises:
            Exception: If unable to connect to the Oracle SQL server or execute the query.
        """
        # Initialize Oracle SQL connection
        connection_string = f"oracle://{user}/{password}@{server}:{port}/{service_name}"
        connection = cx_Oracle.connect(connection_string)

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
            self.log.error(f"Error fetching data from Oracle SQL: {e}")

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
