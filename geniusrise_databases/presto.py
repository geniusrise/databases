import prestodb
from geniusrise import BatchOutput, Spout, State


class Presto(Spout):
    def __init__(self, output: BatchOutput, state: State, **kwargs):
        r"""
        Initialize the Presto class.

        Args:
            output (BatchOutput): An instance of the BatchOutput class for saving the data.
            state (State): An instance of the State class for maintaining the state.
            **kwargs: Additional keyword arguments.

        ## Using geniusrise to invoke via command line
        ```bash
        genius Presto rise \
            batch \
                --output_folder /path/to/output \
                --bucket my_bucket \
                --s3_folder s3/folder \
            none \
            fetch \
                --args host=mypresto.example.com username=myusername password=mypassword catalog=mycatalog schema=myschema table=mytable
        ```

        ## Using geniusrise to invoke via YAML file
        ```yaml
        version: "1"
        spouts:
            my_presto_spout:
                name: "Presto"
                method: "fetch"
                args:
                    host: "mypresto.example.com"
                    username: "myusername"
                    password: "mypassword"
                    catalog: "mycatalog"
                    schema: "myschema"
                    table: "mytable"
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
        catalog: str,
        schema: str,
        table: str,
    ):
        """
        📖 Fetch data from a Presto table and save it in batch.

        Args:
            host (str): The Presto host.
            username (str): The Presto username.
            password (str): The Presto password.
            catalog (str): The Presto catalog name.
            schema (str): The Presto schema name.
            table (str): The name of the Presto table.

        Raises:
            Exception: If unable to connect to the Presto server or execute the command.
        """
        # Initialize the Presto connection
        try:
            conn = prestodb.connect(host=host, user=username, password=password)
        except Exception as e:
            self.log.error(f"Error connecting to Presto server: {e}")
            return

        # Perform the Presto operation
        try:
            cursor = conn.cursor()
            cursor.execute(f"SELECT * FROM {catalog}.{schema}.{table}")

            while True:
                row = cursor.fetchone()
                if not row:
                    break

                # Save the fetched row to a file
                self.output.save(row)

            # Update the state
            current_state = self.state.get_state(self.id) or {
                "success_count": 0,
                "failure_count": 0,
            }
            current_state["success_count"] += 1
            self.state.set_state(self.id, current_state)

        except Exception as e:
            self.log.error(f"Error fetching data from Presto: {e}")

            # Update the state
            current_state = self.state.get_state(self.id) or {
                "success_count": 0,
                "failure_count": 0,
            }
            current_state["failure_count"] += 1
            self.state.set_state(self.id, current_state)

        finally:
            conn.close()
