import ibm_db
from geniusrise import BatchOutput, Spout, State


class DB2(Spout):
    def __init__(self, output: BatchOutput, state: State, **kwargs):
        r"""
        Initialize the DB2 class.

        Args:
            output (BatchOutput): An instance of the BatchOutput class for saving the data.
            state (State): An instance of the State class for maintaining the state.
            **kwargs: Additional keyword arguments.

        ## Using geniusrise to invoke via command line
        ```bash
        genius DB2 rise \
            batch \
                --output_folder /path/to/output \
                --bucket my_bucket \
                --s3_folder s3/folder \
            none \
            fetch \
                --args hostname=mydb2.example.com port=50000 username=myusername password=mypassword database=mydb
        ```

        ## Using geniusrise to invoke via YAML file
        ```yaml
        version: "1"
        spouts:
            my_db2_spout:
                name: "DB2"
                method: "fetch"
                args:
                    hostname: "mydb2.example.com"
                    port: 50000
                    username: "myusername"
                    password: "mypassword"
                    database: "mydb"
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
        hostname: str,
        port: int,
        username: str,
        password: str,
        database: str,
    ):
        """
        📖 Fetch data from a DB2 database and save it in batch.

        Args:
            hostname (str): The DB2 hostname.
            port (int): The DB2 port.
            username (str): The DB2 username.
            password (str): The DB2 password.
            database (str): The DB2 database name.

        Raises:
            Exception: If unable to connect to the DB2 server or execute the command.
        """
        # Initialize the DB2 connection
        try:
            conn = ibm_db.connect(
                hostname,
                username,
                password,
                database,
                port=port,
                ibm_db_ibuf_size=1024 * 1024,
            )
        except Exception as e:
            self.log.error(f"Error connecting to DB2 server: {e}")
            return

        # Perform the DB2 operation
        try:
            stmt = ibm_db.exec_immediate(conn, "SELECT * FROM mytable")

            while True:
                row = ibm_db.fetch_assoc(stmt)
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
            self.log.error(f"Error fetching data from DB2: {e}")

            # Update the state
            current_state = self.state.get_state(self.id) or {
                "success_count": 0,
                "failure_count": 0,
            }
            current_state["failure_count"] += 1
            self.state.set_state(self.id, current_state)

        finally:
            ibm_db.close(conn)
