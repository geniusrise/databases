from typing import Any, Dict

from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from geniusrise import BatchOutput, Spout, State


class Cassandra(Spout):
    def __init__(self, output: BatchOutput, state: State, **kwargs: Any) -> None:
        r"""
        Initialize the Cassandra class.

        Args:
            output (BatchOutput): An instance of the BatchOutput class for saving the data.
            state (State): An instance of the State class for maintaining the state.
            **kwargs: Additional keyword arguments.

        ## Using geniusrise to invoke via command line
        ```bash
        genius Cassandra rise \
            batch \
                --output_folder /path/to/output \
                --bucket my_bucket \
                --s3_folder s3/folder \
            none \
            fetch \
                --args hosts=localhost keyspace=my_keyspace query="SELECT * FROM my_table" page_size=100
        ```

        ## Using geniusrise to invoke via YAML file
        ```yaml
        version: "1"
        spouts:
            my_cassandra_spout:
                name: "Cassandra"
                method: "fetch"
                args:
                    hosts: "localhost"
                    keyspace: "my_keyspace"
                    query: "SELECT * FROM my_table"
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
        hosts: str,
        keyspace: str,
        query: str,
        page_size: int = 100,
    ) -> None:
        """
        📖 Fetch data from a Cassandra database and save it in batch.

        Args:
            hosts (str): Comma-separated list of Cassandra hosts.
            keyspace (str): The Cassandra keyspace to use.
            query (str): The CQL query to execute.
            page_size (int): The number of rows to fetch per page. Defaults to 100.

        Raises:
            Exception: If unable to connect to the Cassandra cluster or execute the query.
        """
        # Initialize Cassandra connection
        cluster = Cluster(contact_points=hosts.split(","))
        session = cluster.connect(keyspace)

        try:
            statement = SimpleStatement(query, fetch_size=page_size)
            rows = session.execute(statement)
            processed_rows = 0

            for row in rows:
                # Save the fetched rows to a file
                self.output.save(dict(row))

                # Update the number of processed rows
                processed_rows += 1
                self.log.debug(f"Processed row {processed_rows}")

            # Update the state
            current_state: Dict[str, Any] = self.state.get_state(self.id) or {
                "success_count": 0,
                "failure_count": 0,
                "processed_rows": 0,
            }
            current_state["success_count"] += 1
            current_state["processed_rows"] = processed_rows
            self.state.set_state(self.id, current_state)

            self.log.info(f"Total rows processed: {processed_rows}")

        except Exception as e:
            self.log.error(f"Error fetching data from Cassandra: {e}")

            # Update the state
            current_state = self.state.get_state(self.id) or {
                "success_count": 0,
                "failure_count": 0,
                "processed_rows": 0,
            }
            current_state["failure_count"] += 1
            self.state.set_state(self.id, current_state)

        finally:
            session.shutdown()
            cluster.shutdown()
