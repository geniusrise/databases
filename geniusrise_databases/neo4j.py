import neo4j
from geniusrise import BatchOutput, Spout, State


class Neo4j(Spout):
    def __init__(self, output: BatchOutput, state: State, **kwargs):
        r"""
        Initialize the Neo4j class.

        Args:
            output (BatchOutput): An instance of the BatchOutput class for saving the data.
            state (State): An instance of the State class for maintaining the state.
            **kwargs: Additional keyword arguments.

        ## Using geniusrise to invoke via command line
        ```bash
        genius Neo4j rise \
            batch \
                --output_folder /path/to/output \
                --bucket my_bucket \
                --s3_folder s3/folder \
            none \
            fetch \
                --args host=localhost port=7687 username=myusername password=mypassword
        ```

        ## Using geniusrise to invoke via YAML file
        ```yaml
        version: "1"
        spouts:
            my_neo4j_spout:
                name: "Neo4j"
                method: "fetch"
                args:
                    host: "localhost"
                    port: 7687
                    username: "myusername"
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
        host: str,
        port: int,
        username: str,
        password: str,
    ):
        """
        📖 Fetch data from a Neo4j database and save it in batch.

        Args:
            host (str): The Neo4j host.
            port (int): The Neo4j port.
            username (str): The Neo4j username.
            password (str): The Neo4j password.

        Raises:
            Exception: If unable to connect to the Neo4j server or execute the query.
        """
        # Initialize Neo4j client
        client = neo4j.GraphDatabase.driver(f"bolt://{host}:{port}", auth=(username, password))

        try:
            # Connect to the database
            with client.session() as session:
                # Get the number of nodes and relationships in the database
                node_count = session.run("MATCH (n) RETURN count(n)").single()["count(n)"]
                relationship_count = session.run("MATCH ()-[r]->() RETURN count(r)").single()["count(r)"]

                # Iterate through each node and relationship in the database
                cursor = session.run("MATCH (n) RETURN n, labels(n), properties(n)")
                processed_nodes = 0
                processed_relationships = 0

                while True:
                    # Get a batch of nodes and relationships
                    batch = list(cursor.batch_size(100))

                    # Check if there are any nodes or relationships in the batch
                    if not batch:
                        break

                    # Save the batch of nodes and relationships to a file
                    self.output.save(batch)

                    # Update the number of processed nodes and relationships
                    processed_nodes += len([n for n, _, _ in batch])
                    processed_relationships += len([r for _, _, r in batch])
                    self.log.info(
                        f"Total nodes processed: {processed_nodes}/{node_count}, total relationships processed: {processed_relationships}/{relationship_count}"
                    )

                # Update the state
                current_state = self.state.get_state(self.id) or {
                    "success_count": 0,
                    "failure_count": 0,
                    "processed_nodes": 0,
                    "processed_relationships": 0,
                }
                current_state["success_count"] += 1
                current_state["processed_nodes"] = processed_nodes
                current_state["processed_relationships"] = processed_relationships
                self.state.set_state(self.id, current_state)

            # Log the total number of nodes and relationships processed
            self.log.info(
                f"Total nodes processed: {processed_nodes}/{node_count}, total relationships processed: {processed_relationships}/{relationship_count}"
            )

        except Exception as e:
            self.log.error(f"Error fetching data from Neo4j: {e}")

            # Update the state
            current_state = self.state.get_state(self.id) or {
                "success_count": 0,
                "failure_count": 0,
                "processed_docs": 0,
            }
            current_state["failure_count"] += 1
            self.state.set_state(self.id, current_state)
