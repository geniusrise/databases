from typing import Any, Dict

from elasticsearch import Elasticsearch as ES
from geniusrise import BatchOutput, Spout, State


class Elasticsearch(Spout):
    def __init__(self, output: BatchOutput, state: State, **kwargs: Any) -> None:
        r"""
        Initialize the Elasticsearch class.

        Args:
            output (BatchOutput): An instance of the BatchOutput class for saving the data.
            state (State): An instance of the State class for maintaining the state.
            **kwargs: Additional keyword arguments.

        ## Using geniusrise to invoke via command line
        ```bash
        genius Elasticsearch rise \
            batch \
                --output_folder /path/to/output \
                --bucket my_bucket \
                --s3_folder s3/folder \
            none \
            fetch \
                --args hosts=localhost:9200 index=my_index query='{"query": {"match_all": {}}}' page_size=100
        ```

        ## Using geniusrise to invoke via YAML file
        ```yaml
        version: "1"
        spouts:
            my_elasticsearch_spout:
                name: "Elasticsearch"
                method: "fetch"
                args:
                    hosts: "localhost:9200"
                    index: "my_index"
                    query: '{"query": {"match_all": {}}}'
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
        index: str,
        query: str,
        page_size: int = 100,
    ) -> None:
        """
        📖 Fetch data from an Elasticsearch index and save it in batch.

        Args:
            hosts (str): Comma-separated list of Elasticsearch hosts.
            index (str): The Elasticsearch index to query.
            query (str): The Elasticsearch query in JSON format.
            page_size (int): The number of documents to fetch per page. Defaults to 100.

        Raises:
            Exception: If unable to connect to the Elasticsearch cluster or execute the query.
        """
        # Initialize Elasticsearch connection
        es = ES(hosts.split(","))

        try:
            # Execute the query
            response = es.search(index=index, body=query, size=page_size)
            hits = response["hits"]["hits"]
            processed_docs = 0

            for hit in hits:
                # Save the fetched document to a file
                self.output.save(hit["_source"])

                # Update the number of processed documents
                processed_docs += 1
                self.log.debug(f"Processed document {processed_docs}")

            # Update the state
            current_state: Dict[str, Any] = self.state.get_state(self.id) or {
                "success_count": 0,
                "failure_count": 0,
                "processed_docs": 0,
            }
            current_state["success_count"] += 1
            current_state["processed_docs"] = processed_docs
            self.state.set_state(self.id, current_state)

            self.log.info(f"Total documents processed: {processed_docs}")

        except Exception as e:
            self.log.error(f"Error fetching data from Elasticsearch: {e}")

            # Update the state
            current_state = self.state.get_state(self.id) or {
                "success_count": 0,
                "failure_count": 0,
                "processed_docs": 0,
            }
            current_state["failure_count"] += 1
            self.state.set_state(self.id, current_state)
