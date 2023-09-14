from azure.cosmosdb.table.models import Entity
from azure.cosmosdb.table.tableservice import TableService
from geniusrise import BatchOutput, Spout, State


class AzureTableStorage(Spout):
    def __init__(self, output: BatchOutput, state: State, **kwargs):
        r"""
        Initialize the AzureTableStorage class.

        Args:
            output (BatchOutput): An instance of the BatchOutput class for saving the data.
            state (State): An instance of the State class for maintaining the state.
            **kwargs: Additional keyword arguments.

        ## Using geniusrise to invoke via command line
        ```bash
        genius AzureTableStorage rise \
            batch \
                --output_folder /path/to/output \
                --bucket my_bucket \
                --s3_folder s3/folder \
            none \
            fetch \
                --args account_name=my_account account_key=my_key table_name=my_table
        ```

        ## Using geniusrise to invoke via YAML file
        ```yaml
        version: "1"
        spouts:
            my_azure_table_spout:
                name: "AzureTableStorage"
                method: "fetch"
                args:
                    account_name: "my_account"
                    account_key: "my_key"
                    table_name: "my_table"
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

    def fetch(self, account_name: str, account_key: str, table_name: str):
        """
        📖 Fetch data from Azure Table Storage and save it in batch.

        Args:
            account_name (str): The Azure Storage account name.
            account_key (str): The Azure Storage account key.
            table_name (str): The Azure Table Storage table name.

        Raises:
            Exception: If unable to connect to Azure Table Storage or fetch the data.
        """
        table_service = TableService(account_name=account_name, account_key=account_key)

        try:
            entities = table_service.query_entities(table_name)
            fetched_data = [Entity(e) for e in entities]

            # Save the fetched rows to a file
            self.output.save(fetched_data)

            # Update the state
            current_state = self.state.get_state(self.id) or {
                "success_count": 0,
                "failure_count": 0,
            }
            current_state["success_count"] += 1
            self.state.set_state(self.id, current_state)

            # Log the total number of rows processed
            self.log.info(f"Total rows processed: {len(fetched_data)}")

        except Exception as e:
            self.log.error(f"Error fetching data from Azure Table Storage: {e}")

            # Update the state
            current_state = self.state.get_state(self.id) or {
                "success_count": 0,
                "failure_count": 0,
            }
            current_state["failure_count"] += 1
            self.state.set_state(self.id, current_state)
