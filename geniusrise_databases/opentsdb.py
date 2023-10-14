# 🧠 Geniusrise
# Copyright (C) 2023  geniusrise.ai
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import opentsdb
from geniusrise import BatchOutput, Spout, State


class OpenTSDB(Spout):
    def __init__(self, output: BatchOutput, state: State, **kwargs):
        r"""
        Initialize the OpenTSDB class.

        Args:
            output (BatchOutput): An instance of the BatchOutput class for saving the data.
            state (State): An instance of the State class for maintaining the state.
            **kwargs: Additional keyword arguments.

        ## Using geniusrise to invoke via command line
        ```bash
        genius OpenTSDB rise \
            batch \
                --output_s3_bucket my_bucket \
                --output_s3_folder s3/folder \
            none \
            fetch \
                --args host=http://localhost:4242
        ```

        ## Using geniusrise to invoke via YAML file
        ```yaml
        version: "1"
        spouts:
            my_opentsdb_spout:
                name: "OpenTSDB"
                method: "fetch"
                args:
                    host: "http://localhost:4242"
                output:
                    type: "batch"
                    args:
                        bucket: "my_bucket"
                        s3_folder: "s3/folder"
        ```
        """
        super().__init__(output, state)
        self.top_level_arguments = kwargs

    def fetch(
        self,
        host: str,
    ):
        """
        📖 Fetch data from an OpenTSDB database and save it in batch.

        Args:
            host (str): The URL of the OpenTSDB instance.

        Raises:
            Exception: If unable to connect to the OpenTSDB server or execute the query.
        """
        # Initialize OpenTSDB client
        client = opentsdb.Client(host)

        try:
            # Connect to the database
            with client:
                # Get the number of metrics in the database
                metric_count = len(client.metrics())

                # Iterate through each metric in the database
                cursor = client.metrics()
                processed_metrics = 0

                while True:
                    # Get a batch of metrics
                    batch = list(cursor.batch(100))

                    # Check if there are any metrics in the batch
                    if not batch:
                        break

                    # Save the batch of metrics to a file
                    self.output.save(batch)

                    # Update the number of processed metrics
                    processed_metrics += len(batch)
                    self.log.info(f"Total metrics processed: {processed_metrics}/{metric_count}")

                # Update the state
                current_state = self.state.get_state(self.id) or {
                    "success_count": 0,
                    "failure_count": 0,
                    "processed_metrics": 0,
                }
                current_state["success_count"] += 1
                current_state["processed_metrics"] = processed_metrics
                self.state.set_state(self.id, current_state)

            # Log the total number of metrics processed
            self.log.info(f"Total metrics processed: {processed_metrics}/{metric_count}")

        except Exception as e:
            self.log.error(f"Error fetching data from OpenTSDB: {e}")

            # Update the state
            current_state = self.state.get_state(self.id) or {
                "success_count": 0,
                "failure_count": 0,
                "processed_metrics": 0,
            }
            current_state["failure_count"] += 1
            self.state.set_state(self.id, current_state)
