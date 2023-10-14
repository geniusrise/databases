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

import boto3
from geniusrise import BatchOutput, Spout, State


class AWSKeyspaces(Spout):
    def __init__(self, output: BatchOutput, state: State, **kwargs):
        r"""
        Initialize the AWS Keyspaces class.

        Args:
            output (BatchOutput): An instance of the BatchOutput class for saving the data.
            state (State): An instance of the State class for maintaining the state.
            **kwargs: Additional keyword arguments.

        ## Using geniusrise to invoke via command line
        ```bash
        genius AWSKeyspaces rise \
            batch \
                --output_s3_bucket my_bucket \
                --output_s3_folder s3/folder \
            none \
            fetch \
                --args region_name=us-east-1 cluster_name=mycluster table_name=mytable
        ```

        ## Using geniusrise to invoke via YAML file
        ```yaml
        version: "1"
        spouts:
            my_aws_keyspaces_spout:
                name: "AWSKeyspaces"
                method: "fetch"
                args:
                    region_name: "us-east-1"
                    cluster_name: "mycluster"
                    table_name: "mytable"
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
        region_name: str,
        cluster_name: str,
        table_name: str,
    ):
        """
        📖 Fetch data from an AWS Keyspaces table and save it in batch.

        Args:
            region_name (str): The AWS region name.
            cluster_name (str): The AWS Keyspaces cluster name.
            table_name (str): The name of the AWS Keyspaces table.

        Raises:
            Exception: If unable to connect to the AWS Keyspaces cluster or execute the query.
        """
        # Initialize AWS Keyspaces client
        client = boto3.client("keyspaces", region_name=region_name)

        # Perform the AWS Keyspaces operation
        try:
            response = client.query(
                ClusterName=cluster_name,
                Cql="SELECT * FROM {}".format(table_name),
            )
            data = response["Rows"]

            # Save the query results to a file
            self.output.save(data)

            # Update the state
            current_state = self.state.get_state(self.id) or {
                "success_count": 0,
                "failure_count": 0,
            }
            current_state["success_count"] += 1
            self.state.set_state(self.id, current_state)

        except Exception as e:
            self.log.error(f"Error fetching data from AWS Keyspaces: {e}")

            # Update the state
            current_state = self.state.get_state(self.id) or {
                "success_count": 0,
                "failure_count": 0,
            }
            current_state["failure_count"] += 1
            self.state.set_state(self.id, current_state)
