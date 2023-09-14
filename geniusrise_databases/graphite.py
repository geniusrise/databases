import requests
from geniusrise import BatchOutput, Spout, State


class Graphite(Spout):
    def __init__(self, output: BatchOutput, state: State, **kwargs):
        r"""
        Initialize the Graphite class.

        Args:
            output (BatchOutput): An instance of the BatchOutput class for saving the data.
            state (State): An instance of the State class for maintaining the state.
            **kwargs: Additional keyword arguments.

        ## Using geniusrise to invoke via command line
        ```bash
        genius Graphite rise \
            batch \
                --output_folder /path/to/output \
                --bucket my_bucket \
                --s3_folder s3/folder \
            none \
            fetch \
                --args url=http://localhost:8080 target=stats_counts.myapp output_format=json from=-1h until=now
        ```

        ## Using geniusrise to invoke via YAML file
        ```yaml
        version: "1"
        spouts:
            my_graphite_spout:
                name: "Graphite"
                method: "fetch"
                args:
                    url: "http://localhost:8080"
                    target: "stats_counts.myapp"
                    output_format: "json"
                    from: "-1h"
                    until: "now"
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
        url: str,
        target: str,
        output_format: str = "json",
        from_time: str = "-1h",
        until: str = "now",
    ):
        """
        📖 Fetch data from a Graphite database and save it in batch.

        Args:
            url (str): The Graphite API URL.
            target (str): The target metric to fetch.
            output_format (str): The output format. Defaults to "json".
            from_time (str): The start time for fetching data. Defaults to "-1h".
            until (str): The end time for fetching data. Defaults to "now".

        Raises:
            Exception: If unable to connect to the Graphite server or fetch the data.
        """
        params = {
            "target": target,
            "format": output_format,
            "from": from_time,
            "until": until,
        }

        try:
            response = requests.get(f"{url}/render", params=params)
            response.raise_for_status()

            data = response.json()

            # Save the fetched data to a file
            self.output.save(data)

            # Update the state
            current_state = self.state.get_state(self.id) or {
                "success_count": 0,
                "failure_count": 0,
            }
            current_state["success_count"] += 1
            self.state.set_state(self.id, current_state)

            # Log the total number of data points fetched
            self.log.info(f"Total data points fetched: {len(data)}")

        except Exception as e:
            self.log.error(f"Error fetching data from Graphite: {e}")

            # Update the state
            current_state = self.state.get_state(self.id) or {
                "success_count": 0,
                "failure_count": 0,
            }
            current_state["failure_count"] += 1
            self.state.set_state(self.id, current_state)
