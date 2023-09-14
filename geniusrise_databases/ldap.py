import ldap
from geniusrise import BatchOutput, Spout, State


class LDAP(Spout):
    def __init__(self, output: BatchOutput, state: State, **kwargs):
        r"""
        Initialize the LDAP class.

        Args:
            output (BatchOutput): An instance of the BatchOutput class for saving the data.
            state (State): An instance of the State class for maintaining the state.
            **kwargs: Additional keyword arguments.

        ## Using geniusrise to invoke via command line
        ```bash
        genius LDAP rise \
            batch \
                --output_folder /path/to/output \
                --bucket my_bucket \
                --s3_folder s3/folder \
            none \
            fetch \
                --args url=ldap://myldap.example.com:389 bind_dn="cn=admin,dc=example,dc=com" bind_password="password" search_base="dc=example,dc=com" search_filter="(objectClass=person)" attributes=["cn", "givenName", "sn"]
        ```

        ## Using geniusrise to invoke via YAML file
        ```yaml
        version: "1"
        spouts:
            my_ldap_spout:
                name: "LDAP"
                method: "fetch"
                args:
                    url: "ldap://myldap.example.com:389"
                    bind_dn: "cn=admin,dc=example,dc=com"
                    bind_password: "password"
                    search_base: "dc=example,dc=com"
                    search_filter: "(objectClass=person)"
                    attributes: ["cn", "givenName", "sn"]
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
        bind_dn: str,
        bind_password: str,
        search_base: str,
        search_filter: str,
        attributes: list,
    ):
        """
        📖 Fetch data from an LDAP server and save it in batch.

        Args:
            url (str): The LDAP URL.
            bind_dn (str): The DN to bind as.
            bind_password (str): The password for the DN.
            search_base (str): The search base.
            search_filter (str): The search filter.
            attributes (list): The list of attributes to retrieve.

        Raises:
            Exception: If unable to connect to the LDAP server or execute the search.
        """
        # Initialize the LDAP connection
        try:
            connection = ldap.initialize(url)
            connection.bind_s(bind_dn, bind_password)
        except ldap.LDAPError as e:
            self.log.error(f"Error binding to LDAP server: {e}")
            return

        # Perform the search
        try:
            search_result = connection.search_s(search_base, ldap.SCOPE_SUBTREE, search_filter, attributes)
        except ldap.LDAPError as e:
            self.log.error(f"Error searching LDAP server: {e}")
            return

        # Save the search results to a file
        self.output.save(search_result)

        # Update the state
        current_state = self.state.get_state(self.id) or {
            "success_count": 0,
            "failure_count": 0,
        }
        current_state["success_count"] += 1
        self.state.set_state(self.id, current_state)
