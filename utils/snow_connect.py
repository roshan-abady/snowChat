from typing import Any, Dict

import streamlit as st
from snowflake.snowpark.session import Session
from snowflake.snowpark.version import VERSION


class SnowflakeConnection:
    """
    This class is used to establish a connection to Snowflake.

    Attributes
    ----------
    connection_parameters : Dict[str, Any]
        A dictionary containing the connection parameters for Snowflake.
    session : snowflake.snowpark.Session
        A Snowflake session object.

    Methods
    -------
    get_session()
        Establishes and returns the Snowflake connection session.

    """

    def __init__(self):
        self.connection_parameters = self._get_connection_parameters_from_env()
        self.session = None

    @staticmethod
    def _get_connection_parameters_from_env() -> Dict[str, Any]:
        connection_parameters = {
            "account": st.secrets["snowflake"]["ACCOUNT"],
            "user": st.secrets["snowflake"]["USER"],
            "password": st.secrets["snowflake"]["PASSWORD"],
            "warehouse": st.secrets["snowflake"]["WAREHOUSE"],
            "database": st.secrets["snowflake"]["DATABASE"],
            "schema": st.secrets["snowflake"]["SCHEMA"],
            "role": st.secrets["snowflake"]["ROLE"],
            "authenticator": st.secrets["snowflake"]["AUTHENTICATOR"],
        }
        return connection_parameters

    def get_session(self):
        """
        Establishes and returns the Snowflake connection session.
        Returns:
            session: Snowflake connection session.
        """
        if self.session is None:
            self.session = Session.builder.configs(self.connection_parameters).create()
            self.session.sql_simplifier_enabled = True
        return self.session
