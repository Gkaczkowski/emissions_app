# Functions for connecting to and interacting with snowflake
#from common.settings import *

import os
from tempfile import NamedTemporaryFile
from pandas import DataFrame, to_datetime
import snowflake.connector


class Snowflake(object):
    """
    Connect to Snowflake. Fetch SQL results as data frames, or send external data to snowflake.
    to-do: create_table, create_stage, update to_table to create stage if DNE
    """
    def __init__(self):
        self.user = os.environ['SNOWFLAKE_USER']
        self.password = os.environ['SNOWFLAKE_PASSWORD']
        self.database = os.environ['SNOWFLAKE_DATABASE']
        self.warehouse = os.environ['SNOWFLAKE_WAREHOUSE']
        self.account = os.environ['SNOWFLAKE_ACCOUNT']

    def __get_connection(self):
        """
        Connects to Snowflake; use in a with statement to ensure connection closes.
        """
        return snowflake.connector.connect(user=self.user,
                                           password=self.password,
                                           account=self.account,
                                           database=self.database,
                                           warehouse=self.warehouse)

    def fetch_sql_df(self, sql:str) -> DataFrame:
        """
        query snowflake and return the result as a dataframe
        """
        with self.__get_connection() as conn:
            with conn.cursor() as curr:
                curr = curr.execute(sql)
                results = curr.fetchall()
                cols = [c[0].lower() for c in curr.description]
                return DataFrame(results, columns=cols)

    def __to_staging(self, df:DataFrame, schema:str, stage:str, incremental:bool=False, staging_suffix:str=None):
        """
        Send a dataframe to an existing staging bucket.
        Set incremental to True to keep the old staging file.
        """
        schema_stage = schema + '.' + stage

        use_schema = 'USE %s.%s;' % (self.database, schema) # Set target schema in connection's database

        if incremental == False:
            remove_staged_file = 'REMOVE @%s;' % schema_stage # Command to clear the old staged file

        if staging_suffix:
            suffix = '_' + staging_suffix
        else:
            now = to_datetime('now')
            suffix = now.strftime('_%Y%m%d_%H%M')

        with NamedTemporaryFile(suffix = suffix, mode='r+') as temp:
            df.to_csv(temp.name, index = False)

            put_staged_file = 'PUT file://%s @%s;' % (temp.name, stage)

            with self.__get_connection() as conn:
                with conn.cursor() as curr:
                    # Multiple SQL statements in a single API call are not supported
                    curr.execute(use_schema)

                    if incremental == False:
                        curr.execute(remove_staged_file) # execute remove unless incremental

                    curr.execute(put_staged_file)

    def __stage_to_table(self, schema:str, stage:str, table:str, incremental:bool=False):
        """
        Copy data from an existing staging bucket into a table.
        Set incremental to append data to True to append new data rather than overwrite old data.
        """
        schema_table = schema + '.' + table

        # Set target schema
        use_schema = 'USE %s.%s;' % (self.database, schema)

        # truncate rows from table, insert new ones from staging
        if incremental == False:
            truncate_table = 'TRUNCATE TABLE IF EXISTS %s;' % schema_table # Command to clear the old staged file

        copy_into_table = 'COPY INTO %s FROM @%s FILE_FORMAT = (TYPE = CSV skip_header = 1 EMPTY_FIELD_AS_NULL = TRUE);' % (schema_table, stage)

        with self.__get_connection() as conn:
            with conn.cursor() as curr:
                # Multiple SQL statements in a single API call are not supported
                curr.execute(use_schema)

                if incremental == False:
                    curr.execute(truncate_table) # execute truncate unless incremental

                curr.execute(copy_into_table)

    def to_table(self, df:DataFrame, schema:str, table:str, staging_suffix:str=None, incremental:bool=False):
        """
        Send a dataframe to a staging bucket and then copy it into a table.
        Set incremental to True to keep files currently in the staging bucket and append new data to the table.
        Set staging_suffix to add a suffix to files loaded into staging, else they will be given a temp name.
        """
        stage = table # placeholder in the case that we want to allow user to set separate stage name

        self.__to_staging(df, schema, stage, incremental, staging_suffix)
        self.__stage_to_table(schema, stage, table, incremental)