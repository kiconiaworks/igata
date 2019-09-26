import logging
import os
from typing import List

import psycopg2

from . import OutputCtxManagerBase

logger = logging.getLogger("cliexecutor")

# database configuration
DB_NAME = os.getenv("DB_NAME", None)
DB_HOST = os.getenv("DB_HOST", None)
DB_PORT = os.getenv("DB_PORT", 5432)
DB_USER = os.getenv("DB_USER", None)
DB_PASSWORD = os.getenv("DB_PASSWORD", None)
DATABASE_CONFIGURATION = {"dbname": DB_NAME, "dbhost": DB_HOST, "dbport": DB_PORT, "dbuser": DB_USER, "dbpass": DB_PASSWORD}


class PostgresqlRdsOutputCtxManager(OutputCtxManagerBase):
    """Context manager for outputting results to a reachable postgresql database table"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dbname = kwargs["dbname"]
        self.dbhost = kwargs["dbhost"]
        self.dbport = kwargs["dbport"]
        self.dbuser = kwargs["dbuser"]
        self.dbpass = kwargs["dbpass"]
        self.tablename = kwargs.get("tablename", "image_detected_objects")
        self.connection = psycopg2.connect(database=self.dbname, host=self.dbhost, port=self.dbport, user=self.dbuser, password=self.dbpass)
        self.cursor = self.connection.cursor()
        self._output_rows = 0

    @classmethod
    def required_kwargs(cls):
        """
        Define the fields that are required on instantiation.
        These fields by be defined as EnvironmentVariables by prefixing with 'OUTPUT_CTXMGR_' and putting values in uppercase

        Ex:

            OUTPUT_CTXMGR_DBNAME

        """
        names = ("dbname", "dbhost", "dbport", "dbuser", "dbpass")
        return names

    def put_records(self, records: List[dict]) -> bool:
        """
        Build INSERT statement.
        Data committed on exit of context manager.
        """
        for record in records:
            # build SQL
            fieldnames = []
            fieldvalues = []
            for fieldname, fieldvalue in record.items():
                fieldnames.append(fieldname)
                fieldvalues.append(fieldvalue)

            fields_str = ", ".join(fieldnames)
            dummy_fields_str = ", ".join(["%s"] * len(fieldvalues))
            sql = f"INSERT INFO {self.tablename} ({fields_str}) VALUES({dummy_fields_str})"

            self.cursor.execute(sql, fieldvalues)
            self._output_rows += 1
        return True

    def __enter__(self):
        # start commit
        sql = "BEGIN TRANSACTION;"
        self.cursor.execute(sql)
        return self

    def __exit__(self, *args):
        # make sure that any remaining records are put
        # --> records added byt the `` defined in OutputCtxManagerBase where self._record_results is populated
        if self._record_results:
            logger.debug(f"put_records(): {len(self._record_results)}")
            self.put_records(self._record_results)

        sql = "END TRANSACTION;"
        self.cursor.execute(sql)
        self.connection.commit()
        self.connection.close()
        logger.info(f"committed rows: {self._output_rows}")
