import time
import logging
import os
from jinja2 import Environment, FileSystemLoader

logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO").upper())

class SqlTemplate:
    def __init__(self, sql_file, bq_client, params):
        """Initiates variables

        Args:
            sql_file (str): absolute file path
            bq_client (_type_): BQ client object
            params (_type_): json config for rendering SQL files
        """
        dag_directory = os.path.dirname(os.path.abspath(__file__))
        self.sql_file = os.path.join(dag_directory, sql_file)
        self.bq_client = bq_client
        self.params = params

    def generate_template(self):
        """Generates rendered sql by substituting jinja variables and/or expressions

        Returns:
            str: rendered sql file
        """
        if self.sql_file == "" or self.sql_file is None:
            raise Exception("Invalid sql path")

        filename = os.path.basename(self.sql_file)
        parent = os.path.dirname(self.sql_file)
        file_loader = FileSystemLoader(parent)
        env = Environment(loader=file_loader)

        template = env.get_template(filename)

        output = template.render(params=self.params)

        return output

    def execute(self):
        """Executes sql statment on BigQuery

        Returns:
            RowIterator: RowIterator object
        """
        sql = self.generate_template()
        job = self.bq_client.query(sql)

        while job.state not in ['DONE', 'FAILURE']:
            time.sleep(1)
            job.reload() # Check if the job is done and handle any errors

        if job.state == 'DONE':
            if job.error_result:
                raise Exception(f"Query error in {self.sql_file} file. The error: {job.error_result['message']}")
            else:
                logging.info(f"Query in {self.sql_file} file executed successfully.")
                return job.result()
        elif job.state == 'FAILURE':
            raise Exception(f"Query failed in {self.sql_file} file with FAILED state.")
        else:
            raise Exception(f"Query failed in {self.sql_file} file with UNKNOWN state.")
