#You don't need to change anything with this file
import yaml
from pathlib import Path
from google.cloud import bigquery
import os
from jinja2 import Environment, FileSystemLoader

class SqlTemplate:
    # Init
    def __init__(self, sql_file, bq_client, params, setup_scripts_directory):
        """Initiates variables

        Args:
            sql_file (str): absolute file path
            bq_client (_type_): BQ client object
            params (_type_): json config for rendering SQL files
        """
        self.sql_file = sql_file
        self.bq_client = bq_client
        self.params = params
        self.setup_scripts_directory = setup_scripts_directory

    # Template and release
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

    # Deploy to BQ
    def execute(self):
        """Executes sql statment on BigQuery

        Returns:
            RowIterator: RowIterator
        """
        sql = self.generate_template()
        job = self.bq_client.query(sql)

        # wait for completion
        return job.result()

    def get_file_list(self):
        """Iterates recursively to find all sql files and returns file list in order:
            create_datasets, create_tables, create_views, update_tables

        Returns:
            PosixPath: List of files in the directory
        """

        file_list = []
        create_datasets_files = []
        create_table_files = []
        create_view_files = []
        update_files = []

        # add create datasets list at the top
        for file in self.setup_scripts_directory.rglob("*.sql"):
            if "create_dataset" in file.name:
                create_datasets_files.append(file.resolve())
            elif "create_table" in file.name:
                create_table_files.append(file.resolve())
            elif "create_view" in file.name:
                create_view_files.append(file.resolve())
            else:
                update_files.append(file.resolve())
        # file list in order
        # datasets -> Tables -> views -> update column, table metadata
        file_list = create_datasets_files
        file_list.extend(create_table_files)
        file_list.extend(create_view_files)
        file_list.extend(update_files)

        return file_list


def load_config(folder: str, wop_region: str, wop_stage: str, wop_regulation: str) -> dict:

    dag_file = Path(__file__)
    file_yaml = None
    if wop_regulation == 'fedramp' and wop_region == 'us':
        file_yaml = (
            f'{folder}/regional_config/region_{wop_region}_{wop_regulation}_{wop_stage}.yaml'
        )
    else:
        file_yaml = f'{folder}/regional_config/region_{wop_region}_{wop_stage}.yaml'

    with open(dag_file.parents[0] / file_yaml, "r") as f:
        return yaml.safe_load(f)


def setup_tables(folders, op_stage, op_region, op_regulation):
    """Renders and create data objects in BigQuery using regional configs

    Args:
        op_stage (str): This provides environment type, either production or staging
        op_region (str): This porvides region info where data resides
        op_regulation (str): This provides whether the dag is running in
                             commerical or FedRAMP
    """

    env_params = {
        "op_region": op_region,
        "op_stage": op_stage,
        "op_regulation": op_regulation,
    }

    for folder in folders:

        config = load_config(folder, op_region, op_stage, op_regulation)

        params = {**env_params, **config}
        params['project'] = config['project_destination']
        print(params)

        file_yaml = f"{folder}/regional_config/mapping_region_bq"
        with open(Path(__file__).parents[0] / f"{file_yaml}.yaml", "r") as f:
            region_mapping = yaml.safe_load(f)

        #Determine BQ region
        params['bq_region'] = region_mapping[op_region]

        if op_stage == "staging":
            params['op_region_name'] = "test"


        with bigquery.Client(
            project=params['project'], location=params['bq_region']
        ) as client:
            # absolute path to setup script
            current_directory = Path(__file__).parent.resolve()
            setup_scripts_directory = current_directory / f"{folder}/setup"

            # dummy file for obj creation
            file_path = ""

            # get SQLTemplate
            custom_sql_templater = SqlTemplate(
                file_path, client, params, setup_scripts_directory
            )

            # get file list in order
            current_working_directory = custom_sql_templater.get_file_list()

            # for template_file in setup_scripts_directory.rglob('create_table.sql'):
            for template_file in current_working_directory:
                print(template_file)
                # update file path with actual file
                custom_sql_templater.sql_file = template_file

                # templated sql script
                templated_sql = custom_sql_templater.generate_template()
                print(templated_sql)

                # run scripts in BQ
                job_result = custom_sql_templater.execute()
                print(job_result)


if __name__ == "__main__":

    # get region, stage from env variables
    wop_region = os.getenv('WOP_REGION')
    # wop_region = "us"

    wop_stage = os.getenv('WOP_STAGE')
    # wop_stage = "staging"

    wop_regulation = os.getenv('WOP_REGULATION', "commercial")
    # wop_regulation = "FEDRAMP"

    import os
    # Get the current directory
    current_directory = os.getcwd()

    # List all folders, excluding ".git"
    folders = [
        name for name in os.listdir(current_directory)
        if os.path.isdir(os.path.join(current_directory, name)) and name != ".git" and name != ".gitlab"
    ]

    if wop_region and wop_stage and wop_regulation:
        setup_tables(
            folders=folders, op_stage=wop_stage, op_region=wop_region, op_regulation=wop_regulation
        )
    else:
        print('check op variables')
