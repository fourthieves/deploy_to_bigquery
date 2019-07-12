from pathlib import Path
from deploy__to_bigquery.bigquery_deployment import BigQueryDeploy
import config as cfg
import logging

logging.basicConfig(level=logging.INFO)

config = cfg.prod

project = config['project']

# Define location of BigQuery Credentials file name and location
creds_folder = Path(config['creds_folder'])
creds_file_name = config['creds_file_name']
creds_file = creds_folder / creds_file_name

views_directory = 'Views'


def run():

    bq_dep = BigQueryDeploy(project, creds_file)

    # Run jobs

    substitutions = {
        'project': project,
        'bq_public_data_set': 'bigquery-public-data'
    }

    bq_dep.create_views_from_file(views_directory, substitutions, True)


if __name__ == "__main__":

    run()
