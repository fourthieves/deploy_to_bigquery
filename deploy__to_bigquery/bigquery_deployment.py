#!/usr/bin/env python3

from google.cloud import bigquery
from google.cloud.bigquery import Dataset
from google.cloud.exceptions import Conflict
from google.cloud.exceptions import Forbidden
from pathlib import Path
import logging


class BigQueryDeploy:

    def __init__(self, project, creds_file):
        self.project = project
        self.creds_file = creds_file
        self.client = bigquery.Client.from_service_account_json(str(creds_file))

    def create_view_from_sql_file(self, destination_data_set, file_path, substitutions=None,
                                  log_error_and_continue=False):
        """
        :param substitutions: Optional, If used, this parameter should be in the format of a dictionary for
        substituting values into SQL templates.  The value will be substituted in where the key is represented in the
        SQL template. I.e. `{key} will be replaced with value`
        :param destination_data_set: the data set that the view should be made in
        :param file_path: the path of the SQL file containing the data for the view
        :param log_error_and_continue: Boolean - This defaults to False meaning the method will throw an exception if
        it encounters an error.  This can be overridden to True and it will instead log the error and continue with the
        rest of the deployment
        :return: 0 indicates success
        """

        p = Path(file_path)

        sql_template = p.read_text()

        destination_data_set_ref = self.client.dataset(destination_data_set)

        view_ref = destination_data_set_ref.table(p.stem)
        view = bigquery.Table(view_ref)

        # Check that this works when substitutions is none

        view.view_query = sql_template.format(**substitutions)

        try:
            view = self.client.create_table(view)  # API request
            logging.info('Successfully created view at {}'.format(view.full_table_id))
        except Conflict:
            try:

                # If billing is not enabled, then this will still fail

                view = self.client.update_table(view, ['view_query'])  # API request
                logging.info('Successfully updated view at {}'.format(view.full_table_id))

            except Forbidden as f:

                if log_error_and_continue:
                    logging.exception('Error updating view ' + str(p.stem) + ' - ' + str(f))
                    logging.info('this happened')
                    return 0

                else:

                    raise Exception

        except Exception as e:

            if log_error_and_continue:
                logging.exception('Error creating view ' + str(p.stem) + ' - ' + str(e))

            else:
                raise Exception

        return 0

    def create_data_set(self, data_set_name):
        """
        :param data_set_name: str - The name of the dataset to be created
        :return: 0 indicates success
        """

        data_set_ref = self.client.dataset(data_set_name)
        data_set = Dataset(data_set_ref)
        data_set.description = ''
        data_set.location = 'EU'

        try:
            self.client.create_dataset(data_set)  # API request
            logging.info('Data set - ' + data_set_name + ' successfully created')
        except Conflict:
                logging.info('Data set - ' + data_set_name + ' already exists')

        return 0

    def create_views_from_file(self, views_directory, substitutions=None, log_error_and_continue=False):
        """
        This will iterate through a directory and create a structure of views and datasets.
        It will only work 1 folder deep as there is no sub structure beneath a dataset.
        Each folder will represent a data set in BigQuery and each SQL file will generate a
        view of the same name.  It will be  assumed that each SELECT statement will be
        electing from a single data set and project.  These should be represented in the
        query as {project} and {data set} so that the string formatter can replace them
        appropriately.

        :param views_directory: str or path - The directory to be iterated through
        :param substitutions: optional - a dictionary of substitutions for SQL template
        :param log_error_and_continue: Boolean - This defaults to False meaning the method will throw an exception if
        it encounters an error.  This can be overridden to True and it will instead log the error and continue with the
        rest of the deployment
        :return: 0 indicates success
        """

        logging.info(f'views_directory - {views_directory}')

        views_directory = Path(views_directory)

        for child in views_directory.iterdir():
            data_set_name = child.name
            data_set_path = views_directory / data_set_name

            self.create_data_set(data_set_name)

            for sql in data_set_path.iterdir():
                sql_file_name = sql.name
                sql_file_path = data_set_path / sql_file_name

                logging.info(f'sql_file_path - {sql_file_path}')

                self.create_view_from_sql_file(data_set_name, sql_file_path, substitutions, log_error_and_continue)

        return 0
