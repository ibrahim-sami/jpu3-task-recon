import os
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
from google.cloud import bigquery
from pytz import timezone

from utils import setup_logging, execute_postgres_query, push_to_bigq

COLUMNS = {
    'project_id':'STRING',
    'submission_date':'DATE',
    'task_id':'STRING',
    'shape_type':'STRING',
    'last_submission_count':'INT64',
    'last_submission_shape_count':'INT64'
}

bigquery.enums.SqlTypeNames

def execute(event, context):
    start = time.time()
    logger = setup_logging(name=Path(__file__).stem)
    logger.debug('Executing . . .')

    parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir))
    query_dir = os.path.join(parent_dir, 'queries')
    credentials_filepath = os.path.join(parent_dir, 'credentials', 'database.ini')
    query_filename = 'all_submission_counts_9129.sql'
    query_filepath = os.path.join(query_dir, query_filename)
    
    logger.debug(f'Pulling JPU3 tasks for query {os.path.basename(query_filepath)}')
    results = execute_postgres_query(
        credentials_filepath=credentials_filepath,
        query_filepath=query_filepath,
        query_params=None
    )
    df = pd.DataFrame(data=results, columns=list(COLUMNS.keys()))
    df['project_id'] = df['project_id'].astype(str)
    logger.debug(f'DF DTYPES: {df.dtypes}')
    logger.debug(f'{len(df.index)} records pulled')

    project = 'hub-data-295911'
    dataset = 'jpu3_task_recon'
    table = query_filename.split('.')[0]
    logger.debug(f'Pushing data to big query at {project}.{dataset}.{table}')
    logger.debug('Building schema. . .')
    schema = []
    for col_name, col_type in COLUMNS.items():
        schema.append(bigquery.SchemaField(name=col_name, field_type=col_type))
    df['ingestion_timestamp'] = datetime.now(tz=timezone('Africa/Nairobi'))
    schema.append(bigquery.SchemaField(name='ingestion_timestamp', field_type='DATETIME'))
    errors = push_to_bigq(
        df=df, 
        project=project, 
        dataset=dataset, 
        table=table, 
        write_disposition='WRITE_TRUNCATE', # TODO revert write disposition
        schema=schema)
    logger.debug(errors)
    end = time.time()
    logger.debug(f'Function execution took {(end-start)/60} mins') 

if __name__ == '__main__':
    execute(None, None)