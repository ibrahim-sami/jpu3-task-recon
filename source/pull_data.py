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

    if event['attributes']:
        try:
            _date = event['attributes']['_date']
            step = int(event['attributes']['step'])
            logger.debug(f"Pulling step {step} data for {_date}")

            parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir))
            query_dir = os.path.join(parent_dir, 'queries')
            credentials_filepath = os.path.join(parent_dir, 'credentials', 'database.ini')

            if step == 1:
                query_filename = 'step_1_submission_counts.sql'
            elif step == 2:
                query_filename = 'step_2_submission_counts.sql'
            else:
                raise Exception("Error invalid step provided. Expected '1' or '2'")
            query_filepath = os.path.join(query_dir, query_filename)
            
            results = execute_postgres_query(
                credentials_filepath=credentials_filepath,
                query_filepath=query_filepath,
                query_params={'_date':_date}
            )
            df = pd.DataFrame(data=results, columns=list(COLUMNS.keys()))
            df['project_id'] = df['project_id'].astype(str)
            
            logger.debug(f'{len(df.index)} records pulled')

            project = 'hub-data-295911'
            dataset = 'jpu3_task_recon'

            table = query_filename.split('.')[0]
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
                write_disposition='WRITE_APPEND', 
                schema=schema)
            logger.debug(errors)
            end = time.time()
            # logger.debug(f'Function execution took {(end-start)/60} mins')

        except KeyError as e:
            raise Exception("KeyError. Invalid attributes supplied.")
    else:
        raise Exception('KeyError. No attributes supplied.')
     

if __name__ == '__main__':
    test = {
        "attributes":dict(
            _date='2021-11-13',
            step='1'
            )
    }
    execute(test, None)