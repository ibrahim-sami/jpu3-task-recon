from datetime import date
from pathlib import Path
from google.cloud import pubsub_v1
import pandas as pd
import time
from google.cloud import bigquery

from utils import setup_logging

def execute(event, context):
    logger = setup_logging(Path(__file__).stem)
    logger.debug('Getting date ranges. . .')


    project_id = 'hub-data-295911'
    dataset_id = 'jpu3_task_recon'
    topic_id = 'jpu3-task-recon'

    start_date_step_1 = '2021-10-01'
    end_date_step_1 = '2021-11-19'

    start_date_step_2 = '2021-11-20'
    end_date_step_2 = '2022-5-10'

    with pubsub_v1.PublisherClient() as publisher:
        topic_path = publisher.topic_path(project_id, topic_id)
        logger.debug(f'Pushing date ranges to Pub/Sub topic {topic_path}')

        # logger.debug('Deleting step 1 and step 2 tables for WRITE_APPEND disposition')
        # client = bigquery.Client(project=project_id)

        # table_id = f'{project_id}.{dataset_id}.step_1_submission_counts'
        # client.delete_table(table_id, not_found_ok=True)  
        # logger.debug("Deleted table '{}'.".format(table_id))

        # table_id = f'{project_id}.{dataset_id}.step_2_submission_counts'
        # client.delete_table(table_id, not_found_ok=True)  
        # logger.debug("Deleted table '{}'.".format(table_id))
        
        all_dates = []
        logger.debug('Generating dates for step 1')
        dates = list(pd.date_range(
            start=start_date_step_1,
            end=end_date_step_1,
            freq='d').strftime('%Y-%m-%d'))
        dates = [{d:'1'} for d in dates]
        all_dates.extend(dates)
        logger.debug(f'{len(dates)} generated for step 1')

        logger.debug('Generating dates for step 2')
        dates = list(pd.date_range(
            start=start_date_step_2,
            end=end_date_step_2,
            freq='d').strftime('%Y-%m-%d'))
        date_dict = [{d:'2'} for d in dates]
        all_dates.extend(date_dict)
        logger.debug(f'{len(dates)} generated for step 2')

        for d in all_dates:
            _date, step = d.popitem()
            logger.debug(f"Date: {_date} published for step {step}")
            future = publisher.publish(topic=topic_path, 
                                        data=b'Date', 
                                        _date=_date,
                                        step=step)
            try:
                future.result()
            except Exception as ex:
                future.cancel()
                raise ex
            time.sleep(3)
        logger.debug(f'{len(all_dates)} published for both step 1 and 2')
        return 'Success'

if __name__ == "__main__":
    execute(None, None)

    