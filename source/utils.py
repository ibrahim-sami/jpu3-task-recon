import logging
from google.cloud import bigquery
import os
from google.cloud import logging as cloudlogging
import psycopg2
from jinjasql import JinjaSql
from configparser import ConfigParser 
import jinja2


def truncate_table(project, dataset, table, schema):

    bqclient = bigquery.Client(project=project)
    table_ref = bigquery.TableReference.from_string(f"{project}.{dataset}.{table}")
    bqclient.delete_table(table_ref, not_found_ok=True)

  
    table = bigquery.Table(table_ref, schema=schema)
    table = bqclient.create_table(table)
    
    return "success"


def push_to_bigq(df, project, dataset, table, write_disposition=None, schema=None):
    if not write_disposition:
        raise Exception('Explicitly provide the write disposition')

    if df.empty:
        return "Empty DF"

    # credentials from the service_account.json are not used
    # authentication is done automatically through the Google Cloud SDK
    client = bigquery.Client(project=project)

    table_id = project + '.' + dataset + '.' + table
    if schema:
        job_config = bigquery.LoadJobConfig(
            write_disposition=write_disposition, 
            autodetect=False,
            schema=schema)
    else:
        job_config = bigquery.LoadJobConfig(
            write_disposition=write_disposition, 
            autodetect=True)

    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    errors = job.result().error_result
    return errors


def setup_logging(name):
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    lg_client = cloudlogging.Client()
    lg_handler = lg_client.get_default_handler()
    # lg_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    # lg_handler.setFortmatter(lg_format)

    c_handler = logging.StreamHandler()
    c_format = logging.Formatter('%(name)s - %(levelname)s - %(message)s')
    c_handler.setFormatter(c_format)

    logger.addHandler(c_handler)
    logger.addHandler(lg_handler)

    return logger


def load_credentials(filename, section):
    parser = ConfigParser()
    db = {}

    # try to get secret variable 
    # for deployment on Google Cloud Function
    secret = os.environ.get('AWS_POSTGRES_CONFIG')
    if secret:
        parser.read_string(secret)
        if parser.has_section(section):
            params = parser.items(section)
            for param in params:
                db[param[0]] = param[1]
        else:
            raise Exception('Section {0} not found in the secret environment variable'.format(section))
    else:
        # read config file 
        # for local execution
        parser.read(filename)
        if parser.has_section(section):
            params = parser.items(section)
            for param in params:
                db[param[0]] = param[1]
        else:
            raise Exception('Section {0} not found in the {1} file'.format(section, filename))
 
    return db


def execute_postgres_query(credentials_filepath, query_filepath, query_params):
    credentials = load_credentials(filename=credentials_filepath, section='postgresql')
    conn = psycopg2.connect(**credentials)
    cursor = conn.cursor()  

    # bind query parameters
    j = JinjaSql(param_style='pyformat')
    query_template = open(query_filepath, 'r').read()
    if query_params:
        fin_query, bind_params = j.prepare_query(query_template, query_params)

        # execute query
        try:
            cursor.execute(fin_query, vars=bind_params)
            query_results = cursor.fetchall()
        except psycopg2.ProgrammingError as exc:
            query_results = None
            print(exc)
            conn.rollback()
        except psycopg2.InterfaceError as exc:
            query_results = None
            print(exc)
            conn = psycopg2.connect(**credentials)
            cursor = conn.cursor()
        finally:
            cursor.close()
            conn.close()
            
    # execute query without parameters
    else:
        try:
            cursor.execute(query_template)
            query_results = cursor.fetchall()
        except psycopg2.ProgrammingError as exc:
            query_results = None
            print(exc)
            conn.rollback()
        except psycopg2.InterfaceError as exc:
            query_results = None
            print(exc)
            conn = psycopg2.connect(**credentials)
            cursor = conn.cursor()
        finally:
            cursor.close()
            conn.close()


    return query_results


def bq_insert_rows(rows_to_insert, project, dataset, table):
    # credentials from the service_account.json are not used
    # authentication is done automatically through the Google Cloud SDK
    client = bigquery.Client(project=project)

    dataset_ref = client.dataset(dataset)

    table_ref = dataset_ref.table(table)
    table = client.get_table(table_ref)  # API call

    errors = client.insert_rows_json(table, rows_to_insert)  # API request
    return errors