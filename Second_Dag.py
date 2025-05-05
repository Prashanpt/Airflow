# Sample Dag - 2 (Second_Dag) [ This dag contains only one task for better understanding]

# Target - To create a table in Bigquery  and then populate it with some data , but this time not with hardcoded values,
# but by defining Airflow variables, which are global variable which can be used in any dag

# operator used --> GoogleCloudStorageToBigQueryOperator ( It takes fixed argument which is defined below)

#

from airflow import DAG
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.providers.google.cloud.hooks.gcs import GCSHook
import json
 
# we have already defined few airflow in Airflow UI, which are also mentioned below for our reference
# we are doing this to so as to avoid hardcoding, this will be helpful incase of migration
#  
# key --> pt_dag_variable
# value --> "gcs_bucket":"packt-data-eng-on-gcp-databuckett", "raw_dataset":"raw_bikesharing" 

settings = Variable.get("pt_dag_variable", deserialize_json=True)    # we will use this airflow variable in our code

# setting will now be a dictionary, containing all our variables needed for code

# study about get function to understand the above code.


# Function to convert json text file to list of dictioanry in python 
def json_to_dict(bucket_name,schema_file_path):
    gcs_hook = GCSHook()


    schema_content = gcs_hook.download(bucket_name=bucket_name, object_name=schema_file_path)
# Parse JSON string into Python list of dicts
    schema_fields = json.loads(schema_content)
    return schema_fields

schema_fields=json_to_dict(settings['gcs_bucket'],'schema/stations_schema.json')

args = {
    'owner': 'prashant-tripathi',
}



with DAG(
    dag_id='second_dag_by_pt',
    default_args=args,
    schedule_interval='0 5 * * *',
    start_date=days_ago(1),
) as dag:


      
    gcs_to_bq_example = GoogleCloudStorageToBigQueryOperator(
    task_id                             = "create_table_and_load_data",
    bucket                              = settings['gcs_bucket'],
    source_objects                      = ['mysql_export/stations/20180101/stations.csv'], # From where you want to load the data 
    destination_project_dataset_table   =f'{settings["raw_dataset"]}.station_new',
    schema_fields=schema_fields,
    write_disposition='WRITE_TRUNCATE'
    )
 
