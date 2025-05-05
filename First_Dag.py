# Sample Dag - 1 (First_Dag) [ This dag contains only one task for better understanding]

# Target - To create a table in Bigquery  and then populate it with some data

# operator used --> GoogleCloudStorageToBigQueryOperator ( It takes fixed argument which is defined below)


from airflow import DAG
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator

from airflow.utils.dates import days_ago


args = {
    'owner': 'prashant-tripathi',
}




with DAG(
    dag_id='First_dag_by_pt',
    default_args=args,
    schedule_interval='0 5 * * *',
    start_date=days_ago(1), # 
) as dag:


      
    gcs_to_bq_example = GoogleCloudStorageToBigQueryOperator(
    task_id                             = "gcs_to_bq_example",
    bucket                              = "packt-data-eng-on-gcp-databuckett",
    source_objects                      = ['mysql_export/stations/20180101/stations.csv'],
    destination_project_dataset_table   ='raw_bikesharing.stationss',
    schema_fields=[
        {'name': 'station_id', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'name', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'region_id', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'capacity', 'type': 'INTEGER', 'mode': 'NULLABLE'}
    ],
    write_disposition='WRITE_TRUNCATE'
    )
 
# Note: If destination table is not present, it will get created during the run time