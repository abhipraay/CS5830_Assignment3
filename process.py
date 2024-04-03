import apache_beam as beam
import os
from parse import parseCSV
from extract import ExtractAndFilterFields, ExtractFieldsWithMonth



## Function to run beam for getting fields from csv
def process_csv(**kwargs):
    required_fields = ["LATITUDE","LONGITUDE","HourlyDryBulbTemperature"]
    os.makedirs('/root/airflow/DAGS/files/results', exist_ok=True)
    with beam.Pipeline(runner='DirectRunner') as p:
        result = (
            p
            | 'ReadCSV' >> beam.io.ReadFromText('/root/airflow/DAGS/files/*.csv')
            | 'ParseData' >> beam.Map(parseCSV)
            | 'FilterAndCreateTuple' >> beam.ParDo(ExtractAndFilterFields(required_fields=required_fields))
            | 'CombineTuple' >> beam.GroupByKey()
            | 'UnpackTuple' >> beam.Map(lambda a:(a[0][0],a[0][1],a[1]))
        )

        result | 'WriteToText' >> beam.io.WriteToText('/root/airflow/DAGS/files/result.txt')