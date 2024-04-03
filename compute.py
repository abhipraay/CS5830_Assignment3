import numpy as np
import pandas as pd
import logging
import os
import apache_beam as beam
from parse import parseCSV
from extract import ExtractAndFilterFields, ExtractFieldsWithMonth

## Function to calculate averages
def compute_avg(data):
    val_data = np.array(data[1])
    val_data_shape = val_data.shape
    val_data = pd.to_numeric(val_data.flatten(), errors='coerce',downcast='float') 
    val_data = np.reshape(val_data,val_data_shape)
    masked_data = np.ma.masked_array(val_data, np.isnan(val_data))
    res = np.ma.average(masked_data, axis=0)
    res = list(res.filled(np.nan))
    logger = logging.getLogger(__name__)
    logger.info(res)
    return ((data[0][1],data[0][2]),res)


## Function to use beam to compute the averages
def compute_monthly_avg( **kwargs):
    required_fields = ["LATITUDE","LONGITUDE","HourlyDryBulbTemperature"]
    os.makedirs('/tmp/results', exist_ok=True)
    with beam.Pipeline(runner='DirectRunner') as p:
        result = (
            p
            | 'ReadProcessedData' >> beam.io.ReadFromText('/root/airflow/DAGS/files/*.csv')
            | 'ParseData' >> beam.Map(parseCSV)
            | 'CreateTupleWithMonthInKey' >> beam.ParDo(ExtractFieldsWithMonth(required_fields=required_fields))
            | 'CombineTupleMonthly' >> beam.GroupByKey()
            | 'ComputeAverages' >> beam.Map(lambda data: compute_avg(data))
            | 'CombineTuplewithAverages' >> beam.GroupByKey()
            | 'UnpackTuple' >> beam.Map(lambda a:(a[0][0],a[0][1],a[1]))
        )
        result | 'WriteAveragesToText' >> beam.io.WriteToText('/root/airflow/DAGS/files/results/averages.txt')