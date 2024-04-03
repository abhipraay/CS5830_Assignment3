import logging 
import numpy as np
import geopandas as gpd
from geodatasets import get_path
import apache_beam as beam
from aggregate import Aggregated
import os
from ast import literal_eval as make_tuple
import matplotlib.pyplot as plt

## Plotting geomaps
def plot_geomaps(values):
    logger = logging.getLogger(__name__)
    logger.info(values)
    data = np.array(values[1],dtype='float')
    d1 = np.array(data,dtype='float')
    
    world = gpd.read_file(get_path('naturalearth.land'))

    data = gpd.GeoDataFrame({
        values[0]:d1[:,2]
    }, geometry=gpd.points_from_xy(*d1[:,(1,0)].T))
    
    fig, ax = plt.subplots(1, 1, figsize=(10, 5))
    
    data.plot(column=values[0], cmap='viridis', marker='o', markersize=150, ax=ax, legend=True)
    ax.set_title(f'{values[0]} Heatmap')
    os.makedirs('/root/airflow/DAGS/results/plots', exist_ok=True)
    
    plt.savefig(f'/root/airflow/DAGS/results/plots{values[0]}_heatmap_plot.png')


## Function to use beam to create plots
def create_heatmap_visualization(**kwargs):
    
    required_fields = ["LATITUDE","LONGITUDE","HourlyDryBulbTemperature"]
    with beam.Pipeline(runner='DirectRunner') as p:
        
        result = (
            p
            | 'ReadProcessedData' >> beam.io.ReadFromText('/root/airflow/DAGS/files/results/averages.txt*')
            | 'preprocessParse' >>  beam.Map(lambda a:make_tuple(a.replace('nan', 'None')))
            | 'Global aggregation' >> beam.CombineGlobally(Aggregated(required_fields = required_fields))
            | 'Flat Map' >> beam.FlatMap(lambda a:a) 
            | 'Plot Geomaps' >> beam.Map(plot_geomaps)            
        )
        