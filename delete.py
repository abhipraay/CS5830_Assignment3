import shutil
## Function to delete csv
def delete_csv(**kwargs):
    shutil.rmtree('/root/airflow/DAGS/files')