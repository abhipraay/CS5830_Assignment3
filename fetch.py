import requests
from bs4 import BeautifulSoup
from zipfile import ZipFile
import random

def get_data(**context):
    
    year = 2023
    url = f'https://www.ncei.noaa.gov/data/local-climatological-data/access/{year}/'

    ## Requesting the API
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    rows = soup.find("table").find_all("tr")[2:-2]

    fileName = []

    total = 10

    for i in range(total):
        index = random.randint(0, len(rows))
        data = rows[index].find_all("td")
        fileName.append(data[0].text)
    
    ## Writing binary data into the local directory

    for name in fileName:
        newUrl = url+name
        response = requests.get(newUrl)
        open(name,'wb').write(response.content)

    ## Zipping the files

    with ZipFile('/root/airflow/DAGS/weather.zip','w') as zip:
        for file in fileName:
            zip.write(file)


## Function to unzip the files
def unzip(**context):
   with ZipFile("/root/airflow/DAGS/weather.zip", 'r') as zObject: 
      zObject.extractall(path="/root/airflow/DAGS/files") 
