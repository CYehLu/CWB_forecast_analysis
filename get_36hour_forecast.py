import sqlite3
import requests
from bs4 import BeautifulSoup
import pandas as pd


with open('my_cwb_apikey.txt') as f:
    apikey = f.readline()
format_ = 'json'
dataid = 'F-C0032-001'   # 一般天氣預報-今明36小時天氣預報
url = f'https://opendata.cwb.gov.tw/fileapi/v1/opendataapi/{dataid}?Authorization={apikey}&format={format_}'

resp = requests.get(url)
data = resp.json()

# extract information from data and store in dataframe
issue_time = data['cwbopendata']['dataset']['datasetInfo']['issueTime']
update = data['cwbopendata']['dataset']['datasetInfo']['update']
locations = data['cwbopendata']['dataset']['location']

df = pd.DataFrame()
for loc in locations:
    loc_name = loc['locationName']
    elements = loc['weatherElement']
    for element in elements:
        element_name = element['elementName']
        for etime in element['time']:
            start_time = etime['startTime']
            end_time = etime['endTime']
            parameter = etime['parameter']['parameterName']
            df = df.append([[issue_time, loc_name, element_name, start_time, end_time, parameter]])
            
df = df.reset_index(drop=True)
df.columns = ['issueTime', 'location', 'element', 'startTime', 'endTime', 'value']

# append data into db
conn = sqlite3.connect('county_36hr_forecast.sqlite')
df.to_sql('county', conn, if_exists='append', index=False)
conn.close()