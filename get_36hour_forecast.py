import sqlite3
import requests
from bs4 import BeautifulSoup
import pandas as pd


def issue_time_lastrow(dbname):
    conn = sqlite3.connect(dbname)
    c = conn.cursor()
    c.execute('SELECT * FROM county ORDER BY id DESC LIMIT 1')
    row = c.fetchall()
    if row == []:
        # empty db
        return None
    else:
        return row[0][1]

def parse_data_into_df(data, df):
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
    return df


# read data
with open('my_cwb_apikey.txt') as f:
    apikey = f.readline()
format_ = 'json'
dataid = 'F-C0032-001'   # 一般天氣預報-今明36小時天氣預報

url = f'https://opendata.cwb.gov.tw/fileapi/v1/opendataapi/{dataid}?Authorization={apikey}&format={format_}'
resp = requests.get(url)
data = resp.json()

# get some information
issue_time = data['cwbopendata']['dataset']['datasetInfo']['issueTime']
update = data['cwbopendata']['dataset']['datasetInfo']['update']
locations = data['cwbopendata']['dataset']['location']

# check if data has existed in db. if no, append data to db
last_issue = issue_time_lastrow('county_36hr_forecast.sqlite')
if not last_issue == issue_time:
    # convert into df
    df = pd.DataFrame()
    df = parse_data_into_df(data, df)
    print('First 8 data this time: ')
    print(df.head(8))
    
    # append data into db
    conn = sqlite3.connect('county_36hr_forecast.sqlite')
    df.to_sql('county', conn, if_exists='append', index=False)
    conn.close()
    print('SUCCESS!')
else:
    print('data has existed')