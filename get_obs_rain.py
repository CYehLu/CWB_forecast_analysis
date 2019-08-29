import sqlite3
import requests
from bs4 import BeautifulSoup
import pandas as pd


def sent_time_lastrow(dbname):
    conn = sqlite3.connect(dbname)
    c = conn.cursor()
    c.execute('SELECT * FROM rainobs ORDER BY id DESC LIMIT 1')
    row = c.fetchall()
    if row == []:
        # empty db
        return None
    else:
        return row[0][1]
    
def parse_obs_into_df(data):
    res = []

    sent_time = data['cwbopendata']['sent']
    locations = data['cwbopendata']['location']
    for loc in locations:
        lon = loc['lon']
        lat = loc['lat']
        loc_name = loc['locationName']
        station_id = loc['stationId']
        obs_time = loc['time']['obsTime']
        elements = loc['weatherElement']
        for e in elements:
            if e['elementName'] == 'RAIN':
                rain = e['elementValue']['value']
            elif e['elementName'] == 'MIN_10':
                min_10 = e['elementValue']['value']
            elif e['elementName'] == 'HOUR_3':
                hour_3 = e['elementValue']['value']
            elif e['elementName'] == 'HOUR_6':
                hour_6 = e['elementValue']['value']
            elif e['elementName'] == 'HOUR_12':
                hour_12 = e['elementValue']['value']
            elif e['elementName'] == 'HOUR_24':
                hour_24 = e['elementValue']['value']
            elif e['elementName'] == 'NOW':
                now = e['elementValue']['value']
            elif e['elementName'] == 'latest_2days':
                lastest_2days = e['elementValue']['value']
            elif e['elementName'] == 'latest_3days':
                lastest_3days = e['elementValue']['value']
        res.append(
            (sent_time, loc_name, station_id, lon, lat, obs_time, 
             rain, min_10, hour_3, hour_6, hour_12, hour_24, now, lastest_2days, lastest_3days)
        )

    df = pd.DataFrame(res)
    df.columns = [
        'sentTime', 'stationName', 'stationId', 'lon', 'lat', 'obsTime',
        'rain1hr', 'rain10min', 'rain3hr', 'rain6hr', 'rain12hr', 'rain24hr', 'rainToday', 
        'fromLast1dayToNow', 'fromLast2dayToNow'
    ]
    return df


with open('my_cwb_apikey.txt') as f:
    apikey = f.readline()
format_ = 'json'
dataid = 'O-A0002-001'   # 自動雨量站-雨量觀測資料

url = f'https://opendata.cwb.gov.tw/fileapi/v1/opendataapi/{dataid}?Authorization={apikey}&format={format_}'
resp = requests.get(url)
data = resp.json()

sent_time = data['cwbopendata']['sent']
last_sent = sent_time_lastrow('obs_rainfall.sqlite')

if not last_sent == sent_time:
    # convert into df
    df = parse_obs_into_df(data)
    
    # append data into db
    conn = sqlite3.connect('obs_rainfall.sqlite')
    df.to_sql('rainobs', conn, if_exists='append', index=False)
    conn.close()
    print('SUCCESS!')
else:
    print('data has existed')