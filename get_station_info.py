import json
import requests
from bs4 import BeautifulSoup


station_dict = {}

resp = requests.get('https://www.cwb.gov.tw/V7/observe/rainfall/Rain_Hr/22.htm')
soup = BeautifulSoup(str(resp.content, 'utf-8'), features='html.parser')
trs = soup.find_all('tr')
stations = trs[4:]

for station in stations:
    county = station.find('td').text[:3]
    name = station.find_all('td')[1].text
    print(county, name)
    station_dict[name] = county
    
with open('station_info.json', 'w') as jsonfile:
    json.dump(station_dict, jsonfile)