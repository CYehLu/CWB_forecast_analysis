import sqlite3

conn = sqlite3.connect('obs_rainfall.sqlite')
c = conn.cursor()
c.execute(
    'CREATE TABLE rainobs '
    + '(id INTEGER PRIMARY KEY AUTOINCREMENT, '
    + 'sentTime TEXT, stationName TEXT, stationId TEXT, '
    + 'lon TEXT, lat TEXT, obsTime TEXT, '
    + 'rain1hr TEXT, rain10min TEXT, rain3hr TEXT, rain6hr TEXT, '
    + 'rain12hr TEXT, rain24hr TEXT, rainToday TEXT, '
    + 'fromLast1dayToNow TEXT, fromLast2dayToNow TEXT)'
)
conn.commit()
conn.close()