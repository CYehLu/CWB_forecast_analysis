import sqlite3

conn = sqlite3.connect('county_36hr_forecast.sqlite')
c = conn.cursor()
c.execute('CREATE TABLE county '
          + '(id INTEGER PRIMARY KEY AUTOINCREMENT, '
          + 'issueTime TEXT, location TEXT, element TEXT, '
          + 'startTime TEXT, endTime TEXT, value TEXT)')
conn.commit()
conn.close()