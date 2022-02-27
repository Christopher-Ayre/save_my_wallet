#!/usr/bin/env python3
import psycopg2
from datetime import date, timedelta

# import fuelwatch.fuelwatch as fw

# opts3 = {'Product': 1, 'Suburb': "Queens Park"}
# url3 = fw.generate_url(opts3)
# data3 = fw.getdata(url3)
# results3 = fw.parse(data3)
# print("url3: ", url3)
# print(results3)

def average(data):
    length = len(data)
    average = 0.0

    for line in data:
        average += float(line[3])

    average = average/length

    return average

def ten_onehundred(postcode,fuel_type):
    conn = psycopg2.connect("dbname=fuel user=postgres")
    cur = conn.cursor()
    #100 days ago was:
    today = date.today()
    delta = timedelta(days=100)
    hundred_days_ago = today - delta
    print(hundred_days_ago)
    print(today)

    sql = "SELECT * FROM PRICE WHERE POSTCODE=%s AND FUEL_TYPE=%s AND DATE BETWEEN %s AND %s ORDER BY DATE DESC"
    data = (postcode,fuel_type, hundred_days_ago, today)
    cur.execute(sql, data)
    #data contains all prices from the required postcode and fuel type from the last 100days
    data = cur.fetchall()
    #Clean up
    cur.close()
    conn.close()

    #Need to get average for ten days and 100 days then compare the two
    
    hundred_days_average = average(data)

    print(hundred_days_average)



ten_onehundred(6107,"ULP")