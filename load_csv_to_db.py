#!/usr/bin/env python3
import psycopg2
from datetime import datetime
import requests

CSV_LOCATION = "csv/"
CSV_DOWNLOAD_NAME = "download"

DB_NAME = 'fuel'
DB_HOST = '127.0.0.1'
DB_USERNAME = 'postgres'
DB_PASSWORD = 'postgres'

# Print iterations progress, From: https://stackoverflow.com/a/34325723
def printProgressBar (iteration, total, prefix = '', suffix = '', decimals = 1, length = 100, fill = '█', printEnd = "\r"):
    """
    Call in a loop to create terminal progress bar
    @params:
        iteration   - Required  : current iteration (Int)
        total       - Required  : total iterations (Int)
        prefix      - Optional  : prefix string (Str)
        suffix      - Optional  : suffix string (Str)
        decimals    - Optional  : positive number of decimals in percent complete (Int)
        length      - Optional  : character length of bar (Int)
        fill        - Optional  : bar fill character (Str)
        printEnd    - Optional  : end character (e.g. "\r", "\r\n") (Str)
    """
    percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
    filledLength = int(length * iteration // total)
    bar = fill * filledLength + '-' * (length - filledLength)
    print(f'\r{prefix} |{bar}| {percent}% {suffix}', end = printEnd)
    # Print New Line on Complete
    if iteration == total: 
        print()


def download(save_location):
    #Needs zfill to add leading zeros if required.
    month = str(datetime.now().month).zfill(2)
    year = datetime.now().year
    url = "https://warsydprdstafuelwatch.blob.core.windows.net/historical-reports/FuelWatchRetail-{0}-{1}.csv".format(month,year)
    print("Downloading %s" % (url,))
    r = requests.get(url, allow_redirects=True)
    open(CSV_LOCATION + save_location + ".csv", 'wb').write(r.content)
    print(url)


def create_table():
    try:
        #Create table
        conn = psycopg2.connect("dbname=%s user=%s password=%s host=%s" % (DB_NAME,DB_USERNAME,DB_PASSWORD,DB_HOST))
        cur = conn.cursor()
        cur.execute("CREATE TABLE PRICE (DATE DATE, LOCATION_NAME VARCHAR(60), FUEL_TYPE VARCHAR(15), PRICE DECIMAL(5,2), ADDRESS VARCHAR(40), SUBURB VARCHAR(30), POSTCODE INT, PRIMARY KEY(DATE, LOCATION_NAME, FUEL_TYPE));")

    except(psycopg2.errors.DuplicateTable):
        print("Price table found.")

    #Save changes to DB
    conn.commit()
    #Clean up
    cur.close()
    conn.close()


def insert(file_name):
    file_path = CSV_LOCATION + file_name + ".csv"
    already_exist = 0

    #Insert file data into table
    print("Opening: %s" % (file_path,))
    with open(file_path) as csv:
        #Remove the first line:
        string = csv.readline()
        lines = []
        for string in csv:
            lines.append(string.split(","))
        #PUBLISH_DATE,TRADING_NAME,BRAND_DESCRIPTION,PRODUCT_DESCRIPTION,PRODUCT_PRICE,ADDRESS,LOCATION,POSTCODE,AREA_DESCRIPTION,REGION_DESCRIPTION
        #DB wants: date | location_name | fuel_type | price | address | suburb | postcode 
        
    sql = "INSERT INTO PRICE (DATE,LOCATION_NAME,FUEL_TYPE,PRICE,ADDRESS,SUBURB,POSTCODE) VALUES (%s, %s, %s, %s, %s, %s, %s)"
    print("Adding to DB")

    length = len(lines)
    for iteration, line in enumerate(lines):
        printProgressBar(iteration,length)
                #date,location, fueltype,price, address, suburb,postcode
        data = (line[0],line[1],line[3],line[4],line[5],line[6], line[7])
        try:
            conn = psycopg2.connect("dbname=%s user=%s password=%s host=%s" % (DB_NAME,DB_USERNAME,DB_PASSWORD,DB_HOST))
            cur = conn.cursor()
            cur.execute(sql, data)
            #Save changes to DB
            conn.commit()

        except psycopg2.errors.UniqueViolation:
            #print("Key: [%s,%s,%s] not added, already in DB" % (line[0],line[1],line[2]))
            already_exist += 1
    
    print("%s/%s Already in DB" % (already_exist, len(lines)))
    #Clean up
    cur.close()
    conn.close()

create_table()
download(CSV_DOWNLOAD_NAME)
insert(CSV_DOWNLOAD_NAME)