import psycopg2

#Create table
conn = psycopg2.connect("dbname=fuel" "user=postgres")
cur = conn.cursor()
cur.execute("CREATE TABLE PRICE (DATE DATE, LOCATION_NAME VARCHAR(45), FUEL_TYPE VARCHAR(15), PRICE DECIMAL(3,2), ADDRESS VARCHAR(40), SUBURB VARCHAR(30), POSTCODE INT, SITE_FEATURES VARCHAR(80), PRIMARY KEY(DATE, LOCATION_NAME, FUEL_TYPE)")

#Insert file data into table

#Test to see if data is in DB

#Save changes to DB
conn.commit()
#Clean up
cur.close()
conn.close()