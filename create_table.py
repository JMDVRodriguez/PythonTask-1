import mysql.connector as msql
from mysql.connector import Error
import pandas as pd

data_frame = pd.read_csv(r"C:/Users/Usuario/Downloads/table_2.csv") 
dp = data_frame.where((pd.notnull(data_frame)), None)

try:
  conn = msql.connect(host='localhost', database='twitter', user='root', password='Cookie123.')
  if conn.is_connected():
    cursor = conn.cursor()
    cursor.execute("select database();")
    record = cursor.fetchone()
    print("You're connected to database: ", record)
    cursor.execute('DROP TABLE IF EXISTS tweets_data_2;')
    print('Creating table....')
    cursor.execute("CREATE TABLE tweets_data_2(id VARCHAR(255),author VARCHAR(255),subreddit VARCHAR(255),date VARCHAR(255),text LONGTEXT,source VARCHAR(255),location VARCHAR(255),negative SMALLINT,neutral SMALLINT,positive SMALLINT,compound SMALLINT(255),analysis VARCHAR(255),keyword VARCHAR(255))")
    print("Table is created....")
    #loop through the data frame
    for i,row in dp.iterrows():
      #here %S means string values 
      sql = "INSERT INTO twitter.tweets_data_2 VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
      cursor.execute(sql, tuple(row))
      print("Record inserted")
      # the connection is not auto committed by default, so we must commit to save our changes
      conn.commit()
except Error as e:
  print("Error while connecting to MySQL", e)
