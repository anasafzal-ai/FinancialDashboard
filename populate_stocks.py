import configuration
import alpha_vantage
from alpha_vantage.timeseries import TimeSeries
import psycopg2


# lets go ahead and see if we can just establish a connection to our TimeScale PostGreSQL database 
connection = psycopg2.connect(host=configuration.DB_HOST, database=configuration.DB_NAME, user=configuration.DB_USER, password=configuration.DB_PASS)

#to make life easier, rather than have cursor return tuples we can change it to dicts
#to do this, we import psycopg2.extras, and specify DictCursor for our cursor
import psycopg2.extras

cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)

#trying to get dataframe of all active symnbols available on Alpha Vantage 
import pandas 
import json
import numpy

api = TimeSeries(key=configuration.API_KEY)

import requests

base_url = 'https://www.alphavantage.co/query?'
params = {'function': 'LISTING_STATUS',
          'datatype': 'csv', 
         'apikey': configuration.API_KEY}

response = requests.get(base_url, params=params)


with open('activestocks.csv', 'wb') as file:
	file.write(response.content)

df = pandas.read_csv('activestocks.csv') #Create pandas dataframe


#Let's clean up the data a little bit, and deal with missing values
universe = df[['name', 'symbol', 'assetType','exchange']].copy()

universe.isnull().any()

df_prep = universe.dropna().copy()

df_prep['is_etf'] = numpy.where(df_prep['assetType']=='ETF', True, False)

df_final = df_prep[['name', 'symbol','exchange', 'is_etf']].copy()

#now we can populate stock table in SQL through iterating over each row in the dataframe and passing it to the stock table. 
for index, row in df_final.iterrows():
    cursor.execute("""
    INSERT INTO stock (full_name, symbol, exchange, is_etf)
    VALUES (%s, %s, %s, false)
    """, (row['name'], row['symbol'], row['exchange']))

connection.commit() #use commit as the final save function which then permanantly stores the records into the stock table