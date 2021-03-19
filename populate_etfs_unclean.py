import configuration
import psycopg2
import psycopg2.extras
import csv


connection = psycopg2.connect(host=configuration.DB_HOST, database=configuration.DB_NAME, user=configuration.DB_USER, password=configuration.DB_PASS)

cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)

#get the stock table where only the is_etf column is True, i.e. the row that contains our etf BTEK
cursor.execute('SELECT * FROM stock WHERE is_etf = TRUE')

etfs = cursor.fetchall()

print(etfs) #it works!

#return the rows in the "stock" table that is_etf is True.
for etf in etfs:
    print(etf)



#return only the symbol in the stock table for each row where is_etf is True.
for etf in etfs:
    print(etf['symbol'])

#using the etf symbol ('BTEK') open the csv file of the etf fund at the latest date, 
#and then print each row in the csv file.
for etf in etfs:
    print(etf['symbol'])
    with open(f"data/26-02-2021/{etf['symbol']}.csv") as file:
        reader = csv.reader(file)
        for row in reader:
            print(row)

#using the etf symbol ('BTEK') open the csv file of the etf fund at the latest date, 
#and then print each row in the csv file BUT don't include the first row which is the headers (so you only get the data)
for etf in etfs:
    print(etf['symbol'])
    with open(f"data/26-02-2021/{etf['symbol']}.csv") as file:
        reader = csv.reader(file)
        next(reader)
        for row in reader:
            print(row)


#first, we're opening up and looking through each directory (i.e. the date folders)
#using the etf symbol ('BTEK') open the csv file of the etf fund at the latest date, 
#and then print each row in the csv file BUT don't include the first row which is the headers (so you only get the data).
#and then insert our csv data into the "etf_holdings" data by
# getting the ticker in each row 
# getting the number of shares in each row 
# getting the percent held in each row 

from download_tidy_etfdata import all_dates


for date in all_dates:
    for etf in etfs:
        # print(etf['symbol'])

        with open("data/{}/{}.csv".format(date, etf['symbol'])) as file:
            reader = csv.reader(file)
            # next(reader)

            for row in reader:
                # print(row)
                ticker = row[3]
                percent_held = row [8]
                num_shares = row[10]
                cursor.execute(""" 
                    SELECT * FROM stock WHERE symbol = '%s'   
                """ %(ticker))
                stock = cursor.fetchone()

                if stock:
                    cursor.execute("""
                    INSERT INTO etf_holding (etf_id, holding_id, dt, num_shares, percent_held)
                    VALUES (%s, %s, %s, %s, %s)
                    """, (etf['id'], stock['id'], date, num_shares, percent_held))

connection.commit()




# import pandas as pd

# for date in all_dates:
#     for etf in etfs:
#         df = pd.read_csv('data/{}/{}.csv'.format(date,etf['symbol']))
#         for index, row in df.iterrows():
#             print(row['Ticker'])

#             cursor.execute(""" 
#                 SELECT * FROM stock WHERE symbol = 'TSLA'
#             """)%(row['Ticker']))
#             stock = cursor.fetchone()
#             if stock:
#                 cursor.execute("""
#                     INSERT INTO etf_holding (etf_id, holding_id, dt, num_shares, percent_held)
#                     VALUES (%s, %s, %s, %s, %s)
#                     """, (etf['id'], stock['id'], date, num_shares, percent_held))


