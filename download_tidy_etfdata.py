# import the necessary libraries to make a directory per date and retrieve the latest csv file through a url request and put it in that date
import os
import urllib
import urllib.request
import datetime
import pandas as pd
from pandas.tseries.offsets import BDay 

#get the latest download date (-1 Business Day from today)
today = datetime.datetime.today()

date = (str(today - BDay(1)))[:10]

DOWNLOAD_DATE = date

#get the necessary pathways to get access to the data
DOWNLOAD_ROOT = "https://www.blackrock.com/us/individual/products/316011/fund/"
HOLDINGS_PATH = os.path.join("/Users/anasafzal/Desktop/BRTECH-track/data",DOWNLOAD_DATE)
HOLDINGS_URL = DOWNLOAD_ROOT + "1464253357814.ajax?fileType=csv&fileName=BTEK_holdings&dataType=fund"

#fetch the data and store it in the correct directory
def fetch_holdings_data(holdings_url=HOLDINGS_URL, holdings_path=HOLDINGS_PATH):
    if not os.path.isdir(holdings_path):
        os.makedirs(holdings_path)
    csv_path = os.path.join(holdings_path, "BTEK.csv")
    urllib.request.urlretrieve(holdings_url, csv_path)



#tidy the csv file up to make it readable for analysis
def tidy_holdings_data(holdings_path=HOLDINGS_PATH):
    csv_path = os.path.join(holdings_path, "BTEK.csv")
    cleaned_csv = pd.read_csv(csv_path, skiprows = 9, nrows=111)
    cleaned_csv['date'] = date
    cleaned_csv['fund'] = 'BTEK'
    cleaned_csv['Shares'] = cleaned_csv['Shares'].str.replace(',','').astype(float)
    cols = cleaned_csv.columns.tolist()
    correct_cols = cols[-2:] + cols[:-2]
    df = cleaned_csv[correct_cols]
    return df.to_csv(csv_path)


fetch_holdings_data()  
tidy_holdings_data()



#lastly, just keep a record all dates for later use
all_dates = [x.split()[0] for x in list(pd.date_range(start='2021-02-26',end=date, freq='B').map(str))] 

