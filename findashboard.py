import streamlit as st
import pandas as pd 
import psycopg2 
import tweepy 
import numpy as np
import requests
import configuration
from configuration import BaseConfig
import psycopg2.extras
import datetime
from pandas.tseries.offsets import BDay 
import plotly.graph_objects as go
from comma_string import get_comma_int, get_comma_float


#get the latest download date (-1 Business Day from today)
today = datetime.datetime.today()

date = (str(today - BDay(2)))[:10]

date_delta_30 = (str(today - BDay(20)))[:10]


connection = psycopg2.connect(host=BaseConfig.DB_HOST, database=BaseConfig.DB_NAME, user=BaseConfig.DB_USER, password=BaseConfig.DB_PASS)
cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)

st.sidebar.write("DASHBOARD OPTIONS")


option = st.sidebar.selectbox('Which Dashboard?',('Home', 'Chart Analysis Of My Stocks', 'Pattern Analysis Of My Stocks','20-Day Simple Moving Average','Research: Fundamental Data','Research: Twitter Stock Trends', 'Tracking Blackrock Future Tech ETF'))

# st.title(option)

if option == 'Home':
    st.title("Hello There...")
    st.title("Welcome To My Financial Dashboard")
    st.header("Developed & Produced By Anas Afzal.")
    st.subheader('On the side-bar, there is a dropdown which will navigate you throughout this dashboard')

    st.write("Using PostgreSQL, TimescaleDB & Docker to build my relaional database of stock data & time-series analysis, and then using Python, APIs, Streamlit & Heroku to create a series of scripts for the front-end, this Financial Dashboard is the culmination of many interesting tools.")
    st.write("Feel free to explore and get in touch with any feedback. Enjoy!")

    st.header("NOTE: For some of the views, API tokens are required from certain websites. Please see the requirements.txt file in the github repo for more info.")



if option == '20-Day Simple Moving Average':

    df_sym = pd.read_sql("SELECT symbol FROM stock WHERE id IN (SELECT holding_id FROM etf_holding)", connection)
    sym_tup = sorted(tuple(df_sym['symbol']))
    
    symbol = st.sidebar.text_input("Input Symbol Here:", value=sorted(df_sym['symbol'])[0], max_chars=None, key=None, type='default')

    available_symbol = st.sidebar.selectbox("List Of Available Stocks", 
                                    sym_tup)
    data_win = pd.read_sql(f""" 
    SELECT bucket as day, AVG(close) OVER (ORDER BY bucket ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS sma_20
    FROM daily_bars
    WHERE stock_id IN (SELECT id FROM stock WHERE symbol = '{symbol}')
    ORDER BY day DESC;""", connection)


    fig_win = go.Figure()
    fig_win.add_trace(go.Scatter(name="Line", x=data_win['day'].map(str).str.slice(stop=10), y=data_win['sma_20']))
    fig_win.add_trace(go.Bar(name="Bar", x=data_win['day'].map(str).str.slice(stop=10), y=data_win['sma_20']))
    
    fig_win.update_xaxes(type='category')
    fig_win.update_layout(margin=dict(l=5, r=5, t=5, b=5))

    st.header(f"20-Day SMA For {symbol}")

    st.plotly_chart(fig_win, use_container_width=True)




if option == 'Research: Twitter Stock Trends':
    symbol = st.sidebar.text_input(label="Input Symbol Here:", value="TSLA", max_chars=5)

    r = requests.get(f'https://api.stocktwits.com/api/2/streams/symbol/{symbol}.json')
    data = r.json()

    for message in data['messages']:
        st.image(message['user']['avatar_url'])
        st.write(message['user']['username'])
        st.write(message['created_at'])
        st.write(message['body'])



if option == 'Tracking Blackrock Future Tech ETF':
    sel_day = st.sidebar.date_input('Select Date',value=datetime.datetime.strptime(date, '%Y-%m-%d'), min_value=datetime.datetime.strptime(date_delta_30, '%Y-%m-%d'), max_value=datetime.datetime.strptime(date, '%Y-%m-%d'))
    st.title("10 Most Thinly Traded Stocks In Fund")
    st.header(f"As Of {sel_day}")


    cursor.execute(f"""
        SELECT stock.symbol, SUM(stock_price.volume) as tot_volume
        FROM stock
        INNER JOIN stock_price ON stock.id=stock_price.stock_id
        WHERE DATE(stock_price.dt) = '{sel_day}'
        GROUP BY stock.symbol
        ORDER BY tot_volume ASC
        LIMIT 10
    """)

    rows = cursor.fetchall()
    for row in rows:
        st.image(f"https://finviz.com/chart.ashx?t={row['symbol']}")
    






if option == 'Pattern Analysis Of My Stocks':
    sel_day = st.sidebar.date_input('Select Date',value=datetime.datetime.strptime(date, '%Y-%m-%d'), min_value=datetime.datetime.strptime(date_delta_30, '%Y-%m-%d'), max_value=datetime.datetime.strptime(date, '%Y-%m-%d'))
    pattern = st.sidebar.selectbox(
        "Select A Pattern",("Bullish Engulfing", "Three-Bar Reversal")
    )

    if pattern == 'Bullish Engulfing':
        cursor.execute(f"""
        SELECT * FROM (
        SELECT bucket, open, close, symbol, LAG(close,1) OVER (
        PARTITION BY stock_id ORDER BY bucket 
        ) AS previous_close, 
        LAG(open, 1) OVER(
        PARTITION BY stock_id ORDER BY bucket
        ) AS previous_open 
        FROM daily_bars
        JOIN stock ON stock.id = daily_bars.stock_id
        ) AS a 
        WHERE previous_close < previous_open
        AND close > previous_open AND open < previous_close AND bucket = '{sel_day}'
        """)
    
    if pattern == 'Three-Bar Reversal':
        cursor.execute(f"""
        SELECT * FROM (
        SELECT bucket, close, volume, symbol, LAG(close, 1) OVER (
        PARTITION BY stock_id
        ORDER BY bucket
        ) AS previous_close, 
        LAG(volume, 1) OVER (
        PARTITION BY stock_id
        ORDER BY bucket
        ) AS previous_volume, 
        LAG(close, 2) OVER (
        PARTITION BY stock_id
        ORDER BY bucket
        ) AS previous_previous_close,
        LAG(volume, 2) OVER (
        PARTITION BY stock_id
        ORDER BY bucket
        ) AS previous_previous_volume, 
        LAG(close, 3) OVER (
        PARTITION BY stock_id
        ORDER BY bucket
        ) AS previous_previous_previous_close,
        LAG(volume, 3) OVER (
        PARTITION BY stock_id
        ORDER BY bucket
        ) AS previous_previous_previous_volume
        FROM daily_bars
        JOIN stock ON stock.id = daily_bars.stock_id
        ) AS a
        WHERE close > previous_previous_previous_close AND 
        previous_close < previous_previous_close AND
        previous_close < previous_previous_previous_close AND
        volume > previous_volume AND 
        previous_volume < previous_previous_volume AND
        previous_previous_volume  < previous_previous_previous_volume AND
        bucket = '{sel_day}'
        """)

    rows = cursor.fetchall()

    for row in rows:
        st.image(f"https://finviz.com/chart.ashx?t={row['symbol']}")





if option == 'Chart Analysis Of My Stocks':

    st.title("Interactive Charts For All My Stocks")
    
    st.subheader("Watch This Short Video To See How You Can Interact With These Charts:")
    video_file = open('/Users/anasafzal/Desktop/StreamlitCharts.mov', 'rb')
    video_bytes = video_file.read()

    st.video(video_bytes)



    df_sym = pd.read_sql("SELECT symbol FROM stock WHERE id IN (SELECT holding_id FROM etf_holding)", connection)
    sym_tup = sorted(tuple(df_sym['symbol']))
    
    symbol = st.sidebar.text_input("Input Symbol Here:", value=sorted(df_sym['symbol'])[0], max_chars=None, key=None, type='default')

    available_symbol = st.sidebar.selectbox("List Of Available Stocks", 
                                    sym_tup)

    data = pd.read_sql("""
    SELECT DATE(bucket) AS day, open, high, low, close
    FROM daily_bars
    WHERE stock_id = (SELECT id FROM stock WHERE UPPER(symbol) = %s)
    ORDER BY day ASC
""", connection, params=(symbol.upper(), ))

    st.title("          VIEW CHART          ")

    st.subheader(symbol.upper())

    fig = go.Figure(data=[go.Candlestick(x=data['day'], 
                    open=data['open'], 
                    high=data['high'], 
                    low=data['low'], 
                    close=data['close'],
                    name=symbol)])
    
    fig.update_xaxes(type='category')
    fig.update_layout(height=700)

    st.plotly_chart(fig, use_container_width=True)

    st.write(data)


if option == 'Research: Fundamental Data':
    fund_type = st.sidebar.selectbox(
        "Company Overview & Fundamentals",("Company Overview","Company Fundamentals")
    )

    st.title(fund_type)

    if fund_type == "Company Overview":
        symbol = st.sidebar.text_input(label="Input Symbol", value="TSLA", max_chars=None)

        # company = AlphaVantageFundamentals(configuration.API_KEY, symbol)
        
        r = requests.get(f'https://www.alphavantage.co/query?function=OVERVIEW&symbol={symbol}&apikey={BaseConfig.API_KEY}')
        data_fund = r.json()
        
        st.image(f"https://eodhistoricaldata.com/img/logos/US/{symbol}.png")
        
        col1, col2 = st.beta_columns([1,4])

        with col1:
            st.header("Name")
            st.subheader(data_fund['Name'])
            st.header("Sector")
            st.subheader(data_fund['Sector'])
            st.header("Industry")
            st.subheader(data_fund['Industry'])
            st.header("Country")
            st.subheader(data_fund['Country'])
            st.header("Currency")
            st.subheader(data_fund['Currency'])
            st.header("Exchange")
            st.subheader(data_fund['Exchange'])
        
        with col2:
            st.header("Description")
            st.write(data_fund['Description'])
        
    
    if fund_type == "Company Fundamentals":
        symbol = st.sidebar.text_input(label="Input Symbol Here:", value="TSLA", max_chars=None)

        r = requests.get(f'https://www.alphavantage.co/query?function=OVERVIEW&symbol={symbol}&apikey={BaseConfig.API_KEY}')
        data_fund = r.json()

        col1, col2 = st.beta_columns([1,2])

        with col1:
            st.subheader("Market Cap")
            st.write(get_comma_int(data_fund["MarketCapitalization"]))
            st.subheader("P/E Ratio")
            st.write(get_comma_float(data_fund["PERatio"]))
            st.subheader("Profit Margin")
            st.write(get_comma_float(data_fund["ProfitMargin"]))
            st.subheader("Operating Margin (TTM)")
            st.write(get_comma_float(data_fund["OperatingMarginTTM"]))
            st.subheader("Revenue (TTM)")
            st.write(get_comma_float(data_fund["RevenueTTM"]))
            st.subheader("Q.Earnings Growth YOY")
            st.write(get_comma_float(data_fund["QuarterlyEarningsGrowthYOY"]))
            st.subheader("Beta")
            st.write(get_comma_float(data_fund["Beta"]))
            st.subheader("52 Week Low")
            st.write(get_comma_float(data_fund["52WeekLow"]))
            st.subheader("EV To Revenue")
            st.write(get_comma_float(data_fund["EVToRevenue"]))

        with col2:

            st.subheader("EBITDA")
            st.write(get_comma_float(data_fund["EBITDA"]))
            st.subheader("Book Value")
            st.write(get_comma_float(data_fund["BookValue"]))
            st.subheader("EPS")
            st.write(get_comma_float(data_fund["EPS"]))
            st.subheader("Return On Assets (TTM)")
            st.write(get_comma_float(data_fund["ReturnOnAssetsTTM"]))
            st.subheader("Q.Revenue Growth YOY")
            st.write(get_comma_float(data_fund["QuarterlyRevenueGrowthYOY"]))
            st.subheader("Price To Book Ratio")
            st.write(get_comma_float(data_fund["PriceToBookRatio"]))
            st.subheader("Price To Sales Ratio (TTM)")
            st.write(get_comma_float(data_fund["PriceToSalesRatioTTM"]))
            st.subheader("52 Week High")
            st.write(get_comma_float(data_fund["52WeekHigh"]))
            st.subheader("EV To EBITDA")
            st.write(get_comma_float(data_fund["EVToEBITDA"]))
