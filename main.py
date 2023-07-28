import json
import pandas as pd
import urllib
import requests
import datetime
from datetime import timedelta
import pyodbc
import yfinance as yf
from yahoofinancials import YahooFinancials
from cgi import print_form
from pandas_datareader import data as web
import numpy as np

conn = pyodbc.connect('')
sirketlerCursor = conn.cursor()
sirketlerCursor.execute('SELECT * FROM Sirketler')
sirketlerList = sirketlerCursor.fetchall()

conn.close()

bugun = datetime.datetime.today()
yarin = bugun + timedelta(1)


def get_yahoo_shortname(sirketKodu):
    response = urllib.request.urlopen(f'https://query2.finance.yahoo.com/v1/finance/search?q={sirketKodu}')
    content = response.read()
    data = json.loads(content.decode('utf8'))['quotes'][0]['shortname']
    print(data)
    return data


query = """INSERT INTO GunlukHisseSenediDegerleri (sirket_id,sirket_kodu,tarih,acilis,high,low,kapanis,ortalama) VALUES (?,?,?,?,?,?,?,?)"""

conn2 = pyodbc.connect('')
cursor1 = conn2.cursor()

for i in sirketlerList:
    print(i.sirket_kodu)
    sirketId = i.id
    sirketKodu = i.sirket_kodu

    try:
        get_yahoo_shortname(sirketKodu)
        data = yf.download(sirketKodu + '.IS', bugun.strftime('%Y-%m-%d'),yarin.strftime('%Y-%m-%d'))
        data = data.reset_index()

        for index, row in data.iterrows():
            cursor1.execute(query,sirketId,sirketKodu,row['Date'],row['Open'],row['High'],row['Low'],row['Close'],row['Volume'])
            conn2.commit()
    except:
        print("hata oluştu")



conn2.close()