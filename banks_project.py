import pandas as pd
import sqlite3
import requests
from bs4 import BeautifulSoup
import numpy as np
from datetime import datetime


url = 'https://en.wikipedia.org/wiki/List_of_largest_banks'
csv_url = "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv"
target_loc = 'Largest_banks_data.csv'
db_name = 'Banks.db'
table_name = 'Largest_banks'
query_statement = f'select * from {table_name}'

def extract(text):
    df = pd.DataFrame(columns=['Name', 'MC_USD_Billion'])
    soup = BeautifulSoup(text, 'html5lib')
    table = soup.find_all('tbody')
    rows = table[2].find_all('tr')
    for row in rows:
        cell = row.find_all('td')
        if len(cell) > 1:
                name = cell[1].text.strip()
                gdp = cell[2].text.strip()
                row_df = pd.DataFrame([[name, gdp]], columns=['Name', 'MC_USD_Billion'])
                df = pd.concat([df, row_df],ignore_index=True)

    return df

def transform(text,fileurl):
    data_dict ={}
    df = pd.read_csv(fileurl)
    df=df.set_index('Currency')
    data_dict = df.to_dict()['Rate']
    text['MC_GBP_Billion'] = [np.round(data_dict['GBP']*float(x),2) for x in text['MC_USD_Billion']]
    text['MC_EUR_Billion'] = [np.round(data_dict['EUR']*float(x),2) for x in text['MC_USD_Billion']]
    text['MC_INR_Billion'] = [np.round(data_dict['INR']*float(x),2) for x in text['MC_USD_Billion']]
    print(text)
    return text

def load_to_csv(loc,text):
    text.to_csv(loc)

def load_to_db(conn,table,text):
    text.to_sql(table,conn,if_exists='replace',index=False)

def log_progress(message):
    timestamp_format = '%y/%m/%d %H:%M:%S'
    current_time = datetime.now()
    timestamp_format = current_time.strftime(timestamp_format)
    with open('code_log.txt','a') as f:
        f.write(timestamp_format + ': ' + message + '\n')

def run_query(query, sql_connection):
    data = pd.read_sql(query, sql_connection)
    print(data)

response = requests.get(url)

log_progress("ETL process started")
log_progress("Extraction started")
extracted_data = extract(response.text)
log_progress("Extraction completed")

log_progress("Transformation started")
transformed_data = transform(extracted_data,csv_url)
log_progress("Transformation completed")

log_progress("Writing to csv...")
load_to_csv(target_loc,transformed_data)
log_progress("Written to csv")

log_progress("DB connection started...")
sql_conn = sqlite3.connect(db_name)
if sql_conn:
    log_progress("Connection successful")
log_progress("Writing to DB....")
load_to_db(sql_conn,table_name,transformed_data)
log_progress("Writing to DB completed...")

log_progress("ETL process completed")

sql_conn.close()