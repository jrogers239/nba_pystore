import urllib3
from bs4 import BeautifulSoup as bs
import pandas as pd
url = 'https://www.basketball-reference.com/leagues/NBA_2019_transactions.html'

http = urllib3.PoolManager()

response = http.request('GET',url)
soup = bs(response.data,'html.parser')

transaction_df = pd.DataFrame()

rows = soup.find('ul',attrs={'class': 'page_index'})

for row in rows.find_all('li'):
    dates = row.find_all('span')
    for date in dates:
        cells = row.find_all('p')
        for cell in cells:
            transaction = [[date.text,cell.text]]
            df_hold = pd.DataFrame(transaction)
            transaction_df = transaction_df.append(df_hold)
