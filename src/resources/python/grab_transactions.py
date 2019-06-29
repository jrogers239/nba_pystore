import urllib3
from bs4 import BeautifulSoup as bs
import pandas as pd
import nltk
from nltk.probability import FreqDist
import numpy as np
import matplotlib.pyplot as plt
url = 'https://www.basketball-reference.com/leagues/NBA_2019_transactions.html'

http = urllib3.PoolManager()

response = http.request('GET',url)
soup = bs(response.data,'html.parser')
transaction_str = ""
transaction_df = pd.DataFrame()

rows = soup.find('ul',attrs={'class': 'page_index'})

for row in rows.find_all('li'):
    dates = row.find_all('span')
    for date in dates:
        cells = row.find_all('p')
        for cell in cells:
            transaction = [[date.text,cell.text]]

            transaction_str+=date.text + " "
            transaction_str+=cell.text + " "
            # print(date.text,cell.text)
            df_hold = pd.DataFrame(transaction)
            transaction_df = transaction_df.append(df_hold)
num = 50
# print(transaction_df)
transaction_df = transaction_df.applymap(str)
# print(transaction_str)
# print(transaction_df)
text1 = nltk.word_tokenize(transaction_str)
# text2 = nltk.word_tokenize(text1)
fdist = FreqDist(text1)
most_fdist= fdist.most_common(num)

x = []
y = []
num_fdist = range(1,num)

for fd in most_fdist:
    x.append(fd[0])
    y.append(fd[1])

plt.plot(y, label='freq')
plt.xticks(num_fdist,x)
plt.show()
