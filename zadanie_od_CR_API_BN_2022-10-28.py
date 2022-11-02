#%% ZADANIE 2022-10-28

#Stwórz zapytanie do API BN, aby otrzymać wszystkie rekordy, których autorem jest Borges. Uwaga: limit rekordów to 100. Jak uzyskać więcej rekordów niż maksymalny limit. 

#link do API BN https://data.bn.org.pl/networks/bibs
#link_query = https://data.bn.org.pl/api/networks/bibs.json?author=Borges+Jorge&amp;limit=100
#dokumentacja API = https://data.bn.org.pl/docs/bibs



#%% ROZWIĄZANIE
import requests
from pprint import pprint
import pandas as pd


author = 'Borges, Jorge Luis (1899-1986)'
link = 'https://data.bn.org.pl/api/networks/bibs.json?'
data_01 = requests.get(link, params = {'author': author, 'limit':100}).json()
data_02 = requests.get(data_01['nextPage'], params = {'author': author, 'limit':100}).json()
data_03 = requests.get(data_02['nextPage'], params = {'author': author, 'limit':100}).json()
data_04 = requests.get(data_03['nextPage'], params = {'author': author, 'limit':100}).json()

all_bibs = data_01['bibs'] + data_02['bibs'] + data_03['bibs']


# Drugie rozwiązanie: 

data = requests.get('https://data.bn.org.pl/api/networks/bibs.json?', params = {'author': 'Borges, Jorge Luis (1899-1986)', 'limit':100}).json()
bibs = data['bibs']

while data['nextPage'] != '':
    data = requests.get(data['nextPage'], params = {'author': 'Borges, Jorge Luis (1899-1986)', 'limit':100}).json()
    bibs = bibs + data['bibs']


#pd_test = pd.DataFrame(bibs)
pd_data = pd.json_normalize(bibs)
pd_data.drop_duplicates



# link = 'https://data.bn.org.pl/api/networks/bibs.json?'
# data_01 = requests.get(link, params = {'author':'Borges, Jorge Luis', 'limit':100}).json()
# sinceID = data_01['bibs'][-1]['id']
# data_02 = requests.get(link, params = {'author':'Borges, Jorge Luis', 'limit':100, 'sinceId': sinceID}).json()
# sinceID_02 = data_02['bibs'][-1]['id']
# data_03 = requests.get(link, params = {'author':'Borges, Jorge Luis', 'limit':100, 'sinceId': sinceID_02}).json()













