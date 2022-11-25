#%% ZADANIE 2022-10-28

#Stwórz zapytanie do API BN, aby otrzymać wszystkie rekordy, których autorem jest Borges. Uwaga: limit rekordów to 100. Jak uzyskać więcej rekordów niż maksymalny limit. 

#link do API BN https://data.bn.org.pl/networks/bibs
#link_query = https://data.bn.org.pl/api/networks/bibs.json?author=Borges+Jorge&amp;limit=100
#dokumentacja API = https://data.bn.org.pl/docs/bibs



#%% ROZWIĄZANIE
import requests
import pandas as pd
from pprint import pprint


author = 'Borges, Jorge Luis (1899-1986)'
link = 'https://data.bn.org.pl/api/networks/bibs.json?'
data_01 = requests.get(link, params = {'author': author, 'limit':100}).json()
data_02 = requests.get(data_01['nextPage']).json()
data_03 = requests.get(data_02['nextPage']).json()
data_04 = requests.get(data_03['nextPage']).json()

all_bibs = data_01['bibs'] + data_02['bibs'] + data_03['bibs']


# Drugie rozwiązanie (krótsze)

data = requests.get('https://data.bn.org.pl/api/networks/bibs.json?', params = {'author': 'Borges, Jorge Luis (1899-1986)', 'limit':100}).json()
bibs = data['bibs']

while data['nextPage'] != '':
    data = requests.get(data['nextPage']).json()
    bibs = bibs + data['bibs']


#2022-11-25 ZADANIE OD CR KOLEJNE
#Efekt w postaci DataFrame, który z tej listy 247 rekordó÷w robi tabele z 247 rekordami z MARC (tylko z pola MARC)
#Mniej wiecej do takiej formy: https://docs.google.com/spreadsheets/d/1QMM1T0PFmcuQZLxhtM-qak6OXAsD39gYZdrgUNiqSUk/edit?pli=1#gid=0 #W pełni dynamiczna opcja



#pprint(bibs)
#pd_test = pd.DataFrame(bibs)
#pd_data = pd.json_normalize(bibs)
#pd_data.drop_duplicates



# link = 'https://data.bn.org.pl/api/networks/bibs.json?'
# data_01 = requests.get(link, params = {'author':'Borges, Jorge Luis', 'limit':100}).json()
# sinceID = data_01['bibs'][-1]['id']
# data_02 = requests.get(link, params = {'author':'Borges, Jorge Luis', 'limit':100, 'sinceId': sinceID}).json()
# sinceID_02 = data_02['bibs'][-1]['id']
# data_03 = requests.get(link, params = {'author':'Borges, Jorge Luis', 'limit':100, 'sinceId': sinceID_02}).json()













