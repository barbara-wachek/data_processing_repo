#%% ZADANIE 2022-10-28

#Stwórz zapytanie do API BN, aby otrzymać wszystkie rekordy, których autorem jest Borges. Uwaga: limit rekordów to 100. Jak uzyskać więcej rekordów niż maksymalny limit. 

#link do API BN https://data.bn.org.pl/networks/bibs
#link_query = https://data.bn.org.pl/api/networks/bibs.json?author=Borges+Jorge&amp;limit=100
#dokumentacja API = https://data.bn.org.pl/docs/bibs



#%% ROZWIĄZANIE
import requests
import pandas as pd
from pprint import pprint
from pymarc import MARCReader
import json


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


marc_list_of_bibs = []
marc_list_of_leader = []

for x in bibs:
    marc_list_of_bibs.append(x['marc']['fields'])
    marc_list_of_leader.append(x['marc']['leader'])


marc_list_of_bibs #Może przekonwertować to na jsona? I potem użyć json_normalize? Na końcu dodać kolumnę leader
records = [x for x in marc_list_of_bibs]   

names_of_columns = []
for x in records:
    for y in x:
        for k,v in y.items():
            if k not in names_of_columns:
                names_of_columns.append(k)

#Jak wykorzystać w odpowiednim miejscu nazwy kolumny?       
        
        

df_marc_list_of_bibs = pd.DataFrame(marc_list_of_bibs, columns=[])

# jsonString = json.dumps(marc_list_of_bibs) 
    
# df_marc_bibs = pd.json_normalize(jsonString)    
    
#Potem jakos polaczyc liste leader z ta tabela. Wyjac z subfields - to nie wyglada tak jak powinno    
  
 
    

        
    
    

# reader = MARCReader(marc_list_of_bibs)
# for record in reader: 
#     print(author(record))

# dir(reader)

# lista = reader.file_handle

# for record in lista:
#     print(record.title())

# record = Record(marc_list_of_bibs=chunk)

# # formated_marc_list = []

# for x in marc_list_of_bibs:
#     for y in x['fields']:
#         for k,v in y.items(): 
#             formated_marc_list.append({k:v})
            


df_marc = pd.DataFrame(marc_list_of_bibs)

df_marc_test = pd.json_normalize(df_series_fields, sep='_')


df_marc = df_marc['leader'] + df_marc['fields'][0]

df_series_fields = pd.DataFrame(df_marc['fields'][0])



new_df = df_marc['leader'] + df_series_fields

seria = df_marc['fields'][0]




#test = pd.DataFrame.from_dict(marc_list_of_bibs) - nie wiem jak to zastosować

import pandas as pd

d = {'a': 1,
     'c': {'a': 2, 'b': {'x': 5, 'y' : 10}},
     'd': [1, 2, 3]}

df = pd.json_normalize(d, sep='_')

print(df.to_dict(orient='records')[0])






new_list_of_dictionaries = []
dictionary_of_bib = {}
for element in marc_list_of_bibs:
    for k,v in element.items():
        if k == 'leader':
            dictionary_of_bib = {k:v}
        elif k == 'fields':
            k['fields'][0]
            dictionary_of_bib 
    
    
    new_list_of_dictionaries.append(element['leader'])

Series.str.split().


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













