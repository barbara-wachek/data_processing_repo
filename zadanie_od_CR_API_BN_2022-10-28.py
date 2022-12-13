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
import re




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



all_marc_series = []
for element in bibs: 
    marc_series = element['marc']
    all_marc_series.append(marc_series)


all_normalize_marc_series = []
for record in all_marc_series:
    normalize_record = pd.json_normalize(record, record_path=['fields'], sep='_')
    all_normalize_marc_series.append(normalize_record)
    
    
############ Kontynuować poniższy kod. Spróbować wyjąć dane z kolumny marc_fields    
    
bibs_norm = pd.json_normalize(bibs, sep='_', record_path='marc')    
  
bibs_norm_only_marc = bibs_norm.drop(columns=['id', 'zone', 'createdDate', 'updatedDate', 'deleted', 'deletedDate', 'language', 'subject', 'subjectPlace', 'subjectWork', 'subjectTime', 'author', 'placeOfPublication', 'location', 'title', 'udc', 'publisher', 'kind', 'domain', 'formOfWork', 'isbnIssn', 'genre', 'timePeriodOfCreation', 'audienceGroup', 'demographicGroup', 'nationalBibliographyNumber', 'publicationYear', 'languageOfOriginal' ])


#bibs_norm_only_marc_norm = pd.json_normalize(bibs_norm_only_marc, record_path='marc_fields', sep='_')

###################
    
# Given nested dictionary

all_marc_series

# Convert to data frame
df = pd.DataFrame(rows)
print(df)    
    
    
    
    
json_marc = pd.json_normalize(all_marc_series, record_path=['fields'])

json_marc_df = pd.DataFrame(json_marc)


element = bibs[2]

element_2 = bibs[5]

all_marcs = []
for element in bibs:
    leader = element['marc']['leader']
    fields = element['marc']['fields']
    
    marc = element['marc']
    
    marc_series = pd.Series(marc)
    
    

    
    
    
    

    marc_2 = element_2['marc']
    
    marc_series_2 = pd.Series(marc_2)
    
    
    test_df = pd.DataFrame([marc, marc_2])
    
    
    
    
    leader_series = pd.Series(leader)
    
    
    for field in fields: 
        fields[0]
        for k,v in field.items():
            fields_series = pd.Series(fields[0])
            test_df = pd.DataFrame([leader_series, fields_series])    
    
    
    
    
    
    
    marc_df_leader = pd.DataFrame(element['marc'])
    marc_dr_fields = pd.DataFrame(element['marc']['fields'])
    
    
    
    marc_df = pd.DataFrame(bibs['marc'])
    all_marcs.append(marc_df)





marc_list = []

for x in bibs:
    marc_list.append(x['marc'],)


marc_normalize = pd.json_normalize(marc_list, sep=',') #Może później?


columns_name = []



# with open(f'marc_Borges.json', 'w', encoding='utf-8') as f:
#     json.dump(marc_list, f)     
                    
# jsonStr = json.dumps(marc_list)
# print(jsonStr)   

# dir(data)
# data = requests.get('https://data.bn.org.pl/api/networks/bibs.marc?', params = {'author': 'Borges, Jorge Luis (1899-1986)', 'limit':100}).text
# bibs = data['bibs']

# while data['nextPage'] != '':
#     data = requests.get(data['nextPage']).json()
#     bibs = bibs + data['bibs']


# reader = MARCReader(data, to_unicode = True, force_utf8 = True)

# dir(reader)


names_of_columns = []
for x in marc_list_of_bibs:
    for y in x:
        for k,v in y.items():
            if k not in names_of_columns:
                names_of_columns.append(k)

#Jak wykorzystać w odpowiednim miejscu nazwy kolumny?       
        
  


df_marc_list_of_bibs = pd.DataFrame(marc_list_of_bibs, columns=[])





import collections

def flatten(d, parent_key='', sep='_'):
    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, collections.MutableMapping):
            items.extend(flatten(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)

>>> flatten({'a': 1, 'c': {'a': 2, 'b': {'x': 5, 'y' : 10}}, 'd': [1, 2, 3]})
{'a': 1, 'c_a': 2, 'c_b_x': 5, 'd': [1, 2, 3], 'c_b_y': 10}




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













