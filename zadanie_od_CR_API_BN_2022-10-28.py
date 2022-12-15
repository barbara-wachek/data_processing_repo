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
from flatten_json import flatten
import tqdm




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
    
dic_flattened = [flatten(d) for d in all_marc_series]    
data_frame_marc = pd.DataFrame(dic_flattened)
data_frame_marc.rename(columns={'leader': 'LDR'}, inplace=True)

#Ustawienie bardziej zrozumiałych nazw kolumn:
format_fields = r'(fields\_\d{1,2}\_)(\d{3})$'
format_ind = r'(fields\_\d{1,2}\_)(\d{3}\_)(ind\d)'
format_subfields = r'(fields\_\d{1,2}\_)(\d{3}\_)(subfields\_\d\_\w)'

for x in data_frame_marc.columns:
    if re.match(format_fields, x):
        new_name = re.sub(r'(fields\_\d{1,2}\_)(\d{3})$', r'\2', x)
        data_frame_marc.rename(columns={x: new_name}, inplace=True)
    elif re.match(format_ind, x):
        new_name = re.sub(r'(fields\_\d{1,2}\_)(\d{3}\_)(ind\d)', r'\2\3', x)
        data_frame_marc.rename(columns={x: new_name}, inplace=True)
    elif re.match(format_subfields, x):
        new_name = re.sub(r'(fields\_\d{1,2}\_)(\d{3}\_)(subfields\_\d\_\w)', r'\2\3', x)
        data_frame_marc.rename(columns={x: new_name}, inplace=True)



format_column_name = r'(\d{3})(\_)?(ind|subfields)?(\_)?(\d)(\_)?(\w)?'
format_marc_field = r'(^\d{3})(?=.*)'



test_list = []

for column_name in data_frame_marc.columns:
    number_of_column = re.sub(r'(\d{3})(\_)?(ind|subfields)?(\_)?(\d)(\_)?(\w)?', r'\1', column_name)
    if number_of_column not in test_list:
        test_list.append(number_of_column)
        data_frame_marc.rename(columns={column_name:number_of_column}, inplace=True)
        if 'ind' in column_name:
            data_frame_marc[number_of_column].replace('', "\\")
            
            
            
            
            for data in data_frame_marc[number_of_column]:  #musi iterować po komórkach, zeby zmianić ich wartoć - nie po całej kolumnie
                if not re.search(r'\d', data):
                    data_frame_marc[number_of_column] = '\\'        
    elif number_of_column in test_list: 
        if 'ind' in column_name:
            if not data_frame_marc[column_name].str.contains('\d', flags=re.I, regex=True):
                data_frame_marc[column_name] = '\\'      
            data_frame_marc.rename(columns={column_name:number_of_column}, inplace=True)
        # elif 'subfields' in column_name:
        #     data_frame_marc[column_name] = f'${data_frame_marc[column_name]}'
        #     data_frame_marc.rename(columns={column_name:number_of_column}, inplace=True)
            
            
            
            
            
    #     data_frame_marc[number_of_column] = data_frame_marc[number_of_column] + data_frame_marc[column_name].convert_dtypes(convert_string=True)


# Define a function
# def add_dolar_to_subfield():
#     for row in data_frame_marc: 
#         for column in data_frame_marc.columns:
#             if 'subfield' in column and data_frame_marc[column].notna(): 
#                 data_frame_marc[column] = f'${data_frame_marc[column]}'
        


# data_frame_marc.apply(add_dolar_to_subfield)




    
    


















