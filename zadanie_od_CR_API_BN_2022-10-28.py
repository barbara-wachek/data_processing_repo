#%% ZADANIE 2022-10-28

#Stwórz zapytanie do API BN, aby otrzymać wszystkie rekordy, których autorem jest Borges. Uwaga: limit rekordów to 100. Jak uzyskać więcej rekordów niż maksymalny limit. 

#link do API BN https://data.bn.org.pl/networks/bibs
#link_query = https://data.bn.org.pl/api/networks/bibs.json?author=Borges+Jorge&amp;limit=100
#dokumentacja API = https://data.bn.org.pl/docs/bibs


#2022-11-25 ZADANIE OD CR KOLEJNE
#Efekt w postaci DataFrame, który z tej listy 247 rekordó÷w robi tabele z 247 rekordami z MARC (tylko z pola MARC)
#Mniej wiecej do takiej formy: https://docs.google.com/spreadsheets/d/1QMM1T0PFmcuQZLxhtM-qak6OXAsD39gYZdrgUNiqSUk/edit?pli=1#gid=0 #W pełni dynamiczna opcja


#2023-02-10

#Wszystkie materialy, ktore tematyzuja Olge Tokarczuk (pole 600, subject). Sprawdz, czy działa z tym kodem
#Z tabeli stworzyc plik tekstowy (mrk - jest zapisany pulpicie)




#%% ROZWIĄZANIE
import requests
import pandas as pd
import re
from collections import ChainMap


data = requests.get('https://data.bn.org.pl/api/networks/bibs.json?', params = {'subject': 'Tokarczuk Olga', 'limit':100}).json()
bibs = data['bibs']
while data['nextPage'] != '':
    data = requests.get(data['nextPage']).json()
    bibs = bibs + data['bibs']

all_marc_series = []
for element in bibs: 
    marc_series = element['marc']
    all_marc_series.append(marc_series)
    

final_df = pd.DataFrame()

for x in all_marc_series:
    LDR = x.get('leader')
    fields = x.get('fields')
    fields_001_009 = [x for x in fields if int(list(x.keys())[0]) in range(10)]
    fields_001_009 = dict(ChainMap(*fields_001_009))

    df = pd.DataFrame([fields_001_009])
    df['LDR'] = LDR
    
    fields_010 =  [x for x in fields if int(list(x.keys())[0]) in range(10, 1000)]
    

    final_dict = dict()
    count_list = list()
    for x in fields_010:
        for key, value in x.items():
            for z, a in value.items():
               if re.match('ind.*', z):
                   if key in final_dict.keys():
                       if key not in count_list:
                           count_list.append(key)
                           if a == ' ':
                               final_dict[key] = str(final_dict[key]) + '\\'
                           else:
                               final_dict[key] = str(final_dict[key])  + str(a)
                       else:
                           if 'ind1' in z:
                               if a == ' ':
                                   final_dict[key] = str(final_dict[key]) + '❦' + '\\'
                               else:
                                   final_dict[key] = str(final_dict[key]) + '❦' + str(a)
                           else:
                               if a == ' ':
                                   final_dict[key] = str(final_dict[key]) + '\\'
                               else:
                                   final_dict[key] = str(final_dict[key]) + str(a)
                               
                   else: 
                       if a == ' ':
                           final_dict[key] = '\\'
                       else:
                           final_dict[key] = str(a)
                       
               elif re.match('subfields', z):
                   for el in a:
                       for char, val in el.items():
                           if key in final_dict.keys():
                               final_dict[key] = final_dict[key] + '$' + char + val
                           else:
                               final_dict[key] = '$' + char + val
    
    
    df2 = pd.DataFrame([final_dict])
    df3 = pd.concat([df, df2], axis=1)
    final_df = pd.concat([final_df, df3], join='outer')
        
    
final_df = final_df.reindex(sorted(final_df.columns), axis=1)
first_column = final_df.pop('LDR')
final_df.insert(0, 'LDR', first_column)


final_df.to_excel('BN.xlsx', index=False, encoding='utf-8')   
  

#Z tabeli stworzyć plik tekstowy w formacie MRC



#txt_file ? czy od razu mrc file
for index, row in final_df.iterrows():


for i in range(len(df)):
    print(df.iloc[i, 0], df.iloc[i, 2])









