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

#2023-02-14
#z pliku mrk zrobić tabelę - wgrać do Pythona mrk i DataFrame zrobić


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
  
    
string_file = []  
for index, row in final_df.iterrows():
    for key, value in row.items():
        if str(value) != 'nan' and '❦' not in str(value):  
            #print(str(key) + '=' + '  ' + str(value) + '\n')     
            string_file.append('=' + str(key) + '  ' + str(value) + '\n')

        elif '❦' in str(value):
            while value.find('❦') != -1: 
                slicing = value.find('❦')
                #print(str(key) + '=' + '  ' + str(value[0:slicing] + '\n'))
                string_file.append('=' + str(key) + '  ' + str(value[0:slicing] + '\n'))
                value = value[slicing+1:]
    
    string_file.append('  \n')            
    join_list = ''.join(string_file)


#join_list.count('LDR') #sprawdzenie, czy są wszystkie rekordy

with open('Olga_Tokarczuk.mrk', 'w', encoding='utf-8') as f:
    f.write(join_list)



#%% Wgrać plik mrk do Pythona i zrobić z niego tabelę DataFrame    

# stacjonarka: "C:\\Users\\PBL_Basia\\Desktop\\Olga_Tokarczuk.mrk"
# laptop: "C:\\Users\\Barbara Wachek\\Desktop\\Olga_Tokarczuk.mrk"

#Utworzenie z każdego podpola słownika, w którym kluczem jest nazwa podpola. 
list_of_marc_dicts = []
with open("C:\\Users\\PBL_Basia\\Desktop\\Olga_Tokarczuk.mrk", 'r', encoding='utf-8') as file: 
    data = file.read()
    
list_of_fields = data.split('\n')

list_of_records = []
dictionary_of_record = {}
for field in list_of_fields: 
    if field.startswith('='): 
        dictionary_of_record[field[1:4]] = field[6:]
    else:
        list_of_records.append(dictionary_of_record)
        dictionary_of_record = {}
        
new_df = pd.DataFrame(list_of_records)        
        

sorted_df = new_df.reindex(sorted(new_df.columns), axis=1)
first_column = sorted_df.pop('LDR')
sorted_df.insert(0, 'LDR', first_column)




#%% Dla Karoliny 2025-03-21
### Prosba: Czy potrafisz wyciągać dane z bazy BN? Potrzebna mi będzie do nowego referatu tabela z polskimi czasopismami literackimi, które odnotowuje BN. Poza podstawowymi informacjami jak tytuł, podtytuł, wydawca/instytucja sprawcza, miejsce wydania ważne będą daty publikacja (w jakich latach się ukazywało - dla nieistniejących / od kiedy się ukazuje - dla aktualnie wychodzących). Chodzi tylko o czasopisma, które w polu Gatunek mają Czasopismo literackie. Dasz radę cos takiego przygotować?
#Przykłady:
#Elewator: https://katalogi.bn.org.pl/discovery/fulldisplay?docid=alma991013120739705066&context=L&vid=48OMNIS_NLOP:48OMNIS_NLOP&lang=pl&search_scope=NLOP_IZ_NZ&adaptor=Local%20Search%20Engine&tab=LibraryCatalog&query=any,contains,elewator&sortby=date_d&facet=frbrgroupid,include,9011996902311126997&offset=0
#Latarnia Morska: https://katalogi.bn.org.pl/discovery/fulldisplay?docid=alma991030789889705066&context=L&vid=48OMNIS_NLOP:48OMNIS_NLOP&lang=pl&search_scope=MyInstitution1&adaptor=Local%20Search%20Engine&tab=BN&query=any,contains,latarnia%20morska&facet=lds04,include,Czasopisma&offset=0


import requests
import pandas as pd
import re
from collections import ChainMap

data = requests.get('https://data.bn.org.pl/api/networks/bibs.json?', params = {'genre': 'Czasopismo literackie', 'kind': 'czasopismo', 'limit':100}).json()

bibs = data['bibs']
while data['nextPage'] != '':
    data = requests.get(data['nextPage']).json()
    bibs = bibs + data['bibs']

all_records = []
for element in bibs: 
    all_records.append(element)


all_records_list = []
for element in all_records:
    # element = all_records[2]
    dictionary_of_records = {'Tytuł': element['title'],
                             'Wydawca': element['publisher'],
                             'Miejsce wydania': element['placeOfPublication'],
                             'Język': element['language'], 
                             'Typ': element['kind'], 
                             'Gatunek': element['genre'], 
                             'Forma': element['formOfWork'], 
                             'Autor': element['author'], 
                             'Czas': element['timePeriodOfCreation']
                             }
    
    all_records_list.append(dictionary_of_records)


final_df = pd.DataFrame(all_records_list)

final_df['Rok_koncowy'] = final_df['Czas'].str.split(' - ').str[1]

final_df['Rok_koncowy'] = pd.to_numeric(final_df['Rok_koncowy'], errors='coerce')

# Jeśli chcesz uzupełnić NaN, np. wartością 0 (lub inną, np. 1945), robimy to:
final_df['Rok_koncowy'] = final_df['Rok_koncowy'].fillna(0).astype(int)


# Filtruj dane, uwzględniając tylko rekordy, w których rok zakończenia jest >= 1945
df_filtered = final_df[(final_df['Język'].str.contains('polski', case=False, na=False)) & ((final_df['Rok_koncowy'] >= 1945) | (final_df['Rok_koncowy'] == 0))]


df_deleted = final_df[~(final_df['Język'].str.contains('polski', case=False, na=False) & ((final_df['Rok_koncowy'] >= 1945) | (final_df['Rok_koncowy'] == 0)))]

df_filtered = final_df[final_df['Język'].str.contains('polski', case=False, na=False)]


#uwzględnić tylko te ktore w jezyk maja polski

df_filtered.to_excel('data/Czasopisma_dla_KP_2025-03-24.xlsx', index=False)  




# all_marc_series = []
# for element in bibs: 
#     marc_series = element['marc']
#     all_marc_series.append(marc_series)



 #%% Dla Karoliny 2025-04-22
 #Konferencja o Reymoncie
 #Reymont podmiotowa
 
import requests
import pandas as pd
import re
from collections import ChainMap

data = requests.get('https://data.bn.org.pl/api/networks/bibs.json?', params = {'kind': 'książka', 'author': 'Reymont, Władysław Stanisław', 'limit':100}).json()

bibs = data['bibs']
while data['nextPage'] != '':
    data = requests.get(data['nextPage']).json()
    bibs = bibs + data['bibs']

all_records = []
for element in bibs: 
    all_records.append(element)

#podmiotowa
all_records_list = []
for element in all_records:
    # element = all_records[2]
    
    author = next((subfield['a'] for field in element['marc']['fields'] for key, value in field.items() if key == '100' for subfield in value['subfields'] if 'a' in subfield), None)


    dictionary_of_records = {'Autor': author if author else element.get('author', None),
                             'Tytuł': element['title'],
                             'Wydawca': element['publisher'],
                             'Miejsce wydania': element['placeOfPublication'],
                             'Język': element['language'], 
                             'Typ': element['kind'], 
                             'Gatunek': element['genre'], 
                             'Forma': element['formOfWork'], 
                             'Rok': element['publicationYear']
                             }
    
    # dictionary_of_records = {'Autor': author,
    #                          'Tytuł': element['title'],
    #                          'Wydawca': element['publisher'],
    #                          'Miejsce wydania': element['placeOfPublication'],
    #                          'Język': element['language'], 
    #                          'Typ': element['kind'], 
    #                          'Gatunek': element['genre'], 
    #                          'Forma': element['formOfWork'], 
    #                          'Rok': element['publicationYear']
    #                          }
    
    all_records_list.append(dictionary_of_records)


final_df = pd.DataFrame(all_records_list)


# Filtrowanie. Rekordy od 1925 roku 

final_df['Rok'] = pd.to_numeric(final_df['Rok'], errors='coerce')
df_filtered = final_df[(final_df['Rok'] >= 1925) | (final_df['Rok'] == 0)]

df_filtered = df_filtered.sort_values(by='Rok', ascending=True)
df_filtered = df_filtered.drop_duplicate()



# df_deleted = final_df[~(final_df['Język'].str.contains('polski', case=False, na=False) & ((final_df['Rok_koncowy'] >= 1945) | (final_df['Rok_koncowy'] == 0)))]
# df_filtered = final_df[final_df['Język'].str.contains('polski', case=False, na=False)]


#uwzględnić tylko te ktore w jezyk maja polski
df_filtered.to_excel('data/KP_Reymont_podmiotowa_2025-04-22.xlsx', index=False)  




# all_marc_series = []
# for element in bibs: 
#     marc_series = element['marc']
#     all_marc_series.append(marc_series)


#%% Reymont przedmiotowa

data = requests.get('https://data.bn.org.pl/api/networks/bibs.json?', params = {'kind': 'książka', 'subject': 'Reymont, Władysław Stanisław', 'limit':100}).json()

bibs = data['bibs']
while data['nextPage'] != '':
    data = requests.get(data['nextPage']).json()
    bibs = bibs + data['bibs']

all_records = []
for element in bibs: 
    all_records.append(element)

all_records_list = []
for element in all_records:
    # element = all_records[2]
    
    author = next((subfield['a'] for field in element['marc']['fields'] for key, value in field.items() if key == '100' for subfield in value['subfields'] if 'a' in subfield), None)


    dictionary_of_records = {'Autor': author if author else element.get('author', None),
                             'Tytuł': element['title'],
                             'Wydawca': element['publisher'],
                             'Miejsce wydania': element['placeOfPublication'],
                             'Język': element['language'], 
                             'Typ': element['kind'], 
                             'Gatunek': element['genre'], 
                             'Forma': element['formOfWork'], 
                             'Rok': element['publicationYear'],
                             'ISBN/ISSN': element['isbnIssn']
                             }

    
    all_records_list.append(dictionary_of_records)


final_df = pd.DataFrame(all_records_list)


# Filtrowanie. Rekordy od 1925 roku 
final_df['Rok'] = pd.to_numeric(final_df['Rok'], errors='coerce')
df_filtered = final_df[(final_df['Rok'] >= 1925) | (final_df['Rok'] == 0)]

df_filtered = df_filtered.sort_values(by='Rok', ascending=True)

df_filtered.to_excel('data/KP_Reymont_przedmiotowa_2025-04-22.xlsx', index=False)  






            

     









