#%% ZADANIE 2022-10-28

#Stwórz zapytanie do API BN, aby otrzymać wszystkie rekordy, których autorem jest Borges. Uwaga: limit rekordów to 100. Jak uzyskać więcej rekordów niż maksymalny limit. 

#link do API BN https://data.bn.org.pl/networks/bibs
#link_query = https://data.bn.org.pl/api/networks/bibs.json?author=Borges+Jorge&amp;limit=100
#dokumentacja API = https://data.bn.org.pl/docs/bibs



#%% ROZWIĄZANIE
import requests
import pandas as pd
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
format_subfields = r'(fields\_\d{1,2}\_)(\d{3}\_)(subfields\_\d{1,2}\_\w)'

for x in data_frame_marc.columns:    
    if re.match(format_fields, x):
        new_name = re.sub(r'(fields\_\d{1,2}\_)(\d{3})$', r'\2', x)
        data_frame_marc.rename(columns={x: new_name}, inplace=True)
    elif re.match(format_ind, x):
        new_name = re.sub(r'(fields\_\d{1,2}\_)(\d{3}\_)(ind\d)', r'\2\3', x)
        data_frame_marc.rename(columns={x: new_name}, inplace=True)
    elif re.match(format_subfields, x):
        new_name = re.sub(r'(fields\_\d{1,2}\_)(\d{3}\_)(subfields\_\d{1,2}\_\w)', r'\2\3', x)
        data_frame_marc.rename(columns={x: new_name}, inplace=True)


#column_list = data_frame_marc.columns %Do sprawdzenia nazw kolumn

# test_list = []

# for column_name in data_frame_marc.columns:
#     number_of_column = re.sub(r'(\d{3})(\_)?(ind|subfields)?(\_)?(\d)(\_)?(\w)?', r'\1', column_name)
#     if number_of_column not in test_list:
#         test_list.append(number_of_column)
#         data_frame_marc.rename(columns={column_name:number_of_column}, inplace=True)
#         if 'ind' in column_name:
#             data_frame_marc[number_of_column] = data_frame_marc[number_of_column].replace(' ', '\\')
#     elif number_of_column in test_list: 
#         data_frame_marc.rename(columns={column_name:number_of_column}, inplace=True)
#         if 'ind' in column_name: 
#             data_frame_marc[number_of_column] = data_frame_marc[number_of_column].replace(' ', '\\')
            
# test_list = []

# for column_name in data_frame_marc.columns:
#     number_of_column = re.sub(r'(\d{3})(\_)?(ind|subfields)?(\_)?(\d)(\_)?(\w)?', r'\1', column_name)
#     data_frame_marc.rename(columns={column_name:number_of_column}, inplace=True)
#     if number_of_column not in test_list:
#         test_list.append(number_of_column)
        
#     if 'ind' in column_name:
#         data_frame_marc[column_name] = data_frame_marc[column_name].replace(' ', '\\')
#     if 'subfields' in column_name: 
#         number_and_letter_of_subfield = re.sub(r'(\d{3})(\_)(subfields)(\_)(\d{1,2})(\_)(\w)', r'\5\7', column_name)
        
#         try:
#             data_frame_marc[column_name] = data_frame_marc[column_name].str.replace(r'(^.*$)', r'$\1').str.replace(r'(\$)(.*)', r'\g<1>'+number_and_letter_of_subfield+r'\g<2>')
#         except AttributeError:
#             data_frame_marc[column_name] = data_frame_marc[column_name].apply(lambda x: x.str.replace(r'(^.*$)', r'$'+number_and_letter_of_subfield+ r'\g<1>❦')) 
           
   

    
#Wersja ostateczna:             
     
test_list = []
for column_name in data_frame_marc.columns:
    if 'ind' in column_name:
        data_frame_marc[column_name] = data_frame_marc[column_name].replace(' ', '\\')
        if column_name not in test_list: 
            test_list.append(column_name)
        
    if 'subfields' in column_name: 
        number_and_letter_of_subfield = re.sub(r'(\d{3})(\_)(subfields)(\_)(\d{1,2})(\_)(\w)', r'\5\7', column_name)
        if column_name not in test_list: #Jesli nazwy kolumny nie ma na liscie testowej tzn., ze pierwszy raz analizujemy kolumnę o takiej nazwie. Musimy dodać ją do listy testowej, a nastepnie mozemy podejsc do analizy: sprobowac dodac odpowiednie znaki na poczatku wartosci z poszczegolnych komorek tej kolumny. Jesli wyskoczy blad (AttributeError) oznacza to, ze mamy wiecej kolumn o tej samej nazwie, bo kod interpretuje to jako DataFrame. Wtedy dodajemy znaczek ❦ na koncu kazdej komorki z takich kolumn. Wazne tez, zeby po w kolejnej iteracji gdy probujemy analizowac kolejna kolumne o tej samej nazwie, zeby po prostu zostawiło ją - bo juz są na niej naniesione zmiany. 
            test_list.append(column_name)
            try:
                data_frame_marc[column_name] = data_frame_marc[column_name].str.replace(r'(^.*$)', r'$\1').str.replace(r'(\$)(.*)', r'\g<1>'+number_and_letter_of_subfield+r'\g<2>')
            except AttributeError:
                data_frame_marc[column_name] = data_frame_marc[column_name].apply(lambda x: x.str.replace(r'(^.*$)', r'$'+number_and_letter_of_subfield+ r'\g<1>❦')) 
                
        if column_name in test_list: 
            pass
            
 
            
#Dlaczego niektóre powtarzające się kolumny nie są obok siebie? 
#Kolejny krok: połączyć kolumny o tych samych poczatkach nazw (trzy pierwsze cyfry)
#Poprawić uzyćie znaczka ❦. Cos sie nie zgadza - moze to nie powinno byc dodane w tej funkcji, ale w kolejnej. BO teraz dodaje po porust w obrębie kolumny nie zwazajac na kontekt calego rekordu? (Np. 7 rekord pole subfields_35 w tabeli ) PRawdopodobnie trzeba bedzie wywalic ten znaczek z powyzszego kodu i dodac go dopiero po scaleniu kolumn (wtedy bedzie sprawdzenie czy dana komorka zawiera kilka $+liczba+litera - to znaczy, ze ma kilka tych samych podpól?)


    
    


















