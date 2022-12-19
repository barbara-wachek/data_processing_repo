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
format_ind = r'(fields\_\d{1,3}\_)(\d{3}\_)(ind\d)'
format_subfields = r'(fields\_\d{1,3}\_)(\d{3}\_)(subfields\_\d{1,2}\_\w)'

for x in data_frame_marc.columns:    
    if re.match(format_fields, x):
        new_name = re.sub(r'(fields\_\d{1,2}\_)(\d{3})$', r'\2', x)
        data_frame_marc.rename(columns={x: new_name}, inplace=True)
    elif re.match(format_ind, x):
        new_name = re.sub(r'(fields\_\d{1,3}\_)(\d{3}\_)(ind\d)', r'\2\3', x)
        data_frame_marc.rename(columns={x: new_name}, inplace=True)
    elif re.match(format_subfields, x):
        new_name = re.sub(r'(fields\_\d{1,3}\_)(\d{3}\_)(subfields\_\d{1,2}\_\w)', r'\2\3', x)
        data_frame_marc.rename(columns={x: new_name}, inplace=True)


column_list = data_frame_marc.columns #Do sprawdzenia nazw kolumn

     
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
                data_frame_marc[column_name] = data_frame_marc[column_name].apply(lambda x: x.str.replace(r'(^.*$)', r'$'+number_and_letter_of_subfield+ r'\g<1>')) 
                
        if column_name in test_list: 
            pass
            
 
 #  ❦        
   
   
   
#Dlaczego niektóre powtarzające się kolumny nie są obok siebie? 
#Kolejny krok: połączyć kolumny o tych samych poczatkach nazw (trzy pierwsze cyfry)
#Poprawić uzyćie znaczka ❦. Cos sie nie zgadza - moze to nie powinno byc dodane w tej funkcji, ale w kolejnej. BO teraz dodaje po porust w obrębie kolumny nie zwazajac na kontekst calego rekordu? (Np. 7 rekord pole subfields_35 w tabeli ) Prawdopodobnie trzeba bedzie wywalic ten znaczek z powyzszego kodu i dodac go dopiero po scaleniu kolumn (wtedy bedzie sprawdzenie czy dana komorka zawiera kilka $+liczba+litera - to znaczy, ze ma kilka tych samych podpól?)

    
#Dodanie znaczka ❦ i połączenie kolumn o tej samej liczbie początkowej w nazwie   

# format_column_name = f'(\d{3})(.*)?'
# format_column_subfields = '(\d{3})(\_subfields\_\d{1,2}\_\w)'


# for column_name in data_frame_marc.columns:
#     if data_frame_marc.columns.duplicated().any(): 
#         data_frame_marc[column_name] = data_frame_marc[column_name].str.replace(r'(^.*$)', r'\g<1>'+'❦ ')




#DO przemyslenia

for column_name in data_frame_marc.columns:
    column_name = '020_ind1'
    if re.match(r'(\d{3})(\_.*)', column_name):
        new_column = re.sub(r'(\d{3})(\_.*)?', r'\1', column_name)
        if new_column in data_frame_marc.columns:
            data_frame_marc[new_column] = data_frame_marc[new_column] + data_frame_marc[column_name]
            pass
            #data_frame_marc.drop(columns=[column_name], inplace=True)
        else:
            data_frame_marc[new_column] = data_frame_marc[column_name]
            #data_frame_marc.drop(columns=[column_name], inplace=True)











