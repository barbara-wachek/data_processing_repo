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


# Wgrać plik mrk do Pythona i zrobić z niego tabelę DataFrame    

# with open("C:\\Users\\PBL_Basia\\Documents\\My scripts\\data_processing_repo\\Olga_Tokarczuk.mrk") as file:
#     f.read()

#UWAGA! Sciezka pliku zmienia sie w zaleznosci czy korzystam ze stacjonarki czy laptopa!
# file = open("C:\\Users\\Barbara Wachek\\Desktop\\Olga_Tokarczuk.mrk", "r")




list_of_marc_dicts = []
with open("C:\\Users\\Barbara Wachek\\Desktop\\Olga_Tokarczuk.mrk", 'r', encoding='utf-8') as file: 
    data = file.read()
    list_of_records = data.split('\n')
    
    for row in list_of_records:
        dictionary_of_field = {}
        if re.match(r'^\=\S{3}', row):
            key = re.sub(r'(^\=)(\S{3})(\s{2})(.*)', r'\2', row)
            value =  re.sub(r'(^\=)(\S{3})(\s{2})(.*)', r'\4', row)
            
            dictionary_of_field[key] = value
            list_of_marc_dicts.append(dictionary_of_field)

            
    
    #df = pd.DataFrame()
    final_list = []
    for element in list_of_marc_dicts:
        for key, value in element.items():
            if key == 'LDR':
                new_dict = {}
                new_dict[key] = value
            if key not in new_dict.keys(): 
                new_dict[key] = value    
            elif key in new_dict.keys(): 
                new_dict[key] = new_dict[key] + '❦'  + value
                
                
    final_list.append(new_dict)
           
   


    # dictionary_of_record = {}
    # for row in list_of_records_without_space:
    #     if re.match(r'^\=\S{3}', row):
    #         key = re.sub(r'(^\=)(\S{3})(\s{2})(.*)', r'\2', row)
    #         value =  re.sub(r'(^\=)(\S{3})(\s{2})(.*)', r'\4', row)
    #         if key not in  dictionary_of_record.keys():
    #             dictionary_of_record[key] = value
    #         else:
    #             dictionary_of_record[key] = dictionary_of_record[key] + '❦' + value
                
    #         list_of_marc_records.append(dictionary_of_record) 
    #     dictionary_of_record = {}
 






            

     









