#%% Import
import requests
from bs4 import BeautifulSoup
import pandas as pd
from tqdm import tqdm  #licznik
from concurrent.futures import ThreadPoolExecutor
import time
from pathlib import Path
import numpy as np
import re

from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive


#%% Downloading and processing file + create list of links

#Link: https://docs.google.com/spreadsheets/d/1ZN5UdQwla4lLvyXZk_2Ug-PAWUrcarEmT0nqSTwdy5Y/edit#gid=1883655234

with open("C:\\Users\\PBL_Basia\\Desktop\\BazHum – czasopisma do pozyskania - lista linków numerów do pozyskania.csv", 'r', encoding='utf-8') as file:
    data = file.read()

list_of_rows = data.split('\n')

list_of_links = []
for row in list_of_rows:
    try: 
        index = row.index('http')
        link = row[index:]
        list_of_links.append(link)
    except ValueError:
        pass
   
    
#%% def   
def dictionary_of_article(link):
    html_text = requests.get(link).text
    while 'Error 503' in html_text:
        time.sleep(6)
        html_text = requests.get(link).text
    soup = BeautifulSoup(html_text, 'lxml')
    
    tr_elements = [x for x in soup.find_all('tr', class_=None)]    
    for x in tr_elements[6:]:
        title = x.find('td', class_='c1 tytul')
        if title:
            title = title.text.strip()
        else:
            title = None
        author = x.find('td', class_='c2 autor')
        if author:
            author = author.text.strip().replace('\n', ' | ')
        else:
            author = None
        pages = x.find('td', class_='c3 strony')
        if pages:
            pages = pages.text.strip()
        else:
            pages = None
            
        pdf_link = x.find('a', class_='czynnosc')
        if pdf_link:
            pdf_link = pdf_link['href']
        else:
            pdf_link = None
        
        dictionary_of_article = {'Link' : link,
                                 'Tytuł': title,
                                 'Autorzy' : author,
                                 'Strony' : pages,
                                 'PDF': pdf_link
                                 }
        
        all_results.append(dictionary_of_article)
        
        
    
#%% main 

all_results = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_article, list_of_links), total=len(list_of_links)))    

df = pd.DataFrame(all_results)

with pd.ExcelWriter(f"BazHum.xlsx", engine='xlsxwriter', options={'strings_to_urls': False}) as writer:    
    df.to_excel(writer, index=False, encoding='utf-8')   
    writer.save()  
   
    

  
        

#%% Pobranie pdfów z linków, gdzie decyzja jest TAK (puste pole)
   
#Link = 'https://docs.google.com/spreadsheets/d/1ZN5UdQwla4lLvyXZk_2Ug-PAWUrcarEmT0nqSTwdy5Y/edit#gid=66593903'

with open("C:\\Users\\PBL_Basia\\Desktop\\BazHum – czasopisma do pozyskania - web scraping.csv", 'r', encoding='utf-8') as file:
    df = pd.read_csv(file) 
    
#df.shape
#df_negative_decisions = df[df['Decyzja'].notna()]
df_positive_decisions = df[df['Decyzja'].isna()]
df_positive_decisions['Plik PDF'] = np.nan


# df_positive_decisions_links = df_positive_decisions['PDF']



list_of_files_paths = []

for link in tqdm(df_positive_decisions['PDF']):
    # link = 'http://bazhum.muzhp.pl/media//files/Teksty_teoria_literatury_krytyka_interpretacja/Teksty_teoria_literatury_krytyka_interpretacja-r1981-t-n6_(60)/Teksty_teoria_literatury_krytyka_interpretacja-r1981-t-n6_(60)-s105-121/Teksty_teoria_literatury_krytyka_interpretacja-r1981-t-n6_(60)-s105-121.pdf'
    #link = 'http://bazhum.muzhp.pl/media//files/Acta_Universitatis_Lodziensis_Folia_Litteraria_Polonica/Acta_Universitatis_Lodziensis_Folia_Litteraria_Polonica-r2005-t7-n1/Acta_Universitatis_Lodziensis_Folia_Litteraria_Polonica-r2005-t7-n1-s303-315/Acta_Universitatis_Lodziensis_Folia_Litteraria_Polonica-r2005-t7-n1-s303-315.pdf'
  
    format_name = re.sub(r'(http\:\/\/bazhum\.muzhp\.pl\/media\/\/files\/)(.*\/)(.*)(\.pdf$)', r'\3', link)
    file_path = f'C:\\Users\\PBL_Basia\\Documents\\My scripts\\BazHum_PDF-y\\{format_name}.pdf'
    
    filename = Path(file_path)
    response = requests.get(link)
    filename.write_bytes(response.content)
    
    list_of_files_paths.append(file_path)
 
    
 
    
dict_of_drive_links = {}   
    
gauth = GoogleAuth()           
drive = GoogleDrive(gauth)   
  
upload_file_list = list_of_files_paths
for upload_file in upload_file_list:
    format_name = re.sub('(C\:\\\\Users\\\\PBL_Basia\\\\Documents\\\\My scripts\\\\BazHum_PDF-y\\\\)(.*)', r'\2', upload_file)
    gfile = drive.CreateFile({'parents': [{'id': '1NgarB8afK-v7J9LjJORjegmd8-7Dw99w'}], 'title': format_name})  
    gfile.SetContentFile(upload_file)
    gfile.Upload()  
    drive_link = gfile.metadata['alternateLink']
    dict_of_drive_links[format_name] = drive_link




# upload_file = file_path
# gfile = drive.CreateFile({'parents': [{'id': '1NgarB8afK-v7J9LjJORjegmd8-7Dw99w'}], 'title': format_name})  
# gfile.SetContentFile(upload_file)
# gfile.Upload()  

# drive_link = gfile.metadata['alternateLink']


    

      
upload_file_list = list_of_files_paths
for upload_file in upload_file_list:
	gfile = drive.CreateFile({'parents': [{'id': '19t1szTXTCczteiKfF2ukYsuiWpDqyo8f'}]})  
	gfile.SetContentFile(upload_file)
	gfile.Upload()  

    

#Uzupełnienie danych w df

for k,v in dict_of_drive_links.items():
    df_positive_decisions[df_positive_decisions[]]  #jesli = link to PDF link = drive_link



# filename = Path(f'{link}.pdf')
# link = 'http://bazhum.muzhp.pl/media//files/Teksty_teoria_literatury_krytyka_interpretacja/Teksty_teoria_literatury_krytyka_interpretacja-r1981-t-n6_(60)/Teksty_teoria_literatury_krytyka_interpretacja-r1981-t-n6_(60)-s105-121/Teksty_teoria_literatury_krytyka_interpretacja-r1981-t-n6_(60)-s105-121.pdf'
# response = requests.get(link)
# filename.write_bytes(response.content)



    
    
    
    