#%% Import
import requests
from bs4 import BeautifulSoup
import pandas as pd
from tqdm import tqdm  #licznik
from concurrent.futures import ThreadPoolExecutor
import time


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
   
    

  
        




















    
    
    
    