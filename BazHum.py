#%% Import
import requests
from bs4 import BeautifulSoup
import pandas as pd
import regex as re
from tqdm import tqdm  #licznik
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import json

import time


#%% Downloading and processing file + create list of links

#Wczytanie pliku csv pobranego z linku: https://docs.google.com/spreadsheets/d/1ZN5UdQwla4lLvyXZk_2Ug-PAWUrcarEmT0nqSTwdy5Y/edit#gid=1883655234

#laptop = "C:\Users\Barbara Wachek\Desktop\BazHum – czasopisma do pozyskania - lista linków numerów do pozyskania.csv"


with open("C:\\Users\\Barbara Wachek\\Desktop\\BazHum – czasopisma do pozyskania - lista linków numerów do pozyskania.csv", 'r', encoding='utf-8') as file:
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

    
list_of_responses = []    
   
def dataframe_of_volume(link):
    link = 'https://bazhum.muzhp.pl/czasopismo/187/?idvol=1257'
    html_text = requests.get(link).text
    dict_of_responses = {link : requests.get(link)}
    list_of_responses.append(dict_of_responses)
    while 'Error 503' in html_text:
        time.sleep(6)
        html_text = requests.get(link).text
    soup = BeautifulSoup(html_text, 'lxml')
    
        
    titles_of_articles = [x for x in soup.find_all('td', class_='c1 tytul')]
    if titles_of_articles:
        titles_of_articles = [x.text.strip().replace('\n', ' ') for x in soup.find_all('td', class_='c1 tytul')]
    else:
        titles_of_articles = None
        
    authors_of_articles = [x for x in soup.find_all('td', class_='c2 autor')]
    if authors_of_articles:
        authors_of_articles = [x.text.strip() for x in soup.find_all('td', class_='c2 autor')]
    else:
        authors_of_articles = None
        
    pages_of_articles = [x for x in soup.find_all('td', class_='c3 strony')]
    if pages_of_articles:
        pages_of_articles = [x.text.strip() for x in soup.find_all('td', class_='c3 strony')]
    else:
        pages_of_articles = None
        
    try:   
        pdf_links = [x.a['href'] for x in soup.find_all('td', class_='c1 tytul')]
    except TypeError:
        pdf_links = None
    
    
    # df_1 = pd.Series(titles_of_articles, name='Tytuł artykułu') 
    # df_2 = pd.Series(authors_of_articles, name='Autor') 
    # df_3 = pd.Series(pages_of_articles, name='Strony') 
    # df_4 = pd.Series(pdf_links, name='PDF')
    
    # #df_5 = pd.Series(len(titles_of_articles)*(link+', ').split(), name='Link')

    # df_volume = pd.concat([df_1, df_2, df_3, df_4], axis=1)
    
    # final_df = pd.concat([final_df, df_volume])
    
    for x in range(len(titles_of_articles)): 
        dictionary_of_article = {'Link' : link,
                                 'Tytuł': titles_of_articles[x],
                                 'Autor' : authors_of_articles[x],
                                 'Strony' : pages_of_articles[x],
                                 'PDF': pdf_links[x] if pdf_links != None else None
                                 }

        
        all_results.append(dictionary_of_article)
    
    
    
    
#%% main    
all_results = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dataframe_of_volume, list_of_links), total=len(list_of_links)))
    
    
#Wyskakuje ConnectionError
df = pd.DataFrame(all_results)  


#df.drop_duplicates()   
#Czy nie chcemy mieć też tytułu czasopisma, rocznika i tomu?       
        
        
        
    
    
    
    
    