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


from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By



#%% Downloading and processing file + create list of links

#Wczytanie pliku csv pobranego z linku: https://docs.google.com/spreadsheets/d/1ZN5UdQwla4lLvyXZk_2Ug-PAWUrcarEmT0nqSTwdy5Y/edit#gid=1883655234

#laptop = "C:\\Users\\Barbara Wachek\\Desktop\\BazHum – czasopisma do pozyskania - lista linków numerów do pozyskania.csv"
#stacjonarka = "C:\\Users\\PBL_Basia\\Desktop\\BazHum – czasopisma do pozyskania - lista linków numerów do pozyskania.csv"

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
    #link = 'https://bazhum.muzhp.pl/czasopismo/187/?idvol=1257'
    #link = 'https://bazhum.muzhp.pl/czasopismo/662/?idno=15525'
    #link = 'https://bazhum.muzhp.pl/czasopismo/652/?idno=15289'
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
            author = author.text.strip()
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
                                 'Autor' : author,
                                 'Strony' : pages,
                                 'PDF': pdf_link
                                 }
        
        all_results.append(dictionary_of_article)
        
        
    
#%% main 

all_results = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_article, list_of_links), total=len(list_of_links)))    

df = pd.DataFrame(all_results)

#df_without_duplicates = df.drop_duplicates()   
   
  
#%% Notatki i testy    
# def dataframe_of_volume(link):
# #for link in tqdm(list_of_links):
#     #link = 'https://bazhum.muzhp.pl/czasopismo/187/?idvol=1257'
#     #link = 'https://bazhum.muzhp.pl/czasopismo/662/?idno=15525'
#     link = 'https://bazhum.muzhp.pl/czasopismo/662/?idno=15525'
#     link = 'https://bazhum.muzhp.pl/czasopismo/652/?idno=15289'
#     html_text = requests.get(link).text
#     while 'Error 503' in html_text:
#         time.sleep(6)
#         html_text = requests.get(link).text
#     soup = BeautifulSoup(html_text, 'lxml')

        
#     titles_of_articles = [x for x in soup.find_all('td', class_='c1 tytul')]
#     if titles_of_articles:
#         titles_of_articles = [x.text.strip().replace('\n', ' ') for x in soup.find_all('td', class_='c1 tytul')]
#     else:
#         titles_of_articles = None
        
#     authors_of_articles = [x for x in soup.find_all('td', class_='c2 autor')]
#     if authors_of_articles:
#         authors_of_articles = [x.text.strip() for x in soup.find_all('td', class_='c2 autor')]
#     else:
#         authors_of_articles = None
        
#     pages_of_articles = [x for x in soup.find_all('td', class_='c3 strony')]
#     if pages_of_articles:
#         pages_of_articles = [x.text.strip() for x in soup.find_all('td', class_='c3 strony')]
#     else:
#         pages_of_articles = None
        
#     # try:   
#     #     pdf_links = [x.a['href'] for x in soup.find_all('td', class_='c1 tytul')]
#     # except TypeError:
#     #     pdf_links = None
        
#     try:   
#         pdf_links = [x['href'] for x in soup.find_all('a', class_='czynnosc') if re.match(r'.*pdf$', x['href'])]
#     except TypeError:
#         pdf_links = None    
            
        
        
#     # try:   
#     #     pdf_links = [x['href'] for x in soup.find('td', class_='c4').findChildren('a', class_='czynnosc')]
#     # except TypeError:
#     #     pdf_links = None    
        
        
        
#     try:
#         for x in range(len(titles_of_articles)): 
            
#             dictionary_of_article = {'Link' : link,
#                                      'Tytuł': titles_of_articles[x],
#                                      'Autor' : authors_of_articles[x],
#                                      'Strony' : pages_of_articles[x],
#                                      'PDF': pdf_links[x] if pdf_links[x] else None
#                                      }
#             all_results.append(dictionary_of_article)
            
#     except TypeError:
#         print('Link bez artykułów: ' + link)
#     # except IndexError: 
#     #     print('Prawdopodobnie liczba linków pdf nie zgadza sie z liczbą tytułów: ' + link )
#     #     dictionary_of_article = {'Link' : link,
#     #                               'Tytuł': titles_of_articles[x],
#     #                               'Autor' : authors_of_articles[x],
#     #                               'Strony' : pages_of_articles[x],
#     #                               'PDF': None
#     #                               }
#     #     all_results.append(dictionary_of_article)
    
  
        
    
    
    
    
# #%% main    
# all_results = []
# with ThreadPoolExecutor() as excecutor:
#     list(tqdm(excecutor.map(dataframe_of_volume, list_of_links), total=len(list_of_links)))
    
    
# #Wyskakuje ConnectionError
# df = pd.DataFrame(all_results)  


# #df.drop_duplicates()   
# #Czy nie chcemy mieć też tytułu czasopisma, rocznika i tomu?       
        
        
#     #3531    
  
    
  
    
# #%% TEST  selenium 

   
# #def dataframe_of_volume(link):
# for link in tqdm(list_of_links):
#     #link = 'https://bazhum.muzhp.pl/czasopismo/187/?idvol=1257'
#     chrome_options = Options()
#     chrome_options.headless = True    
    
#     driver = webdriver.Chrome(options=chrome_options)
#     driver.get(link)
#     #delay = 3
#     soup = BeautifulSoup(driver.page_source, 'lxml')
    
        
        
#     titles_of_articles = [x for x in soup.find_all('td', class_='c1 tytul')]
#     if titles_of_articles:
#         titles_of_articles = [x.text.strip().replace('\n', ' ') for x in soup.find_all('td', class_='c1 tytul')]
#     else:
#         titles_of_articles = None
        
#     authors_of_articles = [x for x in soup.find_all('td', class_='c2 autor')]
#     if authors_of_articles:
#         authors_of_articles = [x.text.strip() for x in soup.find_all('td', class_='c2 autor')]
#     else:
#         authors_of_articles = None
        
#     pages_of_articles = [x for x in soup.find_all('td', class_='c3 strony')]
#     if pages_of_articles:
#         pages_of_articles = [x.text.strip() for x in soup.find_all('td', class_='c3 strony')]
#     else:
#         pages_of_articles = None
        
#     try:   
#         pdf_links = [x.a['href'] for x in soup.find_all('td', class_='c1 tytul')]
#     except TypeError:
#         pdf_links = None
    

    
#     for x in range(len(titles_of_articles)): 
#         dictionary_of_article = {'Link' : link,
#                                  'Tytuł': titles_of_articles[x],
#                                  'Autor' : authors_of_articles[x],
#                                  'Strony' : pages_of_articles[x],
#                                  'PDF': pdf_links[x] if pdf_links != None else None
#                                  }

        
#         all_results.append(dictionary_of_article)





# all_results = []
# with ThreadPoolExecutor() as excecutor:
#     list(tqdm(excecutor.map(dataframe_of_volume, list_of_links), total=len(list_of_links)))






# #%% TEST zip

# def dataframe_of_volume(link):
#     #link = 'https://bazhum.muzhp.pl/czasopismo/187/?idvol=1257'
#     html_text = requests.get(link).text
#     while 'Error 503' in html_text:
#         time.sleep(6)
#         html_text = requests.get(link).text
#     soup = BeautifulSoup(html_text, 'lxml')
 
        
#     titles_of_articles = [x for x in soup.find_all('td', class_='c1 tytul')]
#     if titles_of_articles:
#         titles_of_articles = [x.text.strip().replace('\n', ' ') for x in soup.find_all('td', class_='c1 tytul')]
#     else:
#         titles_of_articles = None
        
#     authors_of_articles = [x for x in soup.find_all('td', class_='c2 autor')]
#     if authors_of_articles:
#         authors_of_articles = [x.text.strip() for x in soup.find_all('td', class_='c2 autor')]
#     else:
#         authors_of_articles = None
        
#     pages_of_articles = [x for x in soup.find_all('td', class_='c3 strony')]
#     if pages_of_articles:
#         pages_of_articles = [x.text.strip() for x in soup.find_all('td', class_='c3 strony')]
#     else:
#         pages_of_articles = None
        
#     try:   
#         pdf_links = [x.a['href'] for x in soup.find_all('td', class_='c1 tytul')]
#     except TypeError:
#         pdf_links = None
    
    
#     # for x in range(len(titles_of_articles)): 
#     #     dictionary_of_article = {'Link' : link,
#     #                              'Tytuł': titles_of_articles[x],
#     #                              'Autor' : authors_of_articles[x],
#     #                              'Strony' : pages_of_articles[x],
#     #                              'PDF': pdf_links[x] if pdf_links != None else None
#     #                             }
    
    
#     zipped_list = list(zip(titles_of_articles, authors_of_articles, pages_of_articles, pdf_links))
   
#     all_results.extend(zipped_list)

    

# all_results = []
# with ThreadPoolExecutor() as excecutor:
    #list(tqdm(excecutor.map(dataframe_of_volume, list_of_links), total=len(list_of_links)))    














    
    
    
    