#%%import
from __future__ import unicode_literals
import requests
from bs4 import BeautifulSoup
import pandas as pd
import regex as re
import time
from time import mktime
from tqdm import tqdm  #licznik
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import json
from matplotlib import pyplot as plt


#%% def


link = 'https://lubimyczytac.pl/autorzy?page=1&listId=authorsList&tab=All&orderBy=booksToReadAmountDesc&showFirstLetter=0&phrase=&paginatorType=Standard'
html_text = requests.get(link).text
soup = BeautifulSoup(html_text, 'lxml')
link_format = 'https://lubimyczytac.pl'
links = [x.a['href'] for x in soup.find_all('div', class_='col-4')]
links_authors = [f'{link_format}{x}' for x in links]

#40 najpopularniejszych: 
twenty_most_popular_authors = links_authors[1:21]

all_results = []

for link in tqdm(twenty_most_popular_authors):
    #link = 'https://lubimyczytac.pl/autor/54/stanislaw-lem'
    html_text = requests.get(link).text
    soup = BeautifulSoup(html_text, 'lxml')

    name = soup.find('div', class_='title-container').h1.text.strip()
    rating = float(soup.find('span', class_='big-number').text.strip().replace(',','.'))
    readers = [x.text for x in soup.find_all('li', class_='authorMain__ratingListItem')]
    number_of_readers = int((" ".join(re.findall(r'(\d+\s*\d*)', readers[0].strip()))).replace(" ", ""))
    #number_of_potential_readers = " ".join(re.findall(r'(\d+\s*\d*)', readers[1].strip())) 
    
    
    dictionary_of_authors_ratings = {"Imię i nazwisko": name,
                                     "Ocena": rating,
                                     "Liczba czytelników": number_of_readers
                                     }
    
    all_results.append(dictionary_of_authors_ratings)
 
    
 
df = pd.DataFrame(all_results)

#Zrobić z tego tabele i wykres? Dla ilustracji. Ale tłumaczyć głównie proces web scrapowania. Dodać link do githuba

#Wykres horyzontalny
df_sorted = df.sort_values(by="Ocena", ascending=True)
plt.figure(figsize = (8, 8))
horizontal_plot = plt.barh(df_sorted['Imię i nazwisko'], df_sorted['Liczba czytelników'], color = 'magenta')
horizontal_plot = plt.text(df_sorted['Ocena'])

#Wykres tradycyjny
df_sorted = df.sort_values(by="Ocena", ascending=False)

#df_without_number_of_readers = df_sorted.drop(columns='Liczba czytelników')

#df_without_rating = df_sorted.drop(columns='Ocena')

#test = df_without_rating.set_index('Imię i nazwisko')

test_2 = df_sorted.set_index('Imię i nazwisko')

plt.figure()
test_2.plot(xlabel='Autorzy', ylabel="Liczba czytelników", kind='bar', title='Popularność vs. ocena', color='magenta')
            
            
plt.xlabel('Autorzy')
plt.ylabel('Liczba czytelników')


test.plot(xlabel='Autorzy', ylabel="Liczba czytelników", kind='bar', title='Popularność vs. ocena', color='magenta')










