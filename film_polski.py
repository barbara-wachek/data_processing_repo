#%% import 

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
import time
from concurrent.futures import ThreadPoolExecutor
import requests


import pandas as pd
import regex as re
from tqdm import tqdm  #licznik
from datetime import datetime
import json
from pandas import json_normalize



#%%Poszczególne sztuki (utwory) mogą miec kilka różnych wystawień scenicznych:
  
def get_movies_and_art_links(link): 
    # link = 'https://www.filmpolski.pl/fp/index.php?osoba=111677'
    
    html_text = requests.get(link).text

    while 'Error 503' in html_text:
        time.sleep(2)
        html_text = requests.get(link).text
    
    soup = BeautifulSoup(html_text, 'lxml')
    
    try:
        containers = []
    
        # dodajemy <div id="filmografia">
        filmografia_div = soup.find('div', id='filmografia')
        if filmografia_div:
            containers.append(filmografia_div)
    
        # dodajemy <ul id="osoba_pierwowzory"> i <ul id="osoba_varia">
        for ul_id in ['osoba_pierwowzory', 'osoba_varia']:
            ul = soup.find('ul', id=ul_id)
            if ul:
                containers.append(ul)
    
        # wyciągamy linki pasujące do wzorca z wszystkich znalezionych kontenerów
        links = ['https://filmpolski.pl/fp/' + e.get('href')
                 for container in containers
                 for e in container.find_all('a')
                 if (href := e.get('href')) and re.search(r'^index\.php\/.*', href)]
    
    except AttributeError:
        links = []

    return links


def fetch_html(link):
    while True:
        response = requests.get(link)
        html_bytes = response.content  # surowe bajty

        try:
            html_text = html_bytes.decode('windows-1250')  # kluczowe!
        except UnicodeDecodeError:
            html_text = html_bytes.decode('utf-8', errors='replace')

        if 'Error 503' not in html_text:
            return html_text
        time.sleep(2)




def dictionary_of_art(link):
    # link = 'https://filmpolski.pl/fp/index.php?film=524678'

    html_text = fetch_html(link)
    soup = BeautifulSoup(html_text, 'lxml')

    # Inicjalizacja słownika wynikowego
    results = {}
    
    results["Link"] = link  
    
    about_film = soup.find('article', {'id':'film'})
    
    try:
        title = about_film.find('h1').text.title()
    except: 
        title = None

    try: 
        information = " | ".join([x.text for x in soup.find('ul', class_='tech').find_all('li')])
    except: 
        information = None
    
    results['Informacje'] = information
    
    try:
        type_of_art = re.search(r'^(.*?)\s*\|', information).group(1)
    except:
        type_of_art = None
        
    results['Typ'] = type_of_art
    
    try:
        year = re.search(r'Rok produkcji:(\d{4})', information).group(1)
    except:
        year = None
        
    results['Rok produkcji'] = year
    
    
    try:
        premiere = re.search(r'Premiera:\s*([\d]{4}\.\s*\d{2}\.\s*\d{2})', information).group(1)
        date = datetime.strptime(premiere.strip(), "%Y. %m. %d")
        date = date.strftime("%Y-%m-%d")
    except:
        date = None
    
    results['Data premiery'] = date
        

    results["Tytuł"] = title
    
    for li in soup.find_all('li'):
        children = [child for child in li.children if child.name == 'div']
        if children:
            first_div = children[0]
            if first_div.get_text(strip=True) == "Obsada aktorska":
                wynik = li
                break
    
    try:
        book = " | ".join([x.text.strip().title() for x in about_film.find('table', {'id':'pierwowzory524678'}).find_all('td', class_='varia_tytul')])
    except:
        book = None
    
    results['Pierwowzór'] = book
    
    
    ekipa_part = about_film.find('ul', class_='ekipa')
            
    
    for x in ekipa_part.find_all('li'):
        key = None
        value = ''
        for element in x.find_all('div'):
            if 'ekipa_funkcja' in element.get('class'):
                key = element.text
            elif 'ekipa_osoba' in element.get('class'):
                value = value + element.text
            elif 'ekipa_opis' in element.get('class'):
                if len(element.text) > 3:
                    value = value + " (" + element.text.strip() + ") | "
                else: 
                    value = value + " | "
            results[key] = value
        
    

    all_results.append(results)
    
    
    

#%%main
all_links = get_movies_and_art_links('https://www.filmpolski.pl/fp/index.php?osoba=111677')


all_results = []       
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_art, all_links),total=len(all_links)))      

df = pd.DataFrame(all_results)
df = df.fillna('brak')

with pd.ExcelWriter(f"data/KP_Reymont_FilmPolski_{datetime.today().date()}.xlsx", engine='xlsxwriter') as writer:    
    df.to_excel(writer, 'Posts', index=False)   


    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    