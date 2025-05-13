#%% import 

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
import time
from concurrent.futures import ThreadPoolExecutor



import pandas as pd
import regex as re
from tqdm import tqdm  #licznik
from datetime import datetime
import json



# Inicjalizacja przeglądarki
driver = webdriver.Chrome()
driver.get("https://encyklopediateatru.pl/sztuki/wyszukaj?search=Reymont")

# Poczekaj na załadowanie danych
time.sleep(5)

# Pobierz źródło strony
soup = BeautifulSoup(driver.page_source, "html.parser")

# Znajdź wszystkie linki do przedstawień
art_links = []
for link in soup.find_all('a', class_='title'):
    href = link.get("href")
    art_links.append('https://encyklopediateatru.pl' + href)

driver.quit()


    
def dictionary_of_art(art_link):
    
    # art_link = 'https://encyklopediateatru.pl/przedstawienie/71189/bunt'
    html_text = requests.get(art_link).text

    while 'Error 503' in html_text:
        time.sleep(2)
        html_text = requests.get(art_link).text
    
    soup = BeautifulSoup(html_text, 'lxml')
    
    # Inicjalizacja słownika wynikowego
    results = {}
    
    results["Link"] = art_link    
    
    # Pobieranie autora
    try:
        author = soup.find('h2', class_='authors').text.strip()
    except:
        author = None
    results["Autor"] = author
    
    # Pobieranie tytułu
    try:
        title_of_art = soup.find('h3').text.strip()
    except:
        title_of_art = None
    results["Tytuł"] = title_of_art
    
    # Parsowanie sekcji grupowych
    groups = soup.find_all('div', class_=lambda x: x and x.startswith('dl-group'))
    
    for group in groups:
        group_title_h5 = group.find('h5', class_='dl-group-title')
        group_data = []
    
        # Sprawdzenie, czy mamy do czynienia z Obsada
        if group_title_h5 and 'Obsada' in group_title_h5.get_text(strip=True):
            obsada_text = []
    
            for dl in group.find_all('dl'):
                dt = dl.find('dt')
                dds = dl.find_all('dd')
    
                value = None
                for dd in dds:
                    text = dd.get_text(strip=True)
                    if text:
                        value = text
                        break
    
                if dt and value is not None:
                    label = dt.get_text(strip=True)
                    obsada_text.append(f"{label}: {value}")
    
            results['Obsada'] = " | ".join(obsada_text)
    
        else:
            for dl in group.find_all('dl'):
                dt = dl.find('dt')
                dds = dl.find_all('dd')
    
                value = None
                for dd in dds:
                    text = dd.get_text(strip=True)
                    if text:
                        value = text
                        break
    
                if dt and value is not None:
                    label = dt.get_text(strip=True)
                    group_data.append({label: value})
    
            if group_title_h5:
                title = group_title_h5.get_text(strip=True)
                if title in results:
                    results[title].extend(group_data)
                else:
                    results[title] = group_data
            else:
                for item in group_data:
                    results.update(item)
        
    all_results.append(results)
        
        
      
          
    
all_results = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_art, art_links),total=len(art_links)))
    
#Stworzenie DataFrame    
df = pd.DataFrame(all_results)
df['premiera'] = df['premiera'].str.replace(r'\s+', ' ', regex=True).str.strip()    

# Rozdzielenie daty i teatru
df[['Data surowa', 'Teatr']] = df['premiera'].str.extract(r'^(\d{1,2} \w+ \d{4})\s*(.+)$')

# Mapowanie miesięcy na liczby
miesiace = {
    'stycznia': '01', 'lutego': '02', 'marca': '03', 'kwietnia': '04',
    'maja': '05', 'czerwca': '06', 'lipca': '07', 'sierpnia': '08',
    'września': '09', 'października': '10', 'listopada': '11', 'grudnia': '12'
}

# Funkcja konwertująca datę na ISO format
def convert_date(date_str):
    try:
        day, month_word, year = date_str.split()
        month = miesiace.get(month_word.lower())
        return f"{year}-{month}-{int(day):02d}"
    except:
        return None

# Krok 5: Stwórz nową kolumnę z datą w formacie ISO
df['Data premiery'] = df['Data surowa'].apply(convert_date)

# Krok 6: Usuń kolumny pomocnicze
df.drop(columns=['premiera', 'Data surowa'], inplace=True)

# Krok 7: Opcjonalnie przesuń kolumnę 'Data premiery' na koniec
data_premiery = df.pop('Data premiery')
df['Data premiery'] = data_premiery
df = df.fillna('brak')
    

#%% saving

   
with pd.ExcelWriter(f"data/KP_Reymont_ETP_{datetime.today().date()}.xlsx", engine='xlsxwriter') as writer:    
    df.to_excel(writer, 'Posts', index=False)   
   


   
   
   
   
   
   
   
   
   
   