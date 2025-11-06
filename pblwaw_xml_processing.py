#%% import 
from __future__ import unicode_literals
import requests
from bs4 import BeautifulSoup
import pandas as pd
import regex as re
from datetime import datetime
import json
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor
import time



#%% def

#1. Zaimportować plik xml z czasopismami
#2. Parser xml - pozyskać numery dla każdego tytułu i rocznika
#3 DF kolumny: Tytuł, Podtytuł, Rocznik, Numer


file_path = "data\\xml_output_2023-10-05\\import_journals_0.xml"

# Otwórz i wczytaj zawartość pliku
with open(file_path, 'r', encoding='utf-8') as f:
    xml_content = f.read()

# Stwórz obiekt BeautifulSoup do analizy XML
soup = BeautifulSoup(xml_content, 'xml')  # lub 'lxml-xml' jeśli masz lxml zainstalowane

# Przykłady analizy:
# 1️⃣ Wyświetlenie całego drzewa XML w czytelnej formie
print(soup.prettify())

# 2️⃣ Znalezienie wszystkich elementów o danej nazwie, np. <author>
authors = soup.find_all('author')
for author in authors:
    print(author.text)

# 3️⃣ Pobranie konkretnego elementu po nazwie tagu
title = soup.find('title')
if title:
    print("Tytuł:", title.text)

# 4️⃣ Pobranie wartości atrybutu np. <record id="123">
records = soup.find_all('record')
for record in records:
    record_id = record.get('id')
    print("ID rekordu:", record_id)
