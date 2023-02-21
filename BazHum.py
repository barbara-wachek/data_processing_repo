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
with open("C:\\Users\PBL_Basia\\Desktop\\BazHum – czasopisma do pozyskania - lista linków numerów do pozyskania.csv", 'r', encoding='utf-8') as file:
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
    

# Dane do zebrania: 
    #Link tomu (ten sam ktory analizuję)
    # Tytuł artykułu
    # Autorzy artykułu 
    # Strony artykułu 
    # Link do pliku pdf