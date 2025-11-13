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





#%% 1) df_xml jako baza (wszystkie rekordy z PBL)
# 2) uzupełnić o podtytuł z drugiej tabeli (Załącznik nr 3)
# 3) Nowa kolumna Czy z BN? - TAK jesli jest równiew w tabeli Załącznik nr 3
# 4) Numery wyjęte z pliku xml 

from bs4 import BeautifulSoup
import pandas as pd
import re

# === ŚCIEŻKI DO PLIKÓW ===
file_path = "data/xml_output_2023-10-05/import_journals_0.xml"
table_website = "data/ZACZNI~1.XLS"

# === WCZYTANIE XML ===
with open(file_path, 'r', encoding='utf-8') as f:
    xml_content = f.read()

soup = BeautifulSoup(xml_content, 'xml')

# === 1️⃣ Mapa id_journal -> tytuł ===
journal_titles = {}
for journal in soup.find_all('journal'):
    title_tag = journal.find('title')
    if not title_tag:
        continue
    title = title_tag.text.strip()
    journal_id = journal.get('id')
    journal_titles[journal_id] = title

# === 2️⃣ Funkcja pomocnicza do sortowania numerów ===
def extract_number_sort_key(value):
    if not value:
        return float('inf')  # brak numeru — na koniec
    match = re.search(r'\d+', value)
    if match:
        return int(match.group())
    else:
        return float('inf')

# === 3️⃣ Struktura: tytuł → roczniki → numery ===
journals_data = {}
for journal_number in soup.find_all('journal-number'):
    year_tag = journal_number.find('journal-year')
    if not year_tag:
        continue

    year_id = year_tag.get('id')  # np. nowa-fantastyka_1993
    parts = year_id.split('_')
    if len(parts) < 2:
        continue

    journal_id = '_'.join(parts[:-1])
    year = parts[-1]

    number_tag = journal_number.find('number')
    number = number_tag.text.strip() if number_tag else None

    title = journal_titles.get(journal_id)
    if not title:
        continue

    journals_data.setdefault(title, {}).setdefault(year, []).append(number)

# 4️Zamiana na DataFrame i sortowanie numerów
rows = []
for title, years_dict in journals_data.items():
    for year, numbers in years_dict.items():
        sorted_numbers = sorted(numbers, key=extract_number_sort_key)
        rows.append({
            "title": title,
            "year": int(year),
            "numbers": sorted_numbers
        })

df_xml = pd.DataFrame(rows)
df_xml['title_clean'] = df_xml['title'].str.strip().str.lower()

# === 5️⃣ Wczytanie Excela (tabela BN) ===
df_excel = pd.read_excel(table_website)
df_excel.columns = [col.strip() for col in df_excel.columns]

# Przygotowanie kolumn pomocniczych
df_excel['title_clean'] = df_excel['Tytuł'].str.strip().str.lower()
df_excel['Podtytuł_clean'] = df_excel['Podtytuł'].str.strip().str.lower()
df_excel['year'] = df_excel['Rocznik'].astype(str).str.extract(r'(\d{4})')
df_excel = df_excel.dropna(subset=['year'])
df_excel['year'] = df_excel['year'].astype(int)

# === 6️⃣ Funkcja sprawdzająca obecność w BN i zwracająca podtytuł ===
def check_bn_get_subtitle(row, df_bn):
    matches = df_bn[
        (df_bn['title_clean'] == row['title_clean']) &
        (df_bn['year'] == row['year'])
    ]
    if not matches.empty:
        # jeśli jest wiele podtytułów, bierzemy pierwszy
        return 'TAK', matches.iloc[0]['Podtytuł_clean']
    else:
        return 'NIE', None

# === 7️⃣ Zastosowanie funkcji do df_xml ===
df_xml[['Czy z BN?', 'Podtytuł']] = df_xml.apply(
    lambda row: pd.Series(check_bn_get_subtitle(row, df_excel)),
    axis=1
)

#  8️⃣ Kolumna z numerami
df_xml['Numer'] = df_xml['numbers'].apply(lambda x: ', '.join(map(str, x)) if isinstance(x, list) else '')

#  9️⃣ Ostateczny wygląd tabeli 
final_df = df_xml[['title', 'Podtytuł', 'year', 'Numer', 'Czy z BN?']]
final_df = final_df.sort_values(by=['title', 'year']).reset_index(drop=True)

#  10. Zapis do pliku 
final_df.to_excel("df_xml_z_kontrola_BN.xlsx", index=False)


print(final_df.head(10))

final_df.to_excel("data\df_xml_z_kontrola_BN.xlsx", index=False)












