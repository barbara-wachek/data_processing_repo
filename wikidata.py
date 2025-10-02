#%% import

from SPARQLWrapper import SPARQLWrapper, JSON
import pandas as pd

#%% def
#Wyciagamy wszystkie wydawnictwa polskie 
#wd:Q36 = encja o identyfikatorze Q36 (czyli Polska).
#wd:Q2085381 = encja o identyfikatorze Q2085381 (czyli "wydawnictwo" / publishing house).'''


# Endpoint Wikidata
endpoint_url = "https://query.wikidata.org/sparql"

# Zapytanie SPARQL – pobranie wszystkich polskich wydawnictw z dodatkowymi właściwościami
query = """
SELECT ?wydawnictwo ?wydawnictwoLabel ?strona ?branżaLabel ?siedzibaLabel ?zalozylLabel ?data_zalozenia ?ISNI ?VIAF
WHERE {
  ?wydawnictwo wdt:P31 wd:Q2085381;
               wdt:P17 wd:Q36.
  
  OPTIONAL { ?wydawnictwo wdt:P856 ?strona }           # strona WWW
  OPTIONAL { ?wydawnictwo wdt:P452 ?branża }           # branża / przemysł
  OPTIONAL { ?wydawnictwo wdt:P159 ?siedziba }         # siedziba
  OPTIONAL { ?wydawnictwo wdt:P112 ?zalozyl }          # założyciel
  OPTIONAL { ?wydawnictwo wdt:P571 ?data_zalozenia }   # data założenia
  OPTIONAL { ?wydawnictwo wdt:P214 ?VIAF }             # kod VIAF

  SERVICE wikibase:label { bd:serviceParam wikibase:language "pl,en". }
}
"""

# Wykonanie zapytania
sparql = SPARQLWrapper(endpoint_url)
sparql.setQuery(query)
sparql.setReturnFormat(JSON)
results = sparql.query().convert()

# Konwersja wyników do listy słowników
data = []
for r in results["results"]["bindings"]:
    data.append({
        "QID": r["wydawnictwo"]["value"].split("/")[-1],
        "Wydawnictwo": r["wydawnictwoLabel"]["value"],
        "Strona WWW": r.get("strona", {}).get("value", ""),
        "Branża": r.get("branżaLabel", {}).get("value", ""),  # nazwa branży
        "Siedziba": r.get("siedzibaLabel", {}).get("value", ""),
        "Założyciel": r.get("zalozylLabel", {}).get("value", ""),
        "Data założenia": pd.to_datetime(r.get("data_zalozenia", {}).get("value", ""), errors='coerce').date()
        if r.get("data_zalozenia") else "",
        "VIAF": r.get("VIAF", {}).get("value", "")
    })

# Tworzenie DataFrame i zapis do CSV
df = pd.DataFrame(data)
df['Data założenia'] = pd.to_datetime(df['Data założenia'], errors='coerce').dt.date
df['VIAF'] = df['VIAF'].apply(lambda x: f"https://viaf.org/en/viaf/{x}" if x else "")
df['QID'] = df['QID'].apply(lambda x: f"https://www.wikidata.org/wiki/{x}" if x else "")

#Zapis do formatu xlsx
df.to_excel("data/wydawnictwa_iPBL.xlsx", index=False, engine='openpyxl')














