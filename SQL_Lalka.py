import cx_Oracle
import pandas as pd
import re
from datetime import datetime
from data import SQL_access as sq #passy

import json



import networkx as nx
import matplotlib.pyplot as plt
from pyvis.network import Network


#%% Połączenie z bazą Oracle (dostęp w pliku SQL_dostep)

cx_Oracle.init_oracle_client(lib_dir=sq.LIB_DIR)
dsn_tns = cx_Oracle.makedsn(sq.HOSTNAME, sq.PORT, service_name=sq.SERVICE_NAME)
connection = cx_Oracle.connect(user=sq.USER, password=sq.PASSWORD, dsn=dsn_tns, encoding=sq.ENCODING)

cursor = connection.cursor()

#%% Odnalezienie rekordów przyporządkowanych pod utwór Lalka (id = 109715)

query= '''
SELECT z.za_zapis_id
FROM IBL_OWNER.pbl_zapisy z
WHERE z.za_za_zapis_id LIKE '109715'
'''

# cursor.execute(query)

df = pd.read_sql(query, con=connection)
id_records_attached_lalka = df['ZA_ZAPIS_ID'].tolist() #93 rekordy


#%% Sprawdzenie, czy którys z zapisów wydobytów do id_list ma jakies zapisy - czyli szukamy zagniezdzonych zapisow podpiętych pod który z tych ID

id_records_attached_lalka_str = ', '.join(map(str, id_records_attached_lalka))

query_2 = f'''
SELECT z.za_zapis_id
FROM IBL_OWNER.pbl_zapisy z
WHERE z.za_za_zapis_id IN ({id_records_attached_lalka_str})
'''

# cursor.execute(query_2)

df_2 = pd.read_sql(query_2, con=connection)
id_records_attached_lalka_one_level_below = df_2['ZA_ZAPIS_ID'].tolist() # 22 rekordy podpięte pod zapisy podpięte pod Lalkę (np. recenzje ksiazek o Lalce)


#%% Teraz chce z dwóch list otrzymac pełne rekordy bibliograficzne 

full_list_of_ids = id_records_attached_lalka + id_records_attached_lalka_one_level_below

full_list_of_ids_str = ', '.join(map(str, full_list_of_ids))

query_full_list = f'''
SELECT z.za_zapis_id, 
z.za_za_zapis_id AS zapis_nadrzedny, 
z.za_type, 
z.za_tytul, 
tw.tw_imie, 
tw.tw_nazwisko,
tw.tw_tworca_id, 
d.dz_nazwa, 
t.am_imie, 
t.am_nazwisko, 
t.am_autor_id,
zr.zr_tytul,
zr.zr_zrodlo_id,
z.za_zrodlo_rok, 
z.za_zrodlo_nr, 
z.za_zrodlo_str,
z.za_seria_wydawnicza,  
z.za_opis_wspoltworcow,
z.za_wydawnictwa,
w.wy_nazwa,
w.wy_wydawnictwo_id,
w.wy_miasto,
z.za_rok_wydania,
z.za_opis_fizyczny_ksiazki,
Z.ZA_ADNOTACJE,
Z.ZA_OPIS_IMPREZY,
Z.ZA_ORGANIZATOR
FROM IBL_OWNER.pbl_zapisy z
LEFT JOIN IBL_OWNER.pbl_zapisy_autorzy a on z.za_zapis_id = a.zaam_za_zapis_id
LEFT JOIN IBL_OWNER.pbl_autorzy t on a.zaam_am_autor_id = t.am_autor_id
left join IBL_OWNER.pbl_zapisy_tworcy ztw on ztw.zatw_za_zapis_id = z.za_zapis_id
left join IBL_OWNER.pbl_zrodla zr on z.za_zr_zrodlo_id = zr.zr_zrodlo_id
left join IBL_OWNER.pbl_tworcy tw on tw.tw_tworca_id = ztw.zatw_tw_tworca_id
LEFT JOIN IBL_OWNER.pbl_dzialy d on z.za_dz_dzial1_id = d.dz_dzial_id  
LEFT JOIN IBL_OWNER.pbl_zapisy_wydawnictwa zwyd on zwyd.zawy_za_zapis_id = z.za_zapis_id
LEFT JOIN IBL_OWNER.pbl_wydawnictwa w on w.wy_wydawnictwo_id = zwyd.zawy_wy_wydawnictwo_id
WHERE z.za_zapis_id IN ({full_list_of_ids_str})
'''

# cursor.execute(query_full_list)

df_full_list = pd.read_sql(query_full_list, con=connection)


#%% Wyszukanie ID wszystkich zapisów podpiętych pod Prusa jako twórcę 
# ID Prusa: TW_TWORCA_ID = 3426, AM_AUTOR_ID = 1082 

query_Prus_records = '''
SELECT  
    z.za_zapis_id
FROM IBL_OWNER.pbl_zapisy z
LEFT JOIN IBL_OWNER.pbl_zrodla zr ON z.za_zr_zrodlo_id = zr.zr_zrodlo_id
LEFT JOIN IBL_OWNER.pbl_zapisy_autorzy a ON z.za_zapis_id = a.zaam_za_zapis_id
LEFT JOIN IBL_OWNER.pbl_dzialy d ON z.za_dz_dzial1_id = d.dz_dzial_id
LEFT JOIN IBL_OWNER.pbl_autorzy t ON a.zaam_am_autor_id = t.am_autor_id
LEFT JOIN IBL_OWNER.pbl_rodzaje_zapisow r ON z.za_rz_rodzaj1_id = r.rz_rodzaj_id
LEFT JOIN IBL_OWNER.pbl_zapisy_tworcy ztw ON ztw.zatw_za_zapis_id = z.za_zapis_id
LEFT JOIN IBL_OWNER.pbl_tworcy tw ON tw.tw_tworca_id = ztw.zatw_tw_tworca_id
WHERE 
    (t.am_imie LIKE 'Boles%' AND t.am_nazwisko = 'Prus')
    OR
    (tw.tw_imie LIKE 'Boles%' AND tw.tw_nazwisko = 'Prus')
ORDER BY z.za_zapis_id
'''

# cursor.execute(query_Prus_records)

df_Prus = pd.read_sql(query_Prus_records, con=connection)
id_list_records_Prus = df_Prus['ZA_ZAPIS_ID'].tolist() #777 rekordów

# result = cursor.fetchall()  # to będzie lista tupli: [(id1,), (id2,), (id3,)]
# id_list_records_Prus = [row[0] for row in result] # 776 rekordów pod Prusem jako twórcą i Prus jako autor. 

#%% Sprawdzenie, czy którys z zapisów wydobytów do id_list_records_Prus_tworca ma jakies zapisy - czyli szukamy zagniezdzonych zapisow podpiętych pod który z tych ID

id_list_records_Prus_str = ', '.join(map(str, id_list_records_Prus))

query_nested_Prus_records = f'''
SELECT z.za_zapis_id
FROM IBL_OWNER.pbl_zapisy z
WHERE z.za_za_zapis_id IN ({id_list_records_Prus_str})
'''

cursor.execute(query_nested_Prus_records)

result_nested_records = cursor.fetchall()

id_list_nested_records_Prus = [row[0] for row in result_nested_records]


#%% DF z rekordów pod PRusem, Prusem jako autorem + rekordy, które są pod nie podpięte


full_list_of_Prus_records = id_list_records_Prus + id_list_nested_records_Prus #1035

if len(full_list_of_Prus_records) > 1000: 
    full_list_of_Prus_records_part_1 = full_list_of_Prus_records[:998]
    full_list_of_Prus_records_part_2 = full_list_of_Prus_records[998:]

full_list_of_Prus_records_part_1_str = ', '.join(map(str, full_list_of_Prus_records_part_1))

full_list_of_Prus_records_part_2_str = ', '.join(map(str, full_list_of_Prus_records_part_2))


query_full_list_of_Prus_records_part_1 = f'''
SELECT z.za_zapis_id, 
z.za_za_zapis_id AS zapis_nadrzedny, 
z.za_type, 
z.za_tytul, 
tw.tw_imie, 
tw.tw_nazwisko,
tw.tw_tworca_id, 
d.dz_nazwa, 
t.am_imie, 
t.am_nazwisko, 
t.am_autor_id,
zr.zr_tytul,
zr.zr_zrodlo_id,
z.za_zrodlo_rok, 
z.za_zrodlo_nr, 
z.za_zrodlo_str,
z.za_seria_wydawnicza,  
z.za_opis_wspoltworcow,
z.za_wydawnictwa,
w.wy_nazwa,
w.wy_wydawnictwo_id,
w.wy_miasto,
z.za_rok_wydania,
z.za_opis_fizyczny_ksiazki,
Z.ZA_ADNOTACJE,
Z.ZA_OPIS_IMPREZY,
Z.ZA_ORGANIZATOR
FROM IBL_OWNER.pbl_zapisy z
LEFT JOIN IBL_OWNER.pbl_zapisy_autorzy a on z.za_zapis_id = a.zaam_za_zapis_id
LEFT JOIN IBL_OWNER.pbl_autorzy t on a.zaam_am_autor_id = t.am_autor_id
left join IBL_OWNER.pbl_zapisy_tworcy ztw on ztw.zatw_za_zapis_id = z.za_zapis_id
left join IBL_OWNER.pbl_zrodla zr on z.za_zr_zrodlo_id = zr.zr_zrodlo_id
left join IBL_OWNER.pbl_tworcy tw on tw.tw_tworca_id = ztw.zatw_tw_tworca_id
LEFT JOIN IBL_OWNER.pbl_dzialy d on z.za_dz_dzial1_id = d.dz_dzial_id  
LEFT JOIN IBL_OWNER.pbl_zapisy_wydawnictwa zwyd on zwyd.zawy_za_zapis_id = z.za_zapis_id
LEFT JOIN IBL_OWNER.pbl_wydawnictwa w on w.wy_wydawnictwo_id = zwyd.zawy_wy_wydawnictwo_id
WHERE z.za_zapis_id IN ({full_list_of_Prus_records_part_1_str})
'''

# cursor.execute(query_full_list_of_Prus_records_part_1)

df_ora_Prus_records_part_1 = pd.read_sql(query_full_list_of_Prus_records_part_1, con=connection)



query_full_list_of_Prus_records_part_2 = f'''
SELECT z.za_zapis_id, 
z.za_za_zapis_id AS zapis_nadrzedny, 
z.za_type, 
z.za_tytul, 
tw.tw_imie, 
tw.tw_nazwisko,
tw.tw_tworca_id, 
d.dz_nazwa, 
t.am_imie, 
t.am_nazwisko, 
t.am_autor_id,
zr.zr_tytul,
zr.zr_zrodlo_id,
z.za_zrodlo_rok, 
z.za_zrodlo_nr, 
z.za_zrodlo_str,
z.za_seria_wydawnicza,  
z.za_opis_wspoltworcow,
z.za_wydawnictwa,
w.wy_nazwa,
w.wy_wydawnictwo_id,
w.wy_miasto,
z.za_rok_wydania,
z.za_opis_fizyczny_ksiazki,
Z.ZA_ADNOTACJE,
Z.ZA_OPIS_IMPREZY,
Z.ZA_ORGANIZATOR
FROM IBL_OWNER.pbl_zapisy z
LEFT JOIN IBL_OWNER.pbl_zapisy_autorzy a on z.za_zapis_id = a.zaam_za_zapis_id
LEFT JOIN IBL_OWNER.pbl_autorzy t on a.zaam_am_autor_id = t.am_autor_id
left join IBL_OWNER.pbl_zapisy_tworcy ztw on ztw.zatw_za_zapis_id = z.za_zapis_id
left join IBL_OWNER.pbl_zrodla zr on z.za_zr_zrodlo_id = zr.zr_zrodlo_id
left join IBL_OWNER.pbl_tworcy tw on tw.tw_tworca_id = ztw.zatw_tw_tworca_id
LEFT JOIN IBL_OWNER.pbl_dzialy d on z.za_dz_dzial1_id = d.dz_dzial_id  
LEFT JOIN IBL_OWNER.pbl_zapisy_wydawnictwa zwyd on zwyd.zawy_za_zapis_id = z.za_zapis_id
LEFT JOIN IBL_OWNER.pbl_wydawnictwa w on w.wy_wydawnictwo_id = zwyd.zawy_wy_wydawnictwo_id
WHERE z.za_zapis_id IN ({full_list_of_Prus_records_part_2_str})
'''

# cursor.execute(query_full_list_of_Prus_records_part_2)

df_ora_Prus_records_part_2 = pd.read_sql(query_full_list_of_Prus_records_part_2, con=connection)




df_Prus_records_all = pd.concat([df_ora_Prus_records_part_1, df_ora_Prus_records_part_2], ignore_index=True).drop_duplicates() #840 rekordów


#Czyszczenie tabelki: 1) usuniecie wierszy ktore w polu DZ_NAZWA mają -- do ustalenia -- 

df_Prus_records_all_clean = df_Prus_records_all.drop(df_Prus_records_all[df_Prus_records_all['DZ_NAZWA'] == "-- do ustalenia --"].index) #mniej o okolo 60 rekordow

df_Prus_records_all_clean['ZAPIS_NADRZEDNY'] = df_Prus_records_all_clean['ZAPIS_NADRZEDNY'].astype('Int64')



#%% Prus wspomniany w polu adnotacja (zastanowic sie czy to jest potrzebne) + Prus w polu Nazwisko w tabeli PBL_OSOBY_DO_INDEKSU

query_records_with_Prus_in_note = f'''
SELECT z.za_zapis_id, 
z.za_za_zapis_id AS zapis_nadrzedny, 
z.za_type, 
z.za_tytul, 
tw.tw_imie, 
tw.tw_nazwisko,
tw.tw_tworca_id, 
d.dz_nazwa, 
t.am_imie, 
t.am_nazwisko, 
t.am_autor_id,
zr.zr_tytul,
zr.zr_zrodlo_id,
z.za_zrodlo_rok, 
z.za_zrodlo_nr, 
z.za_zrodlo_str,
z.za_seria_wydawnicza,  
z.za_opis_wspoltworcow,
z.za_wydawnictwa,
w.wy_nazwa,
w.wy_wydawnictwo_id,
w.wy_miasto,
z.za_rok_wydania,
z.za_opis_fizyczny_ksiazki,
Z.ZA_ADNOTACJE,
Z.ZA_OPIS_IMPREZY,
Z.ZA_ORGANIZATOR
FROM IBL_OWNER.pbl_zapisy z
LEFT JOIN IBL_OWNER.pbl_zapisy_autorzy a on z.za_zapis_id = a.zaam_za_zapis_id
LEFT JOIN IBL_OWNER.pbl_autorzy t on a.zaam_am_autor_id = t.am_autor_id
left join IBL_OWNER.pbl_zapisy_tworcy ztw on ztw.zatw_za_zapis_id = z.za_zapis_id
left join IBL_OWNER.pbl_zrodla zr on z.za_zr_zrodlo_id = zr.zr_zrodlo_id
left join IBL_OWNER.pbl_tworcy tw on tw.tw_tworca_id = ztw.zatw_tw_tworca_id
LEFT JOIN IBL_OWNER.pbl_dzialy d on z.za_dz_dzial1_id = d.dz_dzial_id  
LEFT JOIN IBL_OWNER.pbl_zapisy_wydawnictwa zwyd on zwyd.zawy_za_zapis_id = z.za_zapis_id
LEFT JOIN IBL_OWNER.pbl_wydawnictwa w on w.wy_wydawnictwo_id = zwyd.zawy_wy_wydawnictwo_id
LEFT JOIN IBL_OWNER.pbl_osoby_do_indeksu oi on oi.odi_za_zapis_id = z.za_zapis_id
WHERE z.za_adnotacje like '%Prus%'
AND oi.odi_nazwisko like 'Prus'
AND oi.odi_imie like 'Bolesław'
'''

# cursor.execute(query_records_with_Prus_in_note )

df_ora_Prus_in_note = pd.read_sql(query_records_with_Prus_in_note, con=connection) #372 rekordów
id_records_attached_Prus_in_note = df_ora_Prus_in_note['ZA_ZAPIS_ID'].tolist()

#%% Prus wspomniany w polu adnotacja (zastanowic sie czy to jest potrzebne) + Prus w polu Nazwisko w tabeli PBL_OSOBY_DO_INDEKSU - rekordy podpiete pod te rekordy

id_records_attached_Prus_in_note_str = ', '.join(map(str, id_records_attached_Prus_in_note))

query_records_attached_Prus_in_note_one_level_below = f'''
SELECT z.za_zapis_id, 
z.za_za_zapis_id AS zapis_nadrzedny, 
z.za_type, 
z.za_tytul, 
tw.tw_imie, 
tw.tw_nazwisko,
tw.tw_tworca_id, 
d.dz_nazwa, 
t.am_imie, 
t.am_nazwisko, 
t.am_autor_id,
zr.zr_tytul,
zr.zr_zrodlo_id,
z.za_zrodlo_rok, 
z.za_zrodlo_nr, 
z.za_zrodlo_str,
z.za_seria_wydawnicza,  
z.za_opis_wspoltworcow,
z.za_wydawnictwa,
w.wy_nazwa,
w.wy_wydawnictwo_id,
w.wy_miasto,
z.za_rok_wydania,
z.za_opis_fizyczny_ksiazki,
Z.ZA_ADNOTACJE,
Z.ZA_OPIS_IMPREZY,
Z.ZA_ORGANIZATOR
FROM IBL_OWNER.pbl_zapisy z
LEFT JOIN IBL_OWNER.pbl_zapisy_autorzy a on z.za_zapis_id = a.zaam_za_zapis_id
LEFT JOIN IBL_OWNER.pbl_autorzy t on a.zaam_am_autor_id = t.am_autor_id
left join IBL_OWNER.pbl_zapisy_tworcy ztw on ztw.zatw_za_zapis_id = z.za_zapis_id
left join IBL_OWNER.pbl_zrodla zr on z.za_zr_zrodlo_id = zr.zr_zrodlo_id
left join IBL_OWNER.pbl_tworcy tw on tw.tw_tworca_id = ztw.zatw_tw_tworca_id
LEFT JOIN IBL_OWNER.pbl_dzialy d on z.za_dz_dzial1_id = d.dz_dzial_id  
LEFT JOIN IBL_OWNER.pbl_zapisy_wydawnictwa zwyd on zwyd.zawy_za_zapis_id = z.za_zapis_id
LEFT JOIN IBL_OWNER.pbl_wydawnictwa w on w.wy_wydawnictwo_id = zwyd.zawy_wy_wydawnictwo_id
WHERE z.za_za_zapis_id IN ({id_records_attached_Prus_in_note_str})
'''


df_ora_Prus_in_note_one_level_below = pd.read_sql(query_records_attached_Prus_in_note_one_level_below, con=connection) #384 rekordów

df_merged_Prus_in_note = pd.concat([df_ora_Prus_in_note, df_ora_Prus_in_note_one_level_below], ignore_index=True).drop_duplicates() #607 rekordów



#%%Połączenie wszystkich trzech DF w jeden (1. DF z rekordami podpietymi pod Lalkę, 2. DF z rekordami podpietymi pod Prusa jako tworce i autora, 3. DF z rekordami w ktorych w osobach do indeksu i adnotacji pojawia sie "Prus") 

 
df_final = pd.concat([df_full_list, df_Prus_records_all_clean, df_merged_Prus_in_note], ignore_index=True).drop_duplicates() #1411 rekordy


#Dodać jeszcze Filmy i Seriale!

#%%


#Przeksztalcanie finalnego DF na json (dostosowany troche do KG)

def row_to_dict(row):
    def clean_val(val):
        if pd.isna(val) or val is None:
            return None
        # Jeśli wartość to float bez części dziesiętnej, zamień na int
        if isinstance(val, float) and val.is_integer():
            return int(val)
        return val

    return {
        "za_zapis_id": clean_val(row['ZA_ZAPIS_ID']),
        "zapis_nadrzedny": clean_val(row['ZAPIS_NADRZEDNY']),
        "za_type": clean_val(row['ZA_TYPE']),
        "za_tytul": clean_val(row['ZA_TYTUL']),
        "tworca": {
            "imie": clean_val(row['TW_IMIE']),
            "nazwisko": clean_val(row['TW_NAZWISKO']),
            "identyfikator": clean_val(row['TW_TWORCA_ID'])
        },
        "autor": {
            "imie": clean_val(row['AM_IMIE']),
            "nazwisko": clean_val(row['AM_NAZWISKO']),
            "identyfikator": clean_val(row['AM_AUTOR_ID'])
        },
        "dzial": clean_val(row['DZ_NAZWA']),
        "zrodlo": {
            "tytul": clean_val(row['ZR_TYTUL']),
            "rok": int(clean_val(row['ZA_ZRODLO_ROK'])) if clean_val(row['ZA_ZRODLO_ROK']) is not None else None,
            "numer": clean_val(row['ZA_ZRODLO_NR']),
            "strony": clean_val(row['ZA_ZRODLO_STR']),
            "seria_wydawnicza": clean_val(row['ZA_SERIA_WYDAWNICZA']),
            "miejsce_wydania": clean_val(row['WY_MIASTO']),
            "wydawnictwo": clean_val(row['ZA_WYDAWNICTWA']),
            "rok_wydania": int(clean_val(row['ZA_ROK_WYDANIA'])) if clean_val(row['ZA_ROK_WYDANIA']) is not None else None,
            "opis_fizyczny": clean_val(row['ZA_OPIS_FIZYCZNY_KSIAZKI']),
        },
        "opis_wspoltworcow": clean_val(row['ZA_OPIS_WSPOLTWORCOW']),
        "adnotacje": clean_val(row['ZA_ADNOTACJE']),
        "opis_imprezy": clean_val(row['ZA_OPIS_IMPREZY']),
        "organizator": clean_val(row['ZA_ORGANIZATOR'])
    }

# Załaduj cały DataFrame (zakładam, że masz df)

json_list = [row_to_dict(row) for _, row in df_full_list.iterrows()]


# json_list = [row_to_dict(row) for _, row in df_final.iterrows()]






# Zapis do pliku JSON (lista obiektów)
with open(f"data\Prus_for_KG.json", "w", encoding="utf-8") as f:
    json.dump(json_list, f, ensure_ascii=False, indent=2)




#Co jeszcze przydaloby sie z PBL o Lalce: 

# Film z podpiętymi rekordami
# Serial z podpiętymi rekordami
# Audycje radiowe (jest kilka), ale bez jakichś podpiętych rekordów
# Lalka w teatrze.


#WYJAC FILM I SERIAL - teraz ich nie ma! 
#Brakuje ID przy kazdym autorze 
#Brakuje ID przy kazdym źródle
#Generalnie wszystko co ma ID w bazie powinno miec ID w JSONie




#W kolejnych krokach dodac informacje z Encyklopedii Teatru Polskiego, Filmu Polskiego i Biblioteki Narodowej 
#Powiazac byty z identyfikatorami zewnętrznymi? (Wikidata)
#Zrobic z tego Knowlegde Graph, w którego centrum bedzie Lalka
#Zrobic trójki (template)



#Do poprawy zapytania SQL - nie znajduje nazw wydawnictw i ID




#%% Testowe od NW: 
    



books_lst = [
  {
    "title": "W pustyni i w puszczy",
    "author": {'id': 1,
               'name': "Henryk Sienkiewicz"},
    "place": "Warszawa"
  },
  {
    "title": "Quo Vadis",
    "author": {'id': 1,
               'name': "Henryk Sienkiewicz"},
    "place": "Warszawa"
  },
  {
    "title": "Lalka",
    "author": {'id': 2,
               'name': "Bolesław Prus"},
    "place": "Warszawa"
  }
]


# Utwórz graf
G = nx.Graph()

for book in books_lst:
    title = book["title"]
    author = book["author"]
    place = book["place"]
    
    # Dodaj node dla książki
    G.add_node(title, type="book", label=title)
    
    # Dodaj/upewnij się, że autor i miejsce istnieją
    G.add_node(place, type="place", label=place)
    G.add_node(author['id'], type="author", name=author['name'], label=author['name'])

    # Powiąż książkę z autorem i miejscem
    G.add_edge(title, author['id'], relation="written_by")
    G.add_edge(title, place, relation="published_in")



# (Opcjonalnie) rysowanie grafu
pos = nx.spring_layout(G)
colors = [ 
    'red' if G.nodes[n]['type'] == 'book' 
    else 'green' if G.nodes[n]['type'] == 'author' 
    else 'blue' 
    for n in G.nodes
]
nx.draw(G, pos, with_labels=False, node_color=colors) # with_labels=False nie wyświetlamy domyślnych labeli
# tworzymy zestaw labeli dal krawędzi
edge_labels = nx.get_edge_attributes(G, 'relation')
nx.draw_networkx_edge_labels(G, pos, edge_labels=edge_labels)
# tworzymy zestaw labeli dal węzłów
node_labels = {node: G.nodes[node]['label'] for node in G.nodes}
nx.draw_networkx_labels(G, pos, labels=node_labels)
plt.show()




# eksport html

# Tworzymy obiekt PyVis i konwertujemy graf
net = Network(notebook=False, height="600px", width="100%", bgcolor="#ffffff", font_color="black")
net.from_nx(G)

# Dodajemy etykiety z atrybutów (np. name)
for node_id in net.nodes:
    data = G.nodes[node_id['id']]
    node_id['label'] = data.get('name', str(node_id['id']))
    node_id['title'] = str(data)

# Zapis do pliku HTML
net.show("graph.html", notebook=False)  # otworzy się w przeglądarc


#https://pyvis.readthedocs.io/en/latest/tutorial.html#example-visualizing-a-game-of-thrones-character-network










