import cx_Oracle
import pandas as pd
import re
import matplotlib.pyplot as plt
from wordcloud import WordCloud
from datetime import datetime
import SQL_access as sq


#%% Połączenie z bazą Oracle (dostęp w pliku SQL_dostep)

cx_Oracle.init_oracle_client(lib_dir=sq.LIB_DIR)
dsn_tns = cx_Oracle.makedsn(sq.HOSTNAME, sq.PORT, service_name=sq.SERVICE_NAME)
connection = cx_Oracle.connect(user=sq.USER, password=sq.PASSWORD, dsn=dsn_tns, encoding=sq.ENCODING)

cursor = connection.cursor()

#%% Przetwarzanie rekordów przyporzadkowanych pod utwór Lalka (id = 109715)

query= '''
SELECT z.za_zapis_id
FROM IBL_OWNER.pbl_zapisy z
WHERE z.za_za_zapis_id LIKE '109715'
'''

cursor.execute(query)

result = cursor.fetchall()  # to będzie lista tupli: [(id1,), (id2,), (id3,)]

id_list = [row[0] for row in result] # 92 rekordy pod Lalką

#%% Sprawdzenie, czy którys z zapisów wydobytów do id_list ma jakies zapisy - czyli szukamy zagniezdzonych zapisow podpiętych pod który z tych ID

id_list_str = ', '.join(map(str, id_list))

query_2 = f'''
SELECT z.za_zapis_id
FROM IBL_OWNER.pbl_zapisy z
WHERE z.za_za_zapis_id IN ({id_list_str})
'''

cursor.execute(query_2)

result_2 = cursor.fetchall()

id_list_extra = [row[0] for row in result_2]



#%% Teraz chce z dwóch list otrzymac pełne rekordy bibliograficzne 

full_list_of_ids = id_list + id_list_extra

full_list_of_ids_str = ', '.join(map(str, full_list_of_ids))


query_full_list = f'''
SELECT z.za_zapis_id, 
z.za_za_zapis_id AS zapis_nadrzedny, 
z.za_type, 
z.za_tytul, 
tw.tw_imie, 
tw.tw_nazwisko, 
d.dz_nazwa, 
t.am_imie, 
t.am_nazwisko, 
zr.zr_tytul, 
z.za_zrodlo_rok, 
z.za_zrodlo_nr, 
z.za_zrodlo_str,
z.za_seria_wydawnicza, 
z.za_miejsce_wydania, 
z.za_opis_wspoltworcow,
z.za_wydawnictwa,
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
WHERE z.za_zapis_id IN ({full_list_of_ids_str})
'''

cursor.execute(query_2)

df_ora = pd.read_sql(query_full_list, con=connection)
















df_ora = pd.read_sql(query, con=connection)
df_ora_only_newspapers = df_ora.dropna(subset=['ZR_TYTUL']).drop_duplicates() 


len(df_ora_only_newspapers['ZA_ZAPIS_ID'].unique()) #12 541 wszystkich zapisów z czasopism
len(df_ora_only_newspapers["ZR_TYTUL"].unique())   #94 unikatowych tytułów czasopism dla powyższych zapisów

only_titles = df_ora_only_newspapers.drop(columns=['ZA_ZAPIS_ID']) #stworzenie nowego dataframe bez kolumny ZA_ZAPIS_ID
only_titles_without_x = only_titles[~only_titles['ZR_TYTUL'].str.contains('^x$', flags=re.I, regex=True)] #usunięcie czasopisma o tytule 'x' - dla odwołań pojawia się taki wyjatek
len(only_titles_without_x['ZR_TYTUL'].unique()) #93 tytuły czasopism (bez x) / powinno być o 1 mniej niż wynik z 31 linii kodu
print(only_titles_without_x['ZR_TYTUL'].unique()) #do podejrzenia jakie konkretnie mamy unikatowe tytuły czasopism dla wskazanego okresu 

#%%Skracanie nazw czasopism, żeby sie lepiej prezentowały w wordcloud / np. usunięcie częsci po = i podtytułu jesli to mozliwe

short_titles = only_titles_without_x.replace(['PaperMint', 'Wyspa', 'Foyer', 'Bibliotekarz',
 'Folia Historiae Artium [Seria Nowa]', 'Zeszyty Komiksowe',
 'Wiedza i Życie', 'Dedal', 'Przegląd Polski = Polish Review',
 'Almanach Prowincjonalny', 'Studia Rossica', 'Język Polski w Liceum',
 'Dworzanin', '(Fo:pa)', 'Rubikon', 'Uniwersytet Kulturalny', 'Rerum Artis',
 'Wakat', 'Z Dziejów Polskiej Radiofonii', 'Slavia Orientalis',
 'Annales Academiae Paedagogicae Cracoviensis. Studia Russologica',
 'Annales Universitatis Paedagogicae Cracoviensis. Studia Russologica',
 'Studia nad Polszczyzną Kresową', 'Ślad',
 'Annales Universitatis Paedagogicae Cracoviensis. Studia ad Didacticam Litterarum Polonarum et Linguae Polonae Pertinentia',
 'Annual Report', 'Midrasz',
 'Prace Naukowe Uniwersytetu Śląskiego. Studia Bibliologiczne',
 'Pomosty [Wrocław]', 'Kurier Wileński', 'Wiadomości [ZAiKS]', 'Bluszcz',
 'Nowe Książki', 'Polish Culture', 'Zeszyty Literackie', 'Cracovia Leopolis',
 'Studia Historyczne',
 'Studia Rusycystyczne Uniwersytetu Jana Kochanowskiego = Russian Studies University of the Jan Kochanowski',
 'Studia Rusycystyczne Uniwersytetu Humanistyczno-Przyrodniczego Jana Kochanowskiego = Russian Studies of the Jan Kochanowski University of Humanities and Sciences',
 'Ślad [Rzeszów]', 'Jesteśmy [Bielsko-Biała]',
 'Zeszyty Naukowe Uniwersytetu Rzeszowskiego. Seria Filologiczna. Studia Germanica Resoviensia',
 'Dziennik Polski [Londyn]', 'Aiglos', 'Latarnia Morska',
 "Warszawskie Zeszyty Ukrainoznawcze = Varshavs'kih Ukrajinoznavchih Zapiski",
 'Romano Atmo' 'Autograf' 'Dialog [Warszawa]',
 'Monitor Uniwersytetu Warszawskiego', 'Nasza Rota',
 'Pamiętnik Literacki [Londyn]', 'Głos Ludu',
 'Nadwiślański Rocznik Historyczno-Społeczny', 'Pociąg 76', 'Neurokultura',
 'Panorama Wielkopolskiej Kultury', 'Skafander',
 'Studies of the Department of African Languages and Cultures',
 'Zeitschrift des Verbandes Polnischer Germanisten',
 'Studia Rusycystyczne Akademii Świętokrzyskiej = Akademia Świętokrzyska Russian Studies',
 'Gryfita',
 'Seminaria Naukowe Wrocławskiego Towarzystwa Naukowego. Seria A i B',
 'Kraków', 'Kultura Koszalińska', 'Patos', 'Convivium', 'Rrom p-o Drom',
 'Zeszyty Naukowe Uniwersytetu Rzeszowskiego. Seria Filologiczna. Studia Anglica Resoviensia',
 'Kwartalnik Literacki',
 'Studia Kieleckie. Seria Historyczna = Kielce Studies. Series of History',
 'Czytanie Literatury', 'Pomosty [Piotrków]', 'Klematis', 'Arte', 'Zwrot',
 'Lampa', 'Magazyn Wileński', 'Biuletyn Informacyjny Biblioteki Narodowej',
 'Lublin, Kultura i Społeczeństwo', 'Sprawozdanie Biblioteki Narodowej',
 'Dodatek Literacki', 'Opowieści', 'Polonica [Kraków]', 'Akcent',
 'Studia Regionalne', 'Philosophia', 'Nestor', 'Peitho', 'Schulz / Forum',
 'Zeszyty Naukowe Uniwersytetu Rzeszowskiego. Seria Filologiczna. Historia Literatury',
 'Ecclesia', 'Kajet Artystyczno Kulturalny'], ['PaperMint', 'Wyspa', 'Foyer', 'Bibliotekarz',
 'Folia Historiae Artium', 'Zeszyty Komiksowe',
 'Wiedza i Życie', 'Dedal', 'Przegląd Polski',
 'Almanach Prowincjonalny', 'Studia Rossica', 'Język Polski w Liceum',
 'Dworzanin', '(Fo:pa)', 'Rubikon', 'Uniwersytet Kulturalny', 'Rerum Artis',
 'Wakat', 'Z Dziejów Polskiej Radiofonii', 'Slavia Orientalis',
 'Annales Academiae Paedagogicae Cracoviensis. Studia Russologica',
 'Annales Universitatis Paedagogicae Cracoviensis. Studia Russologica',
 'Studia nad Polszczyzną Kresową', 'Ślad',
 'Annales Universitatis Paedagogicae Cracoviensis. Studia ad Didacticam Litterarum Polonarum et Linguae Polonae Pertinentia',
 'Annual Report', 'Midrasz',
 'Prace Naukowe Uniwersytetu Śląskiego. Studia Bibliologiczne',
 'Pomosty [Wrocław]', 'Kurier Wileński', 'Wiadomości [ZAiKS]', 'Bluszcz',
 'Nowe Książki', 'Polish Culture', 'Zeszyty Literackie', 'Cracovia Leopolis',
 'Studia Historyczne',
 'Studia Rusycystyczne Uniwersytetu Jana Kochanowskiego',
 'Studia Rusycystyczne Uniwersytetu Humanistyczno-Przyrodniczego Jana Kochanowskiego',
 'Ślad [Rzeszów]', 'Jesteśmy [Bielsko-Biała]',
 'Zeszyty Naukowe Uniwersytetu Rzeszowskiego. Seria Filologiczna. Studia Germanica Resoviensia',
 'Dziennik Polski [Londyn]', 'Aiglos', 'Latarnia Morska',
 "Warszawskie Zeszyty Ukrainoznawcze",
 'Romano Atmo' 'Autograf' 'Dialog [Warszawa]',
 'Monitor Uniwersytetu Warszawskiego', 'Nasza Rota',
 'Pamiętnik Literacki [Londyn]', 'Głos Ludu',
 'Nadwiślański Rocznik Historyczno-Społeczny', 'Pociąg 76', 'Neurokultura',
 'Panorama Wielkopolskiej Kultury', 'Skafander',
 'Studies of the Department of African Languages and Cultures',
 'Zeitschrift des Verbandes Polnischer Germanisten',
 'Studia Rusycystyczne Akademii Świętokrzyskiej',
 'Gryfita',
 'Seminaria Naukowe Wrocławskiego Towarzystwa Naukowego. Seria A i B',
 'Kraków', 'Kultura Koszalińska', 'Patos', 'Convivium', 'Rrom p-o Drom',
 'Zeszyty Naukowe Uniwersytetu Rzeszowskiego. Seria Filologiczna. Studia Anglica Resoviensia',
 'Kwartalnik Literacki',
 'Studia Kieleckie. Seria Historyczna',
 'Czytanie Literatury', 'Pomosty [Piotrków]', 'Klematis', 'Arte', 'Zwrot',
 'Lampa', 'Magazyn Wileński', 'Biuletyn Informacyjny Biblioteki Narodowej',
 'Lublin, Kultura i Społeczeństwo', 'Sprawozdanie Biblioteki Narodowej',
 'Dodatek Literacki', 'Opowieści', 'Polonica [Kraków]', 'Akcent',
 'Studia Regionalne', 'Philosophia', 'Nestor', 'Peitho', 'Schulz / Forum',
 'Zeszyty Naukowe Uniwersytetu Rzeszowskiego. Seria Filologiczna. Historia Literatury',
 'Ecclesia', 'Kajet Artystyczno Kulturalny'])


short_titles['ZR_TYTUL'].unique()  #Żeby podpatrzyć jak wyglada efekt po skroceniu nazw czasopism - czy nie ma błędów

#%% Utworzenie słownika na potrzeby wordcloud (klucz to nazwa czasopisma, wartosc to ilosc wystapien)

short_titles_slownik = dict(short_titles['ZR_TYTUL'].value_counts())
short_titles_slownik

#%% Wordcloud
# dokumentacja: https://amueller.github.io/word_cloud/generated/wordcloud.WordCloud.html

wordcloud = WordCloud(colormap='copper', max_font_size=100, width = 1200, height = 900, prefer_horizontal=1, min_font_size=6, background_color='white', collocations=False).generate_from_frequencies(short_titles_slownik)

plt.figure(figsize=[10,10])
plt.imshow(wordcloud, interpolation="bilinear")
plt.axis("off")

plt.savefig(f"C:\\Users\\PBL_Basia\\Desktop\\wordcloud_{datetime.today().date()}.png", format="png") 
























