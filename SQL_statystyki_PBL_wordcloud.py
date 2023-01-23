import cx_Oracle
import pandas as pd
import re
import matplotlib.pyplot as plt
from wordcloud import WordCloud
from datetime import datetime

#%% Połączenie z bazą Oracle

cx_Oracle.init_oracle_client(lib_dir=r"C:\Users\PBL_Basia\Desktop\SQL\sqldeveloper\instantclient_19_6")
dsn_tns = cx_Oracle.makedsn('pbl.ibl.poznan.pl', '1521', service_name='xe')
connection = cx_Oracle.connect(user='IBL_SELECT', password='CR333444', dsn=dsn_tns, encoding='windows-1250')

#%% Zapytanie (podmieniać daty na odpowiednie)

query= '''
select z.za_zapis_id, zr.zr_tytul  
from IBL_OWNER.pbl_zapisy z
left join IBL_OWNER.pbl_zrodla zr on z.za_zr_zrodlo_id = zr.zr_zrodlo_id
where z.za_uzytk_wpis_data between TO_DATE('2022-07-01','YYYY-MM-DD') and TO_DATE('2022-12-31','YYYY-MM-DD')
order by z.za_zapis_id
'''

#%% Przetwarzanie danych

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
























