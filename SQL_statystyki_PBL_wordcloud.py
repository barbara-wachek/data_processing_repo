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

#%% Zapytanie

query= '''
select z.za_zapis_id, zr.zr_tytul  
from IBL_OWNER.pbl_zapisy z
left join IBL_OWNER.pbl_zrodla zr on z.za_zr_zrodlo_id = zr.zr_zrodlo_id
where z.za_uzytk_wpis_data between TO_DATE('2022-01-01','YYYY-MM-DD') and TO_DATE('2022-06-30','YYYY-MM-DD')
order by z.za_zapis_id
'''

#%% Przetwarzanie danych

df_ora = pd.read_sql(query, con=connection)
df_ora_only_newspapers = df_ora.dropna(subset=['ZR_TYTUL']).drop_duplicates() 


len(df_ora_only_newspapers['ZA_ZAPIS_ID'].unique()) #14 199 zapisó1w z czasopism
len(df_ora_only_newspapers["ZR_TYTUL"].unique())   #84 unikatowych tytułów czasopism

only_titles = df_ora_only_newspapers.drop(columns=['ZA_ZAPIS_ID'])
only_titles_without_x = only_titles[~only_titles['ZR_TYTUL'].str.contains('^x$', flags=re.I, regex=True)]
len(only_titles_without_x['ZR_TYTUL'].unique()) #83 tytuły czasopism (bez x)


#%%Skracanie nazw czasopism, żeby sie lepiej prezentowały w wordcloud

short_titles = only_titles_without_x.replace(['Almanach Sejneński = Seinu Almanachas','Dziennik Polski [Londyn]', 'Przegląd Polski = Polish Review', 'Tydzień Polski [Londyn, 2001-2015]', 'Ślad [Rzeszów]', 'Pamiętnik Literacki [Londyn]', 'Dziennik Polski i Dziennik Żołnierza = The Polish Daily and Soldiers Daily', 'Słowo Żydowskie = Dos Jidisze Wort', 'Ekspresje = Expressions', "Ukraiński Zaułek Literacki = Ukrajins'kijj Lihteraturnijj Provulok", 'Dialog [Warszawa]' ], ['Almanach Sejneński','Dziennik Polski', 'Przegląd Polski', 'Tydzień Polski', 'Ślad', 'Pamiętnik Literacki', 'Dziennik Polski i Dziennik Żołnierza', 'Słowo Żydowskie', 'Ekspresje', "Ukraiński Zaułek Literacki", 'Dialog'])


short_titles['ZR_TYTUL'].unique()  #Żeby podpatrzyć jak wyglada efekt po skroceniu nazw czasopism - czy nie ma błędów

#%% Utworzenie słownika na potrzeby wordcloud (klucz to nazwa czasopisma, wartosc to ilosc wystapien)

short_titles_slownik = dict(short_titles['ZR_TYTUL'].value_counts())
short_titles_slownik

#%% Wordcloud

wordcloud = WordCloud(colormap='copper', max_font_size=120, width = 1200, height = 900, prefer_horizontal=1, min_font_size=18, background_color='white', collocations=False).generate_from_frequencies(short_titles_slownik)

plt.figure(figsize=[10,10])
plt.imshow(wordcloud, interpolation="bilinear")
plt.axis("off")

plt.savefig(f"C:\\Users\\PBL_Basia\\Desktop\\wordcloud_{datetime.today().date()}.png", format="png") 
























