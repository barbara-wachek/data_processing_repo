#%%import
import cx_Oracle
import pandas as pd
from datetime import datetime
import matplotlib.pyplot as plt
from wordcloud import WordCloud
import matplotlib.colors 


#%% Wordcloud z polskich twórczyń PBL, które mają najwięcej zapisów (przede wszystkim Polki, bo nie mamy w bazie rozróżnienia na płeć, więc zdefiniowałam to jako imię kończące się na 'a' - to nie obejmie zagranicznych twórczyń)

#%% Połączenie z bazą Oracle (dostęp w pliku SQL_dostep)

cx_Oracle.init_oracle_client(lib_dir=r"C:\Users\PBL_Basia\Desktop\SQL\sqldeveloper\instantclient_19_6")
dsn_tns = cx_Oracle.makedsn('XXXXX', 'XX', service_name='XXXX')
connection = cx_Oracle.connect(user='XXXX', password='XXXX', dsn=dsn_tns, encoding='windows-1250')


#%% Zapytanie

query= '''
select t.tw_nazwisko, t.tw_imie, count(*)
from IBL_OWNER.pbl_tworcy t
join IBL_OWNER.pbl_zapisy_tworcy zt on zt.zatw_tw_tworca_id = t.tw_tworca_id
join IBL_OWNER.pbl_zapisy z on zt.zatw_za_zapis_id = z.za_zapis_id
where t.tw_imie like '%a'
group by t.tw_nazwisko, t.tw_imie
order by count(*) desc
'''


#%% Przetwarzanie danych z uzyskanej tabeli

df_ora = pd.read_sql(query, con=connection)
df_ora_selected = df_ora[:200]

df_ora_selected['NAME'] = df_ora_selected['TW_IMIE'] + " " + df_ora_selected['TW_NAZWISKO'] 
df_ora_selected = df_ora_selected.drop(columns=['TW_IMIE','TW_NAZWISKO' ])
df_ora_selected = df_ora_selected.rename(columns = {'COUNT(*)':'SUM_OF_RECORDS'})

#Usun błędy (nie-polskie twórczynie, męzczyzn)
#df_ora_selected.loc[df_ora_selected['NAME'].str.contains('Rilke')] #Sprwadz indeks wystę○powania Rilke
df_ora_selected = df_ora_selected.drop(19) #Usun wiersz z Rilke

#df_ora_selected.loc[df_ora_selected['NAME'].str.contains('Anna Akhmatova')]
df_ora_selected = df_ora_selected.drop(18)

#df_ora_selected.loc[df_ora_selected['NAME'].str.contains('Agatha Christie')]
df_ora_selected = df_ora_selected.drop(17)

#df_ora_selected.loc[df_ora_selected['NAME'].str.contains('Nora Roberts')]
df_ora_selected = df_ora_selected.drop(34)

#df_ora_selected.loc[df_ora_selected['NAME'].str.contains('Tove Marika Jansson')]
df_ora_selected = df_ora_selected.drop(63)

#df_ora_selected.loc[df_ora_selected['NAME'].str.contains('Kazimierz Przerwa Tetmajer')]
df_ora_selected = df_ora_selected.drop(69)

#df_ora_selected.loc[df_ora_selected['NAME'].str.contains('Jehuda Amichaj')]
df_ora_selected = df_ora_selected.drop(77)

#df_ora_selected.loc[df_ora_selected['NAME'].str.contains('Barbara Cartland')]
df_ora_selected = df_ora_selected.drop(78)

#df_ora_selected.loc[df_ora_selected['NAME'].str.contains('Marina Cvetaeva')]
df_ora_selected = df_ora_selected.drop(80)

#df_ora_selected.loc[df_ora_selected['NAME'].str.contains('Mihlja Luchak')]
df_ora_selected = df_ora_selected.drop(85)

#df_ora_selected.loc[df_ora_selected['NAME'].str.contains('Erich Maria Remarque')]
df_ora_selected = df_ora_selected.drop(93)

#df_ora_selected.loc[df_ora_selected['NAME'].str.contains('Virginia Woolf')]
df_ora_selected = df_ora_selected.drop(94)

#df_ora_selected.loc[df_ora_selected['NAME'].str.contains('Erna Rosenstein')]
df_ora_selected = df_ora_selected.drop(97)

#df_ora_selected.loc[df_ora_selected['NAME'].str.contains('Ewa Stadtmueller')]
df_ora_selected = df_ora_selected.drop(100)

#df_ora_selected.loc[df_ora_selected['NAME'].str.contains('Mircea Eliade')]
df_ora_selected = df_ora_selected.drop(116)

#df_ora_selected.loc[df_ora_selected['NAME'].str.contains('Sylvia Plath')]
df_ora_selected = df_ora_selected.drop(146)

#df_ora_selected.loc[df_ora_selected['NAME'].str.contains('Francesca Simon')]
df_ora_selected = df_ora_selected.drop(149)

#df_ora_selected.loc[df_ora_selected['NAME'].str.contains('Christa Wolf')]
df_ora_selected = df_ora_selected.drop(192)

#df_ora_selected.loc[df_ora_selected['NAME'].str.contains('Natalija Gorbanevskaja')]
df_ora_selected = df_ora_selected.drop(194)

#df_ora_selected.loc[df_ora_selected['NAME'].str.contains('Nadzeja Artymovihch')]
df_ora_selected = df_ora_selected.drop(161)



cols = df_ora_selected.columns.tolist()
cols = cols[-1:] + cols[:-1]
df_ora_selected = df_ora_selected[cols]


women_dictionary = df_ora_selected.to_dict('split')
new_dict = dict()

for x in women_dictionary['data']:
    new_dict[x[0]] = x[1]
        


#%% Wordcloud

custom_cmap_PBL = matplotlib.colors.ListedColormap(['#50504f', '#5f6062', '#87745e', '#968a7e', '#f3b65a'])

wordcloud = WordCloud(colormap=custom_cmap_PBL, max_font_size=800, width=1080, height=1080, prefer_horizontal=1, min_font_size=6, background_color='white', collocations=False).generate_from_frequencies(new_dict)

plt.figure(figsize=[100,100])
plt.imshow(wordcloud, interpolation="bilinear")
plt.axis("off")

plt.savefig(f"C:\\Users\\PBL_Basia\\Desktop\\wordcloud_{datetime.today().date()}.png", format="png") 






