#%%import
import cx_Oracle
import pandas as pd
import regex as re
from datetime import datetime
import matplotlib.pyplot as plt
from wordcloud import WordCloud

import matplotlib.colors 
import imageio


#%% Połączenie z bazą Oracle

cx_Oracle.init_oracle_client(lib_dir=r"C:\Users\PBL_Basia\Desktop\SQL\sqldeveloper\instantclient_19_6")
dsn_tns = cx_Oracle.makedsn('pbl.ibl.poznan.pl', '1521', service_name='xe')
connection = cx_Oracle.connect(user='IBL_SELECT', password='CR333444', dsn=dsn_tns, encoding='windows-1250')

#%% Zapytanie

query= '''
select t.tw_nazwisko, t.tw_imie, z.za_zapis_id
from IBL_OWNER.pbl_tworcy t
join IBL_OWNER.pbl_zapisy_tworcy zt on zt.zatw_tw_tworca_id = t.tw_tworca_id
join IBL_OWNER.pbl_zapisy z on zt.zatw_za_zapis_id = z.za_zapis_id
where t.tw_imie like '%święt%'
or t.tw_nazwisko like '%święt%'
or t.tw_nazw_wlasciwe like '%święt%'
or t.tw_pseudonimy like '%święt%'
or t.tw_nazwisko like 'Wojtyła' and t.tw_imie like 'Karol'
order by t.tw_nazwisko, t.tw_imie asc
'''


#%% Przetwarzanie danych z uzyskanej tabeli

df_ora = pd.read_sql(query, con=connection)
df_ora["NAME"] = df_ora['TW_NAZWISKO'].astype(str)+ " " + df_ora["TW_IMIE"]
df_ora = df_ora.drop(columns=['TW_NAZWISKO', 'TW_IMIE', 'ZA_ZAPIS_ID'])

#df_ora.shape #4109 zapisów, w których swieci wystepuja w kategorii tworcy (nie obejmuje np. wystapien w adnotacji itp. oraz wszystkich w kategorii autor)
len(df_ora["NAME"].unique()) #138 unikatowych swietych (1 duplikat zlokalizowany: Piotr Damiani)

df_ora_without_duplicate_author = df_ora[~df_ora['NAME'].str.contains('^Damiani.*', flags=re.I, regex=True)]
df_ora_without_duplicate_author_and_not_saint = df_ora_without_duplicate_author[~df_ora_without_duplicate_author['NAME'].str.contains('Maria Weronika od Najświętszego Oblicza siostra', flags=re.I, regex=True)]

len(df_ora_without_duplicate_author["NAME"].unique()) #137
len(df_ora_without_duplicate_author_and_not_saint['NAME'].unique()) #136


#all_saints = list(df_ora_without_duplicate_author_and_not_saint["NAME"].unique())

df_ora_new_forms_of_saints = df_ora_without_duplicate_author_and_not_saint.replace(['Aelred z Rievaulx święty *', 'Agobard święty *', 'Albert Wielki święty *', 'Alfons Maria de Liguori święty', 'Alojzy Gonzaga święty *','Ambroży święty *','Andrzej z Krety święty *','Antoni Pustelnik święty *','Antoni z Padwy święty *','Anzelm z Canterbury święty *','Atanazy Wielki święty *','Augustyn święty *','Bazyli Wielki święty *','Beda Czcigodny święty *','Benedykt z Nursji święty *','Bernadetta Soubirous święta *','Bernard z Clairvaux święty *','Bernardyn ze Sieny święty *','Bonawentura z Bagnoregio święty *', 'Brunon z Kwerfurtu święty *','Brygida święta *','Cezary z Arles święty *','Chromacjusz z Akwilei święty *','Cyprian z Tulonu święty *','Cyprian święty *','Cyryl Aleksandryjski święty *','Cyryl Jerozolimski święty *','Cyryl Turowski święty .','Cyryl święty *','Cyryl święty, Metody święty *','Daniel Maria od Najświętszego Serca Pana Jezusa *','Dymitr Rostowski święty .','Efrem Syryjczyk święty *','Elżbieta od Trójcy Przenajświętszej *','Eucheriusz z Lyonu święty *','Eutymiusz Tyrnowski święty *','Faustyna Kowalska święta *','Focjusz święty *','Franciszek Ksawery święty *','Franciszek Salezy święty *','Franciszek z Asyżu święty *','Fulbert z Chartres święty *','Gertruda Wielka święta *','Grzegorz Cudotwórca święty *','Grzegorz Palamas święty *','Grzegorz Wielki święty *','Grzegorz z Elwiry święty *','Grzegorz z Nareku święty *','Grzegorz z Nazjanzu święty *','Grzegorz z Nyssy święty *','Grzegorz z Tours święty *','Hieronim ze Strydonu święty *','Hilary z  Arles święty *','Hilary z Poitiers święty *','Hildegarda z Bingen święta *','Hipolit święty *','Honorat z Marsylii święty *','Hormizdas święty *','Ignacy Antiocheński święty *','Ignacy Loyola święty *','Ireneusz z Lyonu święty *','Ivan Rylski święty *','Izaak Syryjczyk święty *','Izydor z Peluzjum święty *','Izydor z Sewilli święty *','Jakub z Sarug święty *','Jan Brebeuf święty *','Jan Chryzostom święty *','Jan Kasjan święty *','Jan Klimak święty *','Jan od Krzyża święty *','Jan z Damaszku święty *','Jan z Kronsztadu święty *','Justyn święty *','Józef z Kupertynu święty .','Józefina Bakhita święta *','Katarzyna z Genui święta *','Katarzyna ze Sieny święta *','Klara święta *','Klaudiusz la Colombiere święty *','Klemens Aleksandryjski święty *','Klemens I święty *','Klemens z Ochrydy święty *','Kolumba Starszy święty *','Kolumban Młodszy święty *','Leander z Sewilli święty *','Leon I Wielki święty *','Ludwik Maria Grignion de Montfort święty *','Ludwik z Granady święty *','Makary Wielki święty *','Maksym Wyznawca święty *','Maksym z Turynu święty *','Mamcarz Irena','Marceli z Ancyry święty *','Marcin z Bragi święty *','Marek Eremita święty *','Maria Ena od Najświętszego Sakramentu *','Maria od Jezusa Ukrzyżowanego święta *','Małgorzata Maria Alacoque święta *','Metody święty *','Mikołaj Kabasilas święty *','Mikołaj Serbski święty *','Nerses Sznorhali święty *','Optat z Milewe święty *','Pacjan z Barcelony święty *','Paisij Wełyczkowśkyj święty *','Patryk święty *','Paulin z Noli święty *','Pio z Pietrelciny święty *','Piotr Chryzolog święty *','Piotr Damiani święty *','Piotr z Alkantary święty *','Possydiusz z Kalamy święty *','Robert Bellarmin święty *','Romanos Melodos święty *','Rupert z Deutz święty *','Serafin z Sarowa święty *','Sofroniusz Jerozolimski święty *','Stein Edith','Sydoniusz święty *','Symeon święty *','Szenute z Atripe święty','Teresa od Dzieciątka Jezus święta *','Teresa od Jezusa z Los Andes święta *', 'Teresa z Avili święta *','Teresa z Kalkuty święta *','Tomasz Morus święty *','Tomasz a Kempis święty *','Tomasz z Akwinu święty *','Tomasz z Cantimpre święty *','Wawrzyniec z Brindisi święty *','Wenancjusz święty *','Wiktor z Wity święty *','Wincenty z Lerynu święty *','Wojtyła Karol','Zenon z Werony święty *'], ['św. Aelred z Rievaulx', 'św. Agobard', 'św. Albert Wielki', 'św. Alfons Maria de Liguori', 'św. Alojzy Gonzaga','św. Ambroży','św. Andrzej','św. Antoni Pustelnik','św. Antoni z Padwy','św. Anzelm z Canterbury','św. Atanazy Wielki','św. Augustyn','św. Bazyli Wielki','św. Beda Czcigodny','św. Benedykt z Nursji','św. Bernadetta Soubirous','św. Bernard z Clairvaux','św. Bernardyn ze Sieny','św. Bonawentura z Bagnoregio', 'św. Brunon z Kwerfurtu','św. Brygida','św. Cezary z Arles','św. Chromacjusz z Akwilei','św. Cyprian z Tulonu','św. Cyprian','św. Cyryl Aleksandryjski','św. Cyryl Jerozolimski','św. Cyryl Turowski','św. Cyryl','św. Cyryl, św. Metody','św. Daniel Maria od Najświętszego Serca Pana Jezusa','św. Dymitr Rostowski','św. Efrem Syryjczyk','św. Elżbieta od Trójcy Przenajświętszej','św. Eucheriusz z Lyonu','św. Eutymiusz Tyrnowski','św. Faustyna Kowalska','św. Focjusz','św. Franciszek Ksawery','św. Franciszek Salezy','św. Franciszek z Asyżu','św. Fulbert z Chartres','św. Gertruda Wielka','św. Grzegorz Cudotwórca','św. Grzegorz Palamas','św. Grzegorz Wielki','św. Grzegorz z Elwiry','św. Grzegorz z Nareku','św. Grzegorz z Nazjanzu','św. Grzegorz z Nyssy','św. Grzegorz z Tours','św. Hieronim ze Strydonu','św. Hilary z  Arles','św. Hilary z Poitiers','św. Hildegarda z Bingen','św. Hipolit','św. Honorat z Marsylii','św. Hormizdas','św. Ignacy Antiocheński','św. Ignacy Loyola','św. Ireneusz z Lyonu','św. Ivan Rylski','św. Izaak Syryjczyk','św. Izydor z Peluzjum','św. Izydor z Sewilli','św. Jakub z Sarug','św. Jan Brebeuf','św. Jan Chryzostom','św. Jan Kasjan','św. Jan Klimak','św. Jan od Krzyża','św. Jan z Damaszku','św. Jan z Kronsztadu','św. Justyn','św. Józef z Kupertynu','św. Józefina Bakhita','św. Katarzyna z Genui','św. Katarzyna ze Sieny','św. Klara','św. Klaudiusz la Colombiere','św. Klemens Aleksandryjski','św. Klemens I święty','św. Klemens z Ochrydy','św. Kolumba Starszy','św. Kolumban Młodszy','św. Leander z Sewilli','św. Leon I Wielki','św. Ludwik Maria Grignion de Montfort','św. Ludwik z Granady','św. Makary Wielki','św. Maksym Wyznawca','św. Maksym z Turynu','św. Irena Mamcarz','św. Marceli z Ancyry','św. Marcin z Bragi','św. Marek Eremita','św. Maria Ena od Najświętszego Sakramentu','św. Maria od Jezusa Ukrzyżowanego','św. Małgorzata Maria Alacoque','św. Metody','św. Mikołaj Kabasilas','św. Mikołaj Serbski','św. Nerses Sznorhali','św. Optat z Milewe','św. Pacjan z Barcelony','św. Paisij Wełyczkowśkyj','św. Patryk','św. Paulin z Noli','św. Pio z Pietrelciny','św. Piotr Chryzolog','św. Piotr Damiani','św. Piotr z Alkantary','św. Possydiusz z Kalamy','św. Robert Bellarmin','św. Romanos Melodos','św. Rupert z Deutz','św. Serafin z Sarowa','św. Sofroniusz Jerozolimski','św. Edith Stein','św. Sydoniusz','św. Symeon','św. Szenute z Atripe','św. Teresa od Dzieciątka Jezus','św. Teresa od Jezusa z Los Andes', 'św. Teresa z Avili','św. Teresa z Kalkuty','św. Tomasz Morus','św. Tomasz à Kempis','św. Tomasz z Akwinu','św. Tomasz z Cantimpre','św. Wawrzyniec z Brindisi','św. Wenancjusz','św. Wiktor z Wity','św. Wincenty z Lerynu','św. Jan Paweł II','św. Zenon z Werony'])


#df_ora_new_forms_of_saints['NAME'].unique()


#%% Utworzenie słownika na potrzeby wordcloud (klucz to NAME, wartosc to ilosc wystapien/zapisów)

dictionary_of_saints = dict(df_ora_new_forms_of_saints['NAME'].value_counts())

#%% Wordcloud
#colormap = 'copper' (podobna do kolorów PBL)

custom_cmap_PBL = matplotlib.colors.ListedColormap(['#50504f', '#5f6062', '#87745e', '#968a7e', '#f3b65a'])

image = imageio.imread(r'C:\Users\PBL_Basia\Desktop\Promocja\Święci w PBL\spinacz_tło_wordcloud.png')

wordcloud = WordCloud(colormap=custom_cmap_PBL, max_font_size=2500, width = 6000, height = 5400, prefer_horizontal=1, min_font_size=6, background_color='white', collocations=False, mask=image).generate_from_frequencies(dictionary_of_saints)

plt.figure(figsize=[100,100])
plt.imshow(wordcloud, interpolation="bilinear")
plt.axis("off")

plt.savefig(f"C:\\Users\\PBL_Basia\\Desktop\\wordcloud_{datetime.today().date()}.png", format="png") 


























