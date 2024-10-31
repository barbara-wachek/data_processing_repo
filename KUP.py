import cx_Oracle
import pandas as pd
import os
import datetime
import random

#%% System variables
db_password = os.environ.get('DB_PASSWORD')
db_user = os.environ.get('DB_USER')
db_host = os.environ.get('DB_HOST')
db_port = os.environ.get('DB_PORT')
db_sid = os.environ.get('DB_SID')

#%% Connect with DB

cx_Oracle.init_oracle_client(lib_dir=r"C:\Users\PBL_Basia\Desktop\SQL\sqldeveloper\instantclient_19_6")
dsn_tns = cx_Oracle.makedsn(db_host, db_port, service_name=db_sid)
connection = cx_Oracle.connect(user=db_user, password=db_password, dsn=dsn_tns, encoding='windows-1250')


#%% Query

date_start = '2020/01/01'
date_end = datetime.date.today().strftime('%Y/%m/%d')
user_pbl = 'KAROLINA'

query = f'''
select z.za_zapis_id id, z.za_type typ, rz.rz_nazwa rodzaj, dz.dz_nazwa dzial, a.am_nazwisko autor_nazwisko, a.am_imie autor_imie, z.za_tytul tytul, z.za_opis_wspoltworcow wspoltworcy, z.za_adnotacje adnotacja, zr.zr_tytul czasopismo, z.za_zrodlo_rok rok, z.za_zrodlo_nr numer, z.za_zrodlo_str strony, z.za_uzytk_wpisal, zr.zr_zrodlo_id, z.za_uzytk_wpis_data
from pbl_zapisy z
join IBL_OWNER.pbl_zapisy_autorzy za on za.zaam_za_zapis_id=z.za_zapis_id
join IBL_OWNER.pbl_autorzy a on za.zaam_am_autor_id=a.am_autor_id
join IBL_OWNER.pbl_zrodla zr on zr.zr_zrodlo_id=z.za_zr_zrodlo_id
join IBL_OWNER.pbl_dzialy dz on dz.dz_dzial_id=z.za_dz_dzial1_id
join IBL_OWNER.pbl_rodzaje_zapisow rz on rz.rz_rodzaj_id=z.za_rz_rodzaj1_id
where z.za_uzytk_wpis_data between TO_DATE('{date_start}','YYYY-MM-DD') and TO_DATE('{date_end}','YYYY-MM-DD')
and z.za_ro_rok >= 2004
and z.za_uzytk_wpisal like '{user_pbl}'
and (z.za_zr_zrodlo_id is not null or z.za_zr_zrodlo_id not like 'x')
order by z.za_zapis_id
'''


df_ora = pd.read_sql(query, con=connection)


test_list_of_real_id = df_ora['ID'][:20].tolist()

def generate_random_int(length):
    """Generuje losową liczbę całkowitą o podanej długości."""
    min_val = 10**(length-1)
    max_val = 10**length - 1
    return random.randint(min_val, max_val)

test_list_of_random_id = [generate_random_int(7) for x in range(20)]
test_list_of_random_id.append(1415923)


df_filtered = df_ora[~df_ora['ID'].isin(test_list_of_random_id)]


#DALEJ: 
# filtrownie
# identyfikatory not in wcześniej wykorzystane; pliki z już wykorzystanymi identyfikatorami dla każdej osoby oddzielnie 
# plik xlsx → python set()
# df_filtered = df[~df['id'].isin(identyfikatory)]
# sort → rok, pismo
# dzielenie na paczki → 180-220 rekordów (można 200 na sztywno)
# zastanowić się, co zrobić z resztkami
# zapisać pliki jako .ods z odpowiednimi kolumnami (mniej kolumn niż w zwrotce z sql → z.za_zapis_id id, z.za_type typ, rz.rz_nazwa rodzaj, dz.dz_nazwa dzial, a.am_nazwisko autor_nazwisko, a.am_imie autor_imie, z.za_tytul tytul, z.za_opis_wspoltworcow wspoltworcy, z.za_adnotacje adnotacja, zr.zr_tytul czasopismo, z.za_zrodlo_rok rok, z.za_zrodlo_nr numer, z.za_zrodlo_str strony)
# dodać identyfikatory do listy już wykorzystanych

#Wykorzystać funkcję input? 

#%% Przetwarzanie danych

df_ora = pd.read_sql(query, con=connection)
df_ora = df_ora.drop_duplicates() 

df_ora

with pd.ExcelWriter("C:\\Users\\PBL_Basia\\Desktop\\Kadry, sprawka, KUP\KUP\\2022\\2022_WRZESIEŃ_WACHEK_BARBARA.xlsx", engine='xlsxwriter') as writer:    
    df_ora.to_excel(writer, index=False, encoding='utf-8')   
    writer.save()  

