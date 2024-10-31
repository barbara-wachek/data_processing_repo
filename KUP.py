import cx_Oracle
import pandas as pd
import os
import datetime
import random

#%% Pobranie danych dostępowych ze zmiennych srodowiskowych
db_password = os.environ.get('DB_PASSWORD')
db_user = os.environ.get('DB_USER')
db_host = os.environ.get('DB_HOST')
db_port = os.environ.get('DB_PORT')
db_sid = os.environ.get('DB_SID')

#%% Połączenie z bazą Oracle (dostęp w pliku SQL_dostep)

cx_Oracle.init_oracle_client(lib_dir=r"C:\Users\PBL_Basia\Desktop\SQL\sqldeveloper\instantclient_19_6")
dsn_tns = cx_Oracle.makedsn(db_host, db_port, service_name=db_sid)
connection = cx_Oracle.connect(user=db_user, password=db_password, dsn=dsn_tns, encoding='windows-1250')


#%% Zapytania - wybrac odpowiednie
#Wprowadzić zmienne dla niektorych wartosci, aby mozna bylo wielkrotnie uzywac tego zapytania

user_PBL = "BARBARAW"
start_date = 
finish_date = 
journal = 



# query_by_dates = f'''
# select z.za_zapis_id, r.rz_nazwa, d.dz_nazwa, t.am_nazwisko, t.am_imie, z.za_tytul, z.za_adnotacje, zr.zr_tytul, z.za_zrodlo_rok, z.za_zrodlo_nr, z.za_zrodlo_str, z.za_uzytk_wpisal   
# from IBL_OWNER.pbl_zapisy z
# left join IBL_OWNER.pbl_zrodla zr on z.za_zr_zrodlo_id = zr.zr_zrodlo_id
# left join IBL_OWNER.pbl_zapisy_autorzy a on z.za_zapis_id = a.zaam_za_zapis_id
# left join IBL_OWNER.pbl_dzialy d on z.za_dz_dzial1_id = d.dz_dzial_id
# left join IBL_OWNER.pbl_autorzy t on a.zaam_am_autor_id = t.am_autor_id
# left join IBL_OWNER.pbl_rodzaje_zapisow r on z.za_rz_rodzaj1_id = r.rz_rodzaj_id
# where z.za_uzytk_wpis_data between TO_DATE('2022/09/01','YYYY-MM-DD') and TO_DATE('2022/09/19','YYYY-MM-DD')
# and z.za_uzytk_wpisal like '{user_PBL}'
# order by z.za_zapis_id
# '''




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

for index, row in df_ora.iterrows():
    if row['ID'] not in test_list_of_random_id: 
        


df_filtered = df_ora[~df_ora['ID'].isin(test_list_of_random_id)]









# where z.za_uzytk_wpis_data < '{DATA_END}'
# and z.za_uzytk_wpis_data > '{DATA_START}'
# and z.za_ro_rok >= 2004
# and z.za_uzytk_wpisal like '{PRACOWNIK}'
# and (z.za_zr_zrodlo_id is not null or z.za_zr_zrodlo_id not like 'x')






order by z.za_zapis_id
'''




















# date_end = datetime.date.today().strftime('%Y-%m-%d')
# date_start = '2020-01-01'
# pracownik = 'BARBARAW'



# query = '''
# select z.za_zapis_id, r.rz_nazwa, d.dz_nazwa, t.am_nazwisko, t.am_imie, z.za_tytul, z.za_adnotacje, zr.zr_tytul, z.za_zrodlo_rok, z.za_zrodlo_nr, z.za_zrodlo_str, z.za_uzytk_wpisal   
# from IBL_OWNER.pbl_zapisy z
# left join IBL_OWNER.pbl_zrodla zr on z.za_zr_zrodlo_id = zr.zr_zrodlo_id
# left join IBL_OWNER.pbl_zapisy_autorzy a on z.za_zapis_id = a.zaam_za_zapis_id
# left join IBL_OWNER.pbl_dzialy d on z.za_dz_dzial1_id = d.dz_dzial_id
# left join IBL_OWNER.pbl_autorzy t on a.zaam_am_autor_id = t.am_autor_id
# left join IBL_OWNER.pbl_rodzaje_zapisow r on z.za_rz_rodzaj1_id = r.rz_rodzaj_id
# where z.za_uzytk_wpis_data between TO_DATE('2022/09/01','YYYY-MM-DD') and TO_DATE('2022/09/19','YYYY-MM-DD')
# and z.za_uzytk_wpisal like '{pracownik}'
# order by z.za_zapis_id
# '''




# query_NW = '''
# select z.za_zapis_id id, z.za_type typ, rz.rz_nazwa rodzaj, dz.dz_nazwa dzial, a.am_nazwisko autor_nazwisko, a.am_imie autor_imie, z.za_tytul tytul, z.za_opis_wspoltworcow wspoltworcy, z.za_adnotacje adnotacja, zr.zr_tytul czasopismo, z.za_zrodlo_rok rok, z.za_zrodlo_nr numer, z.za_zrodlo_str strony, z.za_uzytk_wpisal, zr.zr_zrodlo_id, z.za_uzytk_wpis_data
# from pbl_zapisy z
# join IBL_OWNER.pbl_zapisy_autorzy za on za.zaam_za_zapis_id=z.za_zapis_id
# join IBL_OWNER.pbl_autorzy a on za.zaam_am_autor_id=a.am_autor_id
# join IBL_OWNER.pbl_zrodla zr on zr.zr_zrodlo_id=z.za_zr_zrodlo_id
# join IBL_OWNER.pbl_dzialy dz on dz.dz_dzial_id=z.za_dz_dzial1_id
# join IBL_OWNER.pbl_rodzaje_zapisow rz on rz.rz_rodzaj_id=z.za_rz_rodzaj1_id
# where z.za_uzytk_wpis_data < TO_DATE(:date_start, 'DD/MM/YYYY')
# and z.za_uzytk_wpis_data > '{date_start}'
# and z.za_ro_rok >= 2004
# and z.za_uzytk_wpisal like '{pracownik}'
# and (z.za_zr_zrodlo_id is not null or z.za_zr_zrodlo_id not like 'x')
# '''


date_end = datetime.date.today().strftime('%Y-%m-%d')
date_start = '2020-01-01'
pracownik = 'OLA'

query_gemini = f'''
SELECT z.za_zapis_id id, z.za_type typ, rz.rz_nazwa rodzaj, dz.dz_nazwa dzial, a.am_nazwisko autor_nazwisko, a.am_imie autor_imie, z.za_tytul tytul, z.za_opis_wspoltworcow wspoltworcy, z.za_adnotacje adnotacja, zr.zr_tytul czasopismo, z.za_zrodlo_rok rok, z.za_zrodlo_nr numer, z.za_zrodlo_str strony, z.za_uzytk_wpisal, zr.zr_zrodlo_id, z.za_uzytk_wpis_data
FROM pbl_zapisy z
JOIN IBL_OWNER.pbl_zapisy_autorzy za ON za.zaam_za_zapis_id = z.za_zapis_id
JOIN IBL_OWNER.pbl_autorzy a ON za.zaam_am_autor_id = a.am_autor_id
JOIN IBL_OWNER.pbl_zrodla zr ON zr.zr_zrodlo_id = z.za_zr_zrodlo_id
JOIN IBL_OWNER.pbl_dzialy dz ON dz.dz_dzial_id = z.za_dz_dzial1_id
JOIN IBL_OWNER.pbl_rodzaje_zapisow rz ON rz.rz_rodzaj_id = z.za_rz_rodzaj1_id
WHERE z.za_uzytk_wpis_data < :date_end
AND z.za_uzytk_wpis_data > TO_DATE(:date_start, 'YYYY-MM-DD')
AND z.za_ro_rok >= 2004
AND z.za_uzytk_wpisal LIKE ':pracownik'
AND (z.za_zr_zrodlo_id IS NOT NULL OR z.za_zr_zrodlo_id NOT LIKE 'x')
'''


#%% Przetwarzanie danych

df_ora = pd.read_sql(query, con=connection)
df_ora = df_ora.drop_duplicates() 

df_ora

with pd.ExcelWriter("C:\\Users\\PBL_Basia\\Desktop\\Kadry, sprawka, KUP\KUP\\2022\\2022_WRZESIEŃ_WACHEK_BARBARA.xlsx", engine='xlsxwriter') as writer:    
    df_ora.to_excel(writer, index=False, encoding='utf-8')   
    writer.save()  

