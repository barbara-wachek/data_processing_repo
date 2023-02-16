import cx_Oracle
import pandas as pd


#%% Połączenie z bazą Oracle (dostęp w pliku SQL_dostep)

cx_Oracle.init_oracle_client(lib_dir=r"C:\Users\PBL_Basia\Desktop\SQL\sqldeveloper\instantclient_19_6")
dsn_tns = cx_Oracle.makedsn('XXX', 'XXX', service_name='xe')
connection = cx_Oracle.connect(user='XXXX', password='XXXX', dsn=dsn_tns, encoding='windows-1250')


#%% Zapytanie

query = '''
select z.za_zapis_id, r.rz_nazwa, d.dz_nazwa, t.am_nazwisko, t.am_imie, z.za_tytul, z.za_adnotacje, zr.zr_tytul, z.za_zrodlo_rok, z.za_zrodlo_nr, z.za_zrodlo_str, z.za_uzytk_wpisal   
from IBL_OWNER.pbl_zapisy z
left join IBL_OWNER.pbl_zrodla zr on z.za_zr_zrodlo_id = zr.zr_zrodlo_id
left join IBL_OWNER.pbl_zapisy_autorzy a on z.za_zapis_id = a.zaam_za_zapis_id
left join IBL_OWNER.pbl_dzialy d on z.za_dz_dzial1_id = d.dz_dzial_id
left join IBL_OWNER.pbl_autorzy t on a.zaam_am_autor_id = t.am_autor_id
left join IBL_OWNER.pbl_rodzaje_zapisow r on z.za_rz_rodzaj1_id = r.rz_rodzaj_id
where z.za_uzytk_wpis_data between TO_DATE('2022/09/01','YYYY-MM-DD') and TO_DATE('2022/09/19','YYYY-MM-DD')
and z.za_uzytk_wpisal like 'BARBARAW'
order by z.za_zapis_id
'''

#%% Przetwarzanie danych

df_ora = pd.read_sql(query, con=connection)
df_ora = df_ora.drop_duplicates() 

df_ora

with pd.ExcelWriter("C:\\Users\\PBL_Basia\\Desktop\\Kadry, sprawka, KUP\KUP\\2022\\2022_WRZESIEŃ_WACHEK_BARBARA.xlsx", engine='xlsxwriter') as writer:    
    df_ora.to_excel(writer, index=False, encoding='utf-8')   
    writer.save()  

