#%%% Description



#%% Import
import cx_Oracle
import pandas as pd
import os
import datetime
import random
# import argparse

from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
from gspread_dataframe import set_with_dataframe, get_as_dataframe
import gspread as gs




#%% System variables

db_password = os.environ.get('DB_PASSWORD')
db_user = os.environ.get('DB_USER')
db_host = os.environ.get('DB_HOST')
db_port = os.environ.get('DB_PORT')
db_sid = os.environ.get('DB_SID')

#%% VARIABLES

date_start = '2020/01/01'
date_end = datetime.date.today().strftime('%Y/%m/%d')

#%% connect google drive

#autoryzacja do tworzenia i edycji plików
gc = gs.oauth()
#autoryzacja do penetrowania dysku
gauth = GoogleAuth()
gauth.LocalWebserverAuth()
drive = GoogleDrive(gauth)


#%% Functions

def gsheet_to_df(gsheetId, worksheet):
    gc = gs.oauth()
    sheet = gc.open_by_key(gsheetId)
    df = get_as_dataframe(sheet.worksheet(worksheet), evaluate_formulas=True, dtype=str).dropna(how='all').dropna(how='all', axis=1)
    return df

#Wybór nazwy użytkownika z predefiniowanej listy
def get_user_pbl():
    proper_users = ['KAROLINA', 'IZA', 'OLA', 'GOSIA', 'BEATAD', 'BEATAS', 'TOMASZ', 'EWA', 'BARBARAW', 'BEATAK', 'NIKODEM', 'CEZARY', 'TOMASZU']

    while True:
        answer = input("Wpisz nazwę użytkownika: ")
        if answer in proper_users:
            user_pbl = answer
            print(f"Wybrałeś: {answer}")
            return user_pbl  # wychodzimy z pętli, bo wpisano poprawną wartość
        else:
            print("Błąd: wpisano niedozwoloną wartość. Spróbuj ponownie.")
        
        

def connect_to_database():
    """Connects to the Oracle database and returns a connection object."""
    cx_Oracle.init_oracle_client(lib_dir=r"C:\Users\PBL_Basia\Desktop\SQL\sqldeveloper\instantclient_19_6")
    dsn_tns = cx_Oracle.makedsn(os.environ.get('DB_HOST'), os.environ.get('DB_PORT'), service_name=os.environ.get('DB_SID'))

    try:
        connection = cx_Oracle.connect(user=os.environ.get('DB_USER'), password=os.environ.get('DB_PASSWORD'), dsn=dsn_tns, encoding='windows-1250')
        return connection
    except cx_Oracle.Error as error:
        print(f"Error connecting to database: {error}")
        return None


def execute_query(conn, query, param):
    cursor = conn.cursor()
    cursor.execute(query, user_pbl)
    columns = [col[0] for col in cursor.description]
    df = pd.DataFrame(cursor.fetchall(), columns=columns)
    cursor.close()
    return df

def split_dataframe(df, min_rows=180, max_rows=220):
    """Dzieli DataFrame na mniejsze DataFrame'y o liczbie wierszy z przedziału [min_rows, max_rows]."""

    result = []
    start_index = 0
    n = len(df)
    
    if n <= min_rows:
        print("Ostrzeżenie: DataFrame jest za mały, aby go podzielić na kilka plików.")
        return [df]
    
    while start_index < n:
        # losowa liczba wierszy w przedziale min_rows–max_rows, ale nie więcej niż pozostało
        chunk_size = min(random.randint(min_rows, max_rows), n - start_index)
        chunk = df.iloc[start_index:start_index + chunk_size]
        result.append(chunk)
        start_index += chunk_size
    
    return result


# def save_dataframes_to_excel(dataframes, user_pbl, output_dir="data", file_format=".xlsx"):
#     """
#     Zapisuje listę DataFrame'ów lub pojedynczy DataFrame do osobnych plików Excel.

#     Args:
#         dataframes: Lista DataFrame'ów lub pojedynczy DataFrame.
#         output_dir: Katalog, w którym mają być zapisane pliki.
#         user_pbl: Prefiks nazwy pliku.
#         file_format: Rozszerzenie pliku (domyślnie .xlsx).
#     """

#     os.makedirs(output_dir, exist_ok=True)

#     if not isinstance(dataframes, list):
#         dataframes = [dataframes]

#     for i, df in enumerate(dataframes):
#         file_name = f"{output_dir}/{user_pbl}_{datetime.date.today().strftime('%Y-%m-%d')}_{i+1}{file_format}"
#         df.to_excel(file_name, index=False)







#%% Main

if __name__ == "__main__":
    
    user_pbl = get_user_pbl()
    file_with_user_records = '1DpHG81z-HCu1JT4mG2whMxWZ83Zas9j6CerZ36HsGFY'

    query = '''
    select distinct z.za_zapis_id as ID, 
           z.za_type as TYP,
           r.rz_nazwa as RODZAJ, 
           d.dz_nazwa as DZIAŁ, 
           t.am_nazwisko as AUTOR_NAZWISKO, 
           t.am_imie as AUTOR_IMIE, 
           z.za_tytul as TYTUL, 
           z.za_adnotacje AS ADNOTACJA, 
           zr.zr_tytul as CZASOPISMO, 
           z.za_zrodlo_rok AS ROK, 
           z.za_zrodlo_nr AS NUMER, 
           z.za_zrodlo_str AS STRONY, 
           z.za_uzytk_wpisal AS UZYTKOWNIK   
    from IBL_OWNER.pbl_zapisy z
    left join IBL_OWNER.pbl_zrodla zr on z.za_zr_zrodlo_id = zr.zr_zrodlo_id
    left join IBL_OWNER.pbl_zapisy_autorzy a on z.za_zapis_id = a.zaam_za_zapis_id
    left join IBL_OWNER.pbl_dzialy d on z.za_dz_dzial1_id = d.dz_dzial_id
    left join IBL_OWNER.pbl_autorzy t on a.zaam_am_autor_id = t.am_autor_id
    left join IBL_OWNER.pbl_rodzaje_zapisow r on z.za_rz_rodzaj1_id = r.rz_rodzaj_id
    where upper(z.za_uzytk_wpisal) like upper(:user_pbl)
    order by z.za_zapis_id
    '''
    
    
    
    with connect_to_database() as conn:
        if conn:
            df = execute_query(conn, query, user_pbl)
                      
            existing_df = gsheet_to_df(file_with_user_records, 'Arkusz1')
            used_id_list = existing_df['ID'].tolist()
            final_df = df[~df['ID'].isin(used_id_list)]
            
            # result = split_dataframe(final_df)  #lista dataframów lub 1 dataframe jesli jest mniej niz 180 zapisow i nie da podzielic
            # save_dataframes_to_excel(result)
            
            # Zapisz do pliku dataframe ze wszystkimi niewykorzystanymi dotychczas rekordami (zamień potem na format ods)
            
            
            if not final_df.empty:
                # Podział final_df na mniejsze pliki
                chunks = split_dataframe(final_df, min_rows=180, max_rows=220)
                
                for i, chunk in enumerate(chunks, start=1):
                    filename = f"data\\{user_pbl}_{datetime.date.today().strftime('%Y-%m-%d')}_part{i}.xlsx"
                    chunk.to_excel(filename, index=False)
                    print(f"Zapisano plik: {filename}")
                
                # Aktualizacja pliku ze wszystkimi ID
                final_df_only_ID = pd.DataFrame({'ID': final_df['ID'].tolist()})
                combined_df = pd.concat([existing_df, final_df_only_ID], ignore_index=True)
                combined_df.to_excel(file_with_user_records, index=False)
                print(f"Zaktualizowano plik z wszystkimi rekordami: {file_with_user_records}")
            else:
                print("Brak nowych rekordów do zapisania.")
        else:
            print("Nie można połączyć się z bazą.")




# #%% Query - moj kod

# #Ponizsze zapytanie generuje wszystkie zapisy danej osoby w okreslonym czasie.

# query = f'''
# select z.za_zapis_id id, z.za_type typ, rz.rz_nazwa rodzaj, dz.dz_nazwa dzial, a.am_nazwisko autor_nazwisko, a.am_imie autor_imie, z.za_tytul tytul, z.za_opis_wspoltworcow wspoltworcy, z.za_adnotacje adnotacja, zr.zr_tytul czasopismo, z.za_zrodlo_rok rok, z.za_zrodlo_nr numer, z.za_zrodlo_str strony, z.za_uzytk_wpisal, zr.zr_zrodlo_id, z.za_uzytk_wpis_data
# from pbl_zapisy z
# join IBL_OWNER.pbl_zapisy_autorzy za on za.zaam_za_zapis_id=z.za_zapis_id
# join IBL_OWNER.pbl_autorzy a on za.zaam_am_autor_id=a.am_autor_id
# join IBL_OWNER.pbl_zrodla zr on zr.zr_zrodlo_id=z.za_zr_zrodlo_id
# join IBL_OWNER.pbl_dzialy dz on dz.dz_dzial_id=z.za_dz_dzial1_id
# join IBL_OWNER.pbl_rodzaje_zapisow rz on rz.rz_rodzaj_id=z.za_rz_rodzaj1_id
# where z.za_uzytk_wpis_data between TO_DATE('{DATE_START}','YYYY-MM-DD') and TO_DATE('{date_end}','YYYY-MM-DD')
# and z.za_ro_rok >= 2004
# and z.za_uzytk_wpisal like '{user_pbl}'
# and (z.za_zr_zrodlo_id is not null or z.za_zr_zrodlo_id not like 'x')
# order by z.za_zapis_id
# '''




# #DALEJ: 
# # filtrownie
# # identyfikatory not in wcześniej wykorzystane; 
# # plik xlsx → python set()
# # df_filtered = df[~df['id'].isin(identyfikatory)]
# # sort → rok, pismo
# # dzielenie na paczki → 180-220 rekordów (można 200 na sztywno)
# # zastanowić się, co zrobić z resztkami
# # zapisać pliki jako .ods z odpowiednimi kolumnami (mniej kolumn niż w zwrotce z sql → z.za_zapis_id id, z.za_type typ, rz.rz_nazwa rodzaj, dz.dz_nazwa dzial, a.am_nazwisko autor_nazwisko, a.am_imie autor_imie, z.za_tytul tytul, z.za_opis_wspoltworcow wspoltworcy, z.za_adnotacje adnotacja, zr.zr_tytul czasopismo, z.za_zrodlo_rok rok, z.za_zrodlo_nr numer, z.za_zrodlo_str strony)
# # dodać identyfikatory do listy już wykorzystanych



