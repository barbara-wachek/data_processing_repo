#%% import

import pandas as pd


#%% function

def create_file_without_duplicates(path):
    df = pd.read_excel(path)
    new_file = df.drop_duplicates()
    new_file.to_excel(path, index=False)
        
#%% main

create_file_without_duplicates('C:\\Users\\PBL_Basia\\Documents\\My scripts\\web_scraping_repo\\pgajda_2022-08-18.xlsx')
