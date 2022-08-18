#%% import
import pandas as pd


#%% function
def creating_file_without_duplicates_and_sorted_by_date_of_publication(path):
    df = pd.read_excel(path)
    new_file = df.drop_duplicates()
    new_file["Data publikacji"] = pd.to_datetime(new_file["Data publikacji"])
    new_file = new_file.sort_values('Data publikacji', ascending=False)
    new_file.to_excel(path, index=False)
    
    
#%% main
creating_file_without_duplicates_and_sorted_by_date_of_publication(path)
