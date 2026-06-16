#Analiza plików 

import pandas as pd




df_articles = pd.read_excel(r"C:\Users\barba\Documents\GitHub\PBL_updating_records\data\bn_articles_marc_2026-01-29.xlsx")



df_articles['260'].value_counts()

df_articles.shape[0] #- 14920

count = df_articles[df_articles['260'].astype(str).str.contains(r"202\d\.$", regex=True)].shape[0]
print(count)  # 10980 artykułów 



df_chapters = pd.read_excel(r"C:\Users\barba\Documents\GitHub\PBL_updating_records\data\bn_chapters_marc_2026-01-30.xlsx")


df_chapters.shape[0] #2142

count = df_chapters[df_chapters['260'].astype(str).str.contains(r"202\d\.$", regex=True)].shape[0]
print(count)  #1116




df_books =  pd.read_excel(r"C:\Users\barba\Documents\GitHub\PBL_updating_records\data\bn_books_marc_2026-01-30.xlsx")

df_books.shape[0] # 28191

count = df_books[df_books['260'].astype(str).str.contains(r"202\d\.$", regex=True)].shape[0]
print(count)  #22728

