import pandas as pd

# wczytanie pliku
df = pd.read_csv(
    "data/AUDYCJE_PBL_202606.csv",
    dtype=str,
    encoding="utf-8"
)



df["Program"] = (
    df["ZA_OPIS_ADAPT_DZIELA"]
        .str.extract(
            r'(?i)(?:^|[.;]\s*|\s)(IV|III|II|I)\s*(?:[,.]|\s)',
            expand=False
        )
        .str.upper()
)


df.loc[
    df["Program"].isna() &
    df["ZA_OPIS_ADAPT_DZIELA"].str.contains(
        r'\b(?:RADIO\s+)?BIS\b',
        case=False,
        na=False
    ),
    "Program"
] = "BIS"




braki = df[df["Program"].isna()]

print(len(braki))

for opis in braki["ZA_OPIS_ADAPT_DZIELA"].dropna().head(50):
    print("-" * 80)
    print(opis)





# zapis wyniku
df.to_csv(
    "data/AUDYCJE_PBL_202606_z_programem.csv",
    index=False,
    encoding="utf-8"
)

print(df["Program"].value_counts(dropna=False))



















# podejrzane = df[df["Program"].isin(
#     ["V", "VI", "VII", "VIII", "IX", "X", "XI", "XII", "IIII"]
# )]

# print(
#     podejrzane[
#         ["ZA_ZAPIS_ID", "ZA_TYTUL", "ZA_OPIS_ADAPT_DZIELA", "Program"]
#     ].to_string()
# )