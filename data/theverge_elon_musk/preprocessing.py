import pandas as pd

pd.set_option("display.max_columns", None)

df = pd.read_csv('data/theverge_elon_musk/theverge_elon_musk_all_articles.csv')

crypto_keywords = [
    'Bitcoin', 'Dogecoin'
]

pattern = '|'.join(crypto_keywords)

filtered_df = df[df['Title'].str.contains(pattern, case=False, na=False, regex=True)]

print(f"전체 기사 수: {df.shape[0]}")
print(f"가상화폐 관련 기사 수: {filtered_df.shape[0]}")
print(filtered_df)

# filtered_df.to_csv('data/theverge_elon_musk/crypto_related_articles.csv', index=False, encoding='utf-8-sig')

# print("가상화폐 관련 기사들이 'crypto_related_articles.csv' 파일로 저장되었습니다.")
