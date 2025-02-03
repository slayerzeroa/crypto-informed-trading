# import glob
# import os
# import pandas as pd

# csv_dir = os.path.join('data', 'theverge_elon_musk')

# csv_files = glob.glob(os.path.join(csv_dir, '*.csv'))

# df_list = []

# for file in csv_files:
#     try:
#         # CSV 파일 읽기
#         df = pd.read_csv(file, encoding='utf-8-sig')  # utf-8-sig 인코딩을 사용하여 한글 깨짐 방지
#         df_list.append(df)
#         print(f"성공적으로 불러온 파일: {file}")
#     except Exception as e:
#         print(f"파일을 불러오는 중 오류 발생: {file}\n오류 내용: {e}")

# combined_df = pd.concat(df_list, ignore_index=True)

# combined_df.drop_duplicates(subset=['URL'], inplace=True)

# print(f"총 합쳐진 데이터프레임의 행 수: {combined_df.shape[0]}")

# output_file = os.path.join(csv_dir, 'theverge_elon_musk_all_articles.csv')
# combined_df.to_csv(output_file, index=False, encoding='utf-8-sig')

# print(f"모든 CSV 파일이 성공적으로 합쳐져 '{output_file}' 파일로 저장되었습니다.")