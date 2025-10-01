import pandas as pd
#import numpy as np
import CSVgenerate

from concurrent.futures import ThreadPoolExecutor

def firstiteration(file):
    
    df = pd.read_csv(file)

    grouped = df.groupby('Категория')['Значение'].agg(['median', 'std']).reset_index()

    grouped.columns = ['Категория', 'Медиана', 'Стандартное_отклонение']

    return grouped



files = []
for i in range (1, 6):
    files.append(f'file{i}.csv')


CSVgenerate.GenerateData() 

with ThreadPoolExecutor(max_workers = 5) as executor:

    tables = list(executor.map(firstiteration, files))

    print("=== медиана и стандартное отклонение по каждой букве в каждой таблице ===")

    combined = pd.concat(tables, ignore_index=False)

    print(combined)

    print("\n=== Медиана из медиан и отклонение из медиан ===")
    result = combined.groupby('Категория')['Медиана'].agg(['median','std']).reset_index()
    result.columns = ['Категория', 'Медиана_медиан', 'Отклонение_медиан']

    print(result)

    

#files = [f'file{i}.csv' for i in range(1, 6)]
#all_data = []

#for file in files:
#    df = pd.read_csv(file)
#    all_data.append(df)

# Объединяем все данные
#full_df = pd.concat(all_data, ignore_index=True)

# Группировка по Категории: медиана и std
#grouped = full_df.groupby('Категория')['Значение'].agg(['median', 'std']).reset_index()
#grouped.columns = ['Категория', 'Медиана', 'Стандартное_отклонение']
#print("=== Медиана и стандартное отклонение по каждой букве ===")
#print(grouped)

# === 3. Медиана из медиан и std из медиан ===
#medians_by_category = full_df.groupby(['Категория', pd.cut(full_df.index, bins=5, labels=False)])['Значение'].median().reset_index()

# Но нам нужно для каждой категории: медианы по файлам, потом из них — итоговая медиана и std

# Сгруппируем по Категории и по индексу файла (поскольку 5 файлов)
#file_medians = full_df.groupby(['Категория', full_df.index // 5])['Значение'].median().reset_index(level=1, drop=True)

# Снова группируем по Категории, теперь считаем медиану и std из медиан по файлам
#final_stats = file_medians.groupby('Категория').agg(['median', 'std']).reset_index()
#final_stats.columns = ['Категория', 'Медиана_медиан', 'Std_медиан']
#print("\n=== Медиана из медиан и std из медиан ===")
#print(final_stats)