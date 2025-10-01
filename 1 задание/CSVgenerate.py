import pandas as pd

import random

#def generate_data():
 #   categories = [random.choice(['A', 'B', 'C', 'D']) for i in range(5)]
  #  nums = [round(random.uniform(0, 1000000), 5) for i in range(5)]
   # return {'Категория': categories , 'Значение': nums}
def GenerateData():
    for i in range(1, 6):
        df = pd.DataFrame({'Категория':[random.choice(['A', 'B', 'C', 'D']) for i in range(10000)],'Значение' : [round(random.uniform(0, 100), 5) for i in range(10000)]})
        df.to_csv(f'file{i}.csv', index=False)