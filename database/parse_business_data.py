import pandas as pd
import psycopg2
import math
import json
import heapq
from pandas.io.json import json_normalize
import decimal

csv_dir = '/Users/apple/Documents/Bigdata/yelp_dataset/ParsedDataBusiness/'
df = pd.DataFrame([])
i = 0
with open('/Users/apple/Documents/Bigdata/yelp_dataset/business.json', 'r', encoding="utf8") as f:
    Head = True
    for line in f:
        if Head:
            Head = False
            continue
        # print(line)
        json_data = json.loads(line)
        json_norm = json_normalize(json_data)
        data = json_norm[['business_id','name','address','city','state','postal_code','latitude','longitude','stars','review_count','categories']]
        df = df.append(data)
        i += 1
        print(i)
        if i % 10000 == 0:
            print(i)
            df.to_csv(csv_dir + str(i/10000) +'.csv', index=False,sep = '|')
            df = pd.DataFrame([])
