import pandas as pd
import psycopg2
import math

conn = psycopg2.connect(
  host='localhost',
  user='',
    dbname='yelp',
    password='',
    port=5432,
    connect_timeout=500)
cur = conn.cursor()

for i in range(668):
    table_num = math.ceil((i+1)/100)
    file_num = i + 1
    print(table_num)
    print(file_num)

    if i%100 == 0:
        build_table_cmd = '''drop table if exists yelp.master%(a)d;
         create table yelp.master%(a)d(
         business_id varchar,
         cool float,
         review_date timestamp,
         funny float,
         review_id varchar,
         stars float,
         text varchar,
         useful float,
         user_id varchar);''' %{"a":table_num}

        cur.execute(build_table_cmd)
        conn.commit


    copy_cmd = '''
      COPY yelp.master%(a)d FROM '/Users/apple/Documents/Bigdata/yelp_dataset/ParsedDataReview/%(b)d.0.csv' DELIMITER',' CSV HEADER;
      '''  %{"a":table_num,"b":file_num}

    cur.execute(copy_cmd)
    conn.commit()
