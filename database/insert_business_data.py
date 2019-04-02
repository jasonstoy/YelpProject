import pandas as pd
import psycopg2
import math

conn = psycopg2.connect(
  host='ec2url',
  user='postgres',
    dbname='yelp',
    password='xxxxxxx',
    port=5432,
    connect_timeout=500)
cur = conn.cursor()

for i in range():
    table_num = math.ceil((i+1)/100)
    file_num = i + 1
    print(table_num)
    print(file_num)

    if i%100 == 0:
        build_table_cmd = '''drop table if exists history.business%(a)d;
         create table history.business%(a)d(
         business_id varchar,
         name varchar,
         address varchar,
         city varchar,
         state varchar,
         postal_code varchar,
         latitude float,
         longitude float,
         stars float,
         review_count float,
         categories varchar);''' %{"a":table_num}

        cur.execute(build_table_cmd)
        conn.commit()


    copy_cmd = '''
      COPY history.business%(a)d FROM '/home/ec2-user/jason/data/ParsedDataBusiness/%(b)d.0.csv' DELIMITER',' CSV HEADER;
      '''  %{"a":table_num,"b":file_num}
# Make sure the whole directory is accessanle (chmod)
    cur.execute(copy_cmd)
    conn.commit()
