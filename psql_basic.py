# This is the Basic Setup and PSQL
import pandas as pd
import psycopg2

pd.options.display.max_colwidth = 50

conn = psycopg2.connect(
  host='localhost',
  user='',
    dbname='yelp',
    password='',
    port=5432,
    connect_timeout=500)
cur = conn.cursor()

def run_query(cmd):
    try:
        cur.execute(cmd)
        try:
            print(cur.fetchall())
        except:
            pass
    except:
        cnn.commit()


cmd = '''drop table if exists yelp.test;
         create table yelp.test(
         bussiness_id varchar,
         cool float,
         date timestamp,
         funny float,
         review_id varchar,
         stars float,
         text varchar,
         useful float,
         user_id varchar);'''

cur.execute(cmd)
df = pd.DataFrame(cur.fetchall())
df.columns = ['bussiness_id','cool','date','funny','review','stars','text','useful','user_id']
conn.commit()
