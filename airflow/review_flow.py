
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'jtong',
    'start_date': datetime.now() - timedelta(days = 1),
    'email': ['XXXX@XX.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('review_flow',
         default_args=default_args,
         schedule_interval='*/1 * * * *',
         catchup=False
         )
# t0 = BashOperator(
#     task_id='export py path',
#     bash_command='export PATH=/home/ec2-user/anaconda3/bin:$PATH',
#     retries=3,
#     dag=dag)

t1 = BashOperator(task_id='review_flow',
                  bash_command = '''
                  export PATH=/home/ec2-user/anaconda3/bin:$PATH
                  python /home/ec2-user/jason/project/yelp/airflow_jobs/review_flow.py''',dag = dag)

t2 = BashOperator(
    task_id='sleep',
    bash_command='sleep 5 ',
    retries=3,
    dag=dag)

# t0 >> 
t1 >> t2
