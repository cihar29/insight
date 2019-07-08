#put in airflow/dags directory

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import timedelta, datetime

default_args = {
    'owner': 'insight',
    'depends_on_past': False,
    'start_date': datetime.today() - timedelta(minutes=20),
    # datetime.now() - timedelta(minutes=20) # could be useful for the future
    'email': ['airflow@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG('update_elasticsearch', default_args=default_args, schedule_interval=timedelta(minutes=1))

java_update_elasticsearch = 'java -cp /home/ubuntu/insight/target/insight-1.0-SNAPSHOT.jar elasticsearch.Elasticsearch'

update_elasticsearch = BashOperator(
    task_id='update_elasticsearch',
    bash_command=java_update_elasticsearch,
    dag=dag)

