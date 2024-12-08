from airflow.decorators import dag, task,task_group
from datetime import datetime
import requests
import json
import uuid
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.utils.edgemodifier import Label
import logging
from airflow.providers.apache.kafka.hooks.produce import KafkaProducerHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import pytz

tz=pytz.timezone("Asia/Ho_Chi_Minh")
logging.basicConfig(format="{asctime} - {levelname} - {message}",style="{",datefmt="%Y-%m-%d %H:%M")
job_variable=Variable.get('tiki-product-crawl-by-categories',deserialize_json=True)
api_config=job_variable['api']
s3_config=job_variable['s3']
s3_hook=S3Hook(aws_conn_id="levietnam-aws")
producer=KafkaProducerHook(kafka_config_id="levietnam-kafka").get_producer()
today = datetime.today()
date_string=today.strftime('%Y/%m/%d')
category_prefix=f"{s3_config['prefix']}/{date_string}/"

@task(task_id='exception_api')
def handle_exception_api():
    logging.warning(f"FAILED TO FETCH CATEGORIES")

@task(task_id='success_api')
def fetch_success(**kwargs):
    logging.info(f"SUCCESS TO FETCH CATEGORIES")

@task.branch
def check_status_code(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='fetch_categories', key='return_value')
    if data['status_code']!=200:
        return 'exception_api'
    
    return 'success_api'

@task(task_id="fetch_categories")
def fetch_categories():
    session=requests.session()
    res=session.get(api_config['category']['url'],
                    headers=api_config['category']['headers'])
    
    obj_name=str(uuid.uuid4())+".json"
    if res.status_code==200:
        s3_hook.load_string(
            string_data=json.dumps(res.json()['menu_block']['items']),
            bucket_name=s3_config['bucket'],
            key=f"{category_prefix}{obj_name}",
            replace=True 
        )

    return {
        "status_code":res.status_code,
    }

def fetch_product(category_id,category_name):
    session=requests.session()
    for page in range(10):
        res=session.get(f"{api_config['product']['url']}?limit=40&page={str(page)}&urlKey={category_name}&category={category_id}",
                    headers=api_config['product']['headers'])
        
        if res.status_code==200:
            data=res.json()['data']
            
            for product in data:
                product['crawled_at']=datetime.now().timestamp()*1000
                producer.produce(f'ecommerce-tiki-product-data-source', json.dumps(product).encode('utf8'))
                producer.flush()
            
@task_group
def fetch_products():

    list_obj=s3_hook.get_file_metadata(prefix=f"{category_prefix}",bucket_name=s3_config['bucket'])
    latest_key = sorted(list_obj, key=lambda x: x['LastModified'],reverse=True)[0]

    obj=s3_hook.get_key(bucket_name=s3_config['bucket'],key=latest_key['Key']).get()
    data=json.loads(obj['Body'].read().decode('utf-8'))

    task_list=[]
    for category in data[:1]:
        link=category['link']
        url_parser=link.split("/")
        category_id=url_parser[-1][1:]
        category_name=url_parser[-2]
        task_name=f"fetching_by_{category_name.replace(" ","_")}"
        op = PythonOperator(
                task_id=task_name,
                python_callable=fetch_product,
                op_kwargs={'category_id': category_id,'category_name':category_name}
            )
        task_list.append(op)

    task_list

@dag(dag_id="Crawl-Tiki-Product-By-Categories",schedule_interval='0 */1 * * *', start_date=datetime(2024, 1, 1), catchup=False)
def crawl_product_by_category():
    
    is_success=check_status_code()
    fetch_categories() >>is_success

    is_success>>Label('No')>>handle_exception_api()
    is_success>>Label('Yes')>>fetch_success() >>fetch_products()


crawl_product_by_category()
