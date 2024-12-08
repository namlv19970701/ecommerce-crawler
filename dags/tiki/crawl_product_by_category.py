from airflow.decorators import dag, task,task_group
from datetime import datetime
import requests
import json
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.utils.edgemodifier import Label
import logging
import pickle
from airflow.providers.apache.kafka.hooks.produce import KafkaProducerHook

logging.basicConfig(format="{asctime} - {levelname} - {message}",style="{",datefmt="%Y-%m-%d %H:%M")
tiki_crawler_variable=Variable.get('tiki_crawler_variable',deserialize_json=True)

@task(task_id='exception_api')
def handle_exception_api():
    logging.warning(f"FAILED TO FETCH CATEGORIES")

@task(task_id='success_api')
def fetch_success(**kwargs):
    logging.info(f"SUCCESS TO FETCH CATEGORIES")

@task.branch
def check_status_code(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='fetch_tiki_categories', key='return_value')
    if data['status_code']!=200:
        return 'exception_api'
    
    return 'success_api'

@task(task_id="fetch_tiki_categories")
def fetch_tiki_categories():
    
    session=requests.session()
    res=session.get(tiki_crawler_variable['category']['url'],
                    headers=tiki_crawler_variable['category']['headers'])
    if res.status_code==200:
        with open('categories.pickle', 'wb') as handle:
            pickle.dump(res.json()['menu_block']['items'], handle, protocol=pickle.HIGHEST_PROTOCOL)
    
    return {
        "status_code":res.status_code,
    }

def fetch_tiki_product(category_id,category_name):
    producer=KafkaProducerHook(kafka_config_id="kafka").get_producer()
    session=requests.session()
    for page in range(10):
        res=session.get(f"{tiki_crawler_variable['product']['url']}?limit=40&page={str(page)}&urlKey={category_name}&category={category_id}",
                    headers=tiki_crawler_variable['product']['headers'])
        
        if res.status_code==200:
            data=res.json()['data']
            
            for product in data:
                product['crawled_at']=datetime.now().timestamp()*1000
                producer.produce(f'ecommerce-tiki-product-data-source', json.dumps(product).encode('utf8'))
                producer.flush()
            
@task_group
def fetch_product():
    with open('categories.pickle', 'rb') as handle:
        data = pickle.load(handle)

    task_list=[]
    
    for category in data[:1]:
        link=category['link']
        url_parser=link.split("/")
        category_id=url_parser[-1][1:]
        category_name=url_parser[-2]
        task_name=f"fetching_by_{category_name.replace(" ","_")}"
        op = PythonOperator(
                task_id=task_name,
                python_callable=fetch_tiki_product,
                op_kwargs={'category_id': category_id,'category_name':category_name}
            )
        task_list.append(op)

    task_list

@dag(schedule_interval='0 */1 * * *', start_date=datetime(2024, 1, 1), catchup=False)
def crawl_product_by_category(**kwargs):
    
    is_success=check_status_code()
    fetch_tiki_categories() >>is_success

    is_success>>Label('No')>>handle_exception_api()
    is_success>>Label('Yes')>>fetch_success() >>fetch_product()


crawl_product_by_category()

