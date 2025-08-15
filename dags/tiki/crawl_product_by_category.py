from airflow import DAG
import subprocess
from airflow.models import Variable
from utils import request_proxy
import pendulum
from airflow.decorators import task
import json

tiki_category_curl = Variable.get("tiki_category_curl")
tiki_product_by_category_curl = Variable.get("tiki_product_by_category_curl")

with DAG(
    dag_id="crawl_tiki_products",
    schedule="*/30 * * * *",
    max_active_runs=1,
    start_date=pendulum.datetime(2023, 5, 8, tz="Asia/Ho_Chi_Minh"),
    catchup=False,
    tags=["crawler",'tiki'],
    default_args={"owner": "levietnam"},
) as dag:

    @task 
    def get_category():
        proxy=request_proxy()
        curl_command = tiki_category_curl.replace('curl',f'curl --proxy {proxy}')
        curl_command=curl_command.split("\\")
        print(curl_command)
        result=subprocess.run(curl_command,shell=True,text=True,capture_output=True)
        print(result.stdout)
        print(result.stderr)
        response=json.loads(result.stdout)
        category= response.get("menu_block",{}).get("items",[])
        return category

    @task
    def get_products(category):
        proxy=request_proxy()
        
        link=category['link']
        url_key=link.split("/")[-2]
        category_id=link.split("/")[-1][1:]
        print(url_key,category_id)
        
        product_curl=tiki_product_by_category_curl
        for page in range(2):
            values={
                "$url_key":url_key,
                "$category_id":category_id,
                "$page":page
            }
            for k,v in values.items():
                product_curl=product_curl.replace(k,str(v))

        curl_command = product_curl.replace('curl',f'curl --proxy {proxy}')
        result=subprocess.run(product_curl,shell=True,text=True,capture_output=True)
        response=json.loads(result.stdout)

        return response.get(data,[])

    
        

    categories=get_category()
    products=get_products.expand(category=categories)
