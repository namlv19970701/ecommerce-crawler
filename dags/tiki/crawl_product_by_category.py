from airflow import DAG
import subprocess
from airflow.models import Variable
from utils import request_proxy
import pendulum
from airflow.decorators import task
import json
from airflow.providers.apache.kafka.hooks.produce import KafkaProducerHook
from datetime import datetime 
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

tiki_category_curl = Variable.get("tiki_category_curl")
tiki_product_by_category_curl = Variable.get("tiki_product_by_category_curl")

with DAG(
    dag_id="crawl_tiki_products",
    schedule="*/60 * * * *",
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

        # producer = KafkaProducerHook(
        #     kafka_config_id="kafka",  # Make sure you configure the Kafka connection in Airflow
        # ).get_producer()
        client = S3Hook(aws_conn_id='s3')
        total_items=0
        product_curl=tiki_product_by_category_curl
        for page in range(1,2):
            values={
                "$url_key":url_key,
                "$category_id":category_id,
                "$page":page
            }
            for k,v in values.items():
                product_curl=product_curl.replace(k,str(v))

            curl_command = product_curl.replace('curl',f'curl --proxy {proxy}')
            curl_command=curl_command.split("\\")
            result=subprocess.run(curl_command,shell=True,text=True,capture_output=True)
            response=json.loads(result.stdout)
            now=datetime.now()
            today=now.strftime("%Y/%m/%d")
            crawled_at=int(now.timestamp()*1000)
            
            data={'data':response.get('data',[])}
            
            data.update({
                "crawled_at":int(datetime.now().timestamp()*1000),
                "category_id":category_id,
                "category_name":url_key
            })
                
            client.load_string(
                string_data=json.dumps(data),
                key=f"raw/tiki/{today}/{crawled_at}.json",  # đường dẫn trong bucket
                bucket_name="test",    # tên bucket
                replace=True                # ghi đè nếu tồn tại
            )

            total_items+=len(data['data'])
            # for item in data:
            #     item.update({
            #         "crawled_at":int(datetime.now().timestamp()*1000),
            #         "category_id":category_id,
            #         "category_name":url_key
            #     })
            #     producer.produce(
            #         topic="crawl-tiki-product",
            #         value=json.dumps(item).encode("utf8"),
            #     )
            #     total_items+=1
            # producer.flush()

        return total_items

    @task
    def sum_total_crawled_products(total_items):
        return sum(total_items)
        
        
    categories=get_category()
    products=get_products.expand(category=categories)
    sum_total_crawled_products(products)
