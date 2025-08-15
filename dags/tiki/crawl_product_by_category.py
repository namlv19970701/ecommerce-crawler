from airflow import DAG
import subprocess
from airflow.models import Variable
from utils import request_proxy
import pendulum
from airflow.decorators import task

tiki_category_curl = Variable.get("tiki_category_curl")

with DAG(
    dag_id="crawl_tiki_products",
    schedule="*/10 * * * *",
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
        print(curl_command)
        result=subprocess.run(curl_command,shell=True,text=True,capture_output=True)
        print(result.stdout)
        print(result.stderr)
        return result.stdout

    get_category()

