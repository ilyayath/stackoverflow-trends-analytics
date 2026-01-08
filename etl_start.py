import requests
import pandas as pd
import time
from google.cloud import bigquery
from google.oauth2 import service_account

KEY_PATH = 'credentials.json'
PROJECT_ID = 'data-portfolio-lab'
DATASET_ID = 'stackoverflow_data'
TABLE_ID = 'questions_raw'


URL = "https://api.stackexchange.com/2.3/questions"
params = {
    "site" : "stackoverflow",
    "tagged": "python",
    "sort": "creation",
    "order": "desc",
    "pagesize": 100
}

response = requests.get(URL, params=params)
data = response.json()
items = data.get("items", [])

df = pd.DataFrame(items)
df = df[['question_id', 'creation_date', 'title', 'view_count', 'tags', 'score']]

df['creation_date'] = pd.to_datetime(df['creation_date'], unit='s')

credentials = service_account.Credentials.from_service_account_file(KEY_PATH)
client = bigquery.Client(credentials=credentials, project=credentials.project_id)

dataset_ref = client.dataset(DATASET_ID)
try:
    client.get_dataset(dataset_ref)
    print("Dataset exists")
except:
    dataset = bigquery.Dataset(dataset_ref)
    dataset.location = "US"
    client.create_dataset(dataset)
    print("Dataset created")

table_ref = dataset_ref.table(TABLE_ID)

job_config = bigquery.LoadJobConfig(
    # Явно вказуємо схему, щоб tags стали масивом (REPEATED), а не рядком
    schema=[
        bigquery.SchemaField("question_id", "INTEGER"),
        bigquery.SchemaField("creation_date", "TIMESTAMP"),
        bigquery.SchemaField("title", "STRING"),
        bigquery.SchemaField("view_count", "INTEGER"),
        bigquery.SchemaField("score", "INTEGER"),
        bigquery.SchemaField("tags", "STRING", mode="REPEATED"), # <--- ОСЬ ВОНО!
    ],
    # WRITE_APPEND - додавати нові рядки. WRITE_TRUNCATE - перезаписувати таблицю.
    write_disposition="WRITE_APPEND",
)

job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
job.result()