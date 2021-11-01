import datetime

from google.cloud import bigquery, storage

from pyspark import SparkContext
from pyspark.sql import SQLContext

PROJECT_ID = 'gcloudworkshop2021'
BUCKET_NAME = 'instagram-reporting'

def gcloud_reporting():
    top10 = get_bigquery_top10()
    upload_report_cloud_storage(top10)

def get_bigquery_top10():
    query = ""
    query.append("SELECT top.position as position, top.date as date, top.categoryId as categoryId, ")
    query.append("(SELECT DISTINCT r.categoryName FROM gcloudworkshop2021.resources r WHERE r.categoryId = top.categoryId) as categoryName, ")
    query.append("top.resourceId as resourceId, ")
    query.append("(SELECT r.resourceName FROM gcloudworkshop2021.resources r WHERE r.resourceId = top.resourceId) as resourceName")
    query.append("FROM ")
    query.append("(SELECT COUNT(e.eventId) as position, CURRENT_DATE() as date, r.categoryId as categoryId, r.resourceId as resourceId ")
    query.append("FROM gcloudworkshop2021.events e, gcloudworkshop2021.resources r ")
    query.append("WHERE e.resourceId = r.resourceId ")
    query.append("GROUP BY DATE(e.eventProcessingTime), r.categoryId, r.resourceId ")
    query.append("ORDER BY COUNT(e.eventId) DESC ")
    query.append("LIMIT 10) top")

    bigquery_client = bigquery.Client(project=PROJECT_ID)
    query_result = bigquery_client.query(query)

    lines = []

    for row in query_result:
        line = [row.position, row.date, row.categoryId, row.categoryName, row.resourceId, row.resourceName]
        line = '|'.join(str(x) for x in line)
        lines.append(line)
    
    return '\n'.join(lines)

def upload_report_cloud_storage(csv_text):
    storage_client = storage.Client(project=PROJECT_ID)

    bucket = storage_client.bucket(BUCKET_NAME)
    if not bucket.exists():
        bucket = storage_client.create_bucket(BUCKET_NAME, location="europe-west1")
    
    filename = datetime.datetime.now().strftime("%Y-%m-%d") + ".csv"
    blob = bucket.blob(filename)
    blob.upload_from_string(csv_text, content_type="text/plain")



def spark_reporting():
    sc = SparkContext("local")
    sq = SQLContext(sc)

    df_categories = sq.read.json('gs://gcloudworkshop2021/categories.json')
    df_categories.printSchema()

    df_resources = sq.read.json('gs://gcloudworkshop2021/resources.json')
    df_resources.printSchema()

    df_events = sq.read.json('gs://gcloudworkshop2021/events.json')
    df_resources.printSchema()

    # Using API
    df_categories.join(df_resources, df_categories.categoryId == df_resources.categoryId).show()
    df_resources.select('categoryId').show()



if __name__ == "__main__":
    gcloud_reporting()
    spark_reporting()
