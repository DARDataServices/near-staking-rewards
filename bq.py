from google.cloud import bigquery, storage
from datetime import datetime
from io import StringIO


class BQ:
    def __init__(self, project_id, dataset_id, table_id):
        self.storage_client = storage.Client()
        self.bq_client = bigquery.Client()
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_id = table_id

    def insert_epoch_to_bigquery(self, row):
        table_ref = f"{self.project_id}.{self.dataset_id}.{self.table_id}"
        self.bq_client.insert_rows_json(table_ref, [row])
        print("Row successfully inserted into BigQuery table.")

    def get_historical_data(self):
        curr_date= int(datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0).timestamp())
        end_date = curr_date - (30 * 86400)  # 30 days in seconds

        query = f"""
            SELECT * FROM `{self.dataset_id}.{self.table_id}`
            WHERE timestamp >= {end_date} AND timestamp < {curr_date}
            ORDER BY epoch DESC
        """
        query_job = self.bq_client.query(query)
        return query_job.result().to_dataframe()

    def has_current_data(self, current_epoch):
        query = f"""
                   SELECT *
                   FROM `{self.project_id}.{self.dataset_id}.{self.table_id}`
                   WHERE epoch = {current_epoch};
                """
        results = self.bq_client.query(query)
        return bool(list(results))

    def write_to_s3(self, data, bucket_name):
        csv_buffer = StringIO()
        data.to_csv(csv_buffer, index=False)

        file_name = f"DARNEARYieldFile_{datetime.utcnow().strftime('%Y%m%d')}.csv"

        bucket = self.storage_client.bucket(bucket_name)
        blob = bucket.blob(file_name)
        blob.upload_from_string(csv_buffer.getvalue(), content_type="text/csv")

        return f"Data successfully written to gs://{bucket_name}/{file_name}"