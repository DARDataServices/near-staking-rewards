from google.cloud import bigquery, storage
from datetime import datetime
import os
from io import StringIO


class BQ:
    def __init__(self, project_id, dataset_id, table_id):
        self.storage_client = storage.Client()
        self.bq_client = bigquery.Client()
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_id = table_id

    def insert_epoch_to_bigquery(self, row):
        table_ref = self.bq_client.dataset(self.dataset_id).table(self.table_id)

        row_to_insert = {
            "epoch": row[0],
            "timestamp": datetime.utcnow(),
            "total_staked_near": row[2],
            "epoch_rewards": row[3],
            "active_validators": row[4],
        }
        self.bq_client.insert_rows_json(table_ref, [row_to_insert])
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

        self.storage_client.put_object(Bucket=bucket_name, Key=file_name, Body=csv_buffer.getvalue())
        return f"Data successfully written to {bucket_name}/{file_name}"