import os
import traceback

from clients.near import Near
from clients.bq import BQ
import datetime
from clients.helpers import clean, send_slack_alert

PROJECT_ID = os.environ.get("PROJECT_ID")
DATASET_ID = os.environ.get('DATASET_ID')
TABLE_ID = os.environ.get('TABLE_ID')
GCS_BUCKET_NAME = os.environ.get('GCS_BUCKET_NAME')
AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID")
# AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")
# AWS_BUCKET_NAME = os.environ.get('AWS_BUCKET_NAME')
# AWS_REGION_NAME = os.environ.get('AWS_REGION_NAME')


def main():
    bq = BQ(PROJECT_ID, DATASET_ID, TABLE_ID)
    near = Near()

    rewards = near.calculate_metrics()
    current_epoch = rewards["epoch"]

    if bq.has_current_data(current_epoch):
        print(f"Data for current period already in {PROJECT_ID}.{DATASET_ID}.{TABLE_ID}")
    else:
        bq.insert_epoch_to_bigquery(rewards)

    current_utc_time = datetime.datetime.now(datetime.timezone.utc)
    est_offset = datetime.timedelta(hours=-5)
    current_est_time = current_utc_time + est_offset
    is_after_7pm_est = current_est_time.hour >= 19  # Midnight UTC

    if is_after_7pm_est:
        data = clean(bq.get_historical_data())
        send_slack_alert(bq.write_to_s3(data, GCS_BUCKET_NAME))
        # send_slack_alert(upload_to_aws(data, AWS_BUCKET_NAME, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION_NAME))
    else:
        print("It's not 7 PM EST or later yet. Skipping file delivery.")

    print("Done.")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        send_slack_alert(f"Error collecting NEAR rewards: {str(e)}\nTraceback: {traceback.format_exc()}")
