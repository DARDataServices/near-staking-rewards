import requests
import os
from near import Near
from bq import BQ
import datetime
from helpers import clean, upload_to_aws, send_slack_alert

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
        rewards = rewards # TODO get proper data, covert to row
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


# def fetch_validator_data():
#     url = "https://rpc.mainnet.near.org"
#     headers = {"Content-Type": "application/json"}
#     payload = {
#         "jsonrpc": "2.0",
#         "id": "dontcare",
#         "method": "validators",
#         "params": [None]
#     }
#     response = requests.post(url, headers=headers, json=payload)
#     return response.json()
#
# def sum_total_staked(validator_data):
#     total_stake = 0
#     for validator in validator_data['result']['current_validators']:
#         total_stake += int(validator['stake'])
#     return total_stake
#
# def convert_yocto_to_near(yocto_near_amount):
#     return yocto_near_amount / (10**24)
#
# def count_active_validators(validator_data):
#     return len(validator_data['result']['current_validators'])
#
# def calculate_epoch_rewards(total_stake):
#     annual_inflation_rate = 0.045  # 4.5%
#     daily_inflation_rate = annual_inflation_rate / 365
#     epoch_inflation_rate = daily_inflation_rate / 2  # Two epochs per day
#     epoch_rewards = total_stake * epoch_inflation_rate
#     return convert_yocto_to_near(epoch_rewards)
#
# def print_results(total_stake, active_validators):
#     near_amount = convert_yocto_to_near(total_stake)
#     print(f"Total staked amount: {near_amount:.2f} NEAR")
#     print(f"Number of active validators: {active_validators}")
#
# def print_epoch_rewards(rewards):
#     print(f"Estimated staking rewards for current epoch: {rewards:.2f} NEAR")
#
# def main():
#     validator_data = fetch_validator_data()
#     total_stake = sum_total_staked(validator_data)
#     active_validators = count_active_validators(validator_data)
#     print_results(total_stake, active_validators)
#     epoch_rewards = calculate_epoch_rewards(total_stake)
#     print_epoch_rewards(epoch_rewards)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        send_slack_alert(f"Error collecting NEAR rewards: {e}")