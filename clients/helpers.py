import os
import requests
import pandas as pd
from datetime import datetime, timedelta
import boto3

def clean(data_from_bq):
    df = pd.DataFrame(data_from_bq)
    df['day_settled'] = (df['timestamp']).dt.date # Group rewards from same day

    grouped = df.groupby('day_settled').agg(epoch_rewards=('epoch_rewards', 'sum'), total_staked_near=('total_staked_near', 'sum')).reset_index()
    grouped.sort_values(by='day_settled', ascending=True, inplace=True)
    return grouped.apply(lambda row: format_row(row['day_settled'], row['epoch_rewards'], row['total_staked_near']), axis=1)

def format_row(date, current_rewards, current_staked):
    return pd.Series({
        'blockchain': 'NEAR',
        'darAssetID': 'DALYJ9J',
        'darAssetTicker': 'NEAR',
        'sedol': '???',         # TODO
        'periodType': 'daily',
        'rewardPeriodStartTime': (date - timedelta(days=1)).strftime('%Y-%m-%d 00:00:00'),
        'rewardPeriodEndTime': date.strftime('%Y-%m-%d 00:00:00'),
        'totalRewardQuantity': float(f"{float(current_rewards):.6f}"),
        'stakedQuantity': int(current_staked),
        'reserved1': None,
        'reserved2': None,
        'reserved3': None,
        'reserved4': None,
        'reserved5': None,
        'reserved6': None,
        'reserved7': None,
        'reserved8': None,
        'reserved9': None,
        'reserved10': None
    })


def upload_to_aws(data, bucket_name, aws_access_key_id, aws_secret_access_key, aws_region_name):
    df = pd.DataFrame(data)
    csv_data = df.to_csv(index=False)

    bucket_name, object_key = bucket_name.split('/', 1)
    object_key += f"DARNEARYieldFile_{datetime.utcnow().strftime('%Y%m%d')}.csv"

    boto3.Session(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=aws_region_name
    ).resource('s3').Object(bucket_name, object_key).put(Body=csv_data)

    return f"Data successfully written to {bucket_name}/{object_key}"

def send_slack_alert(message):
    slack_alert_webhook = os.environ.get('SLACK_ALERT_WEBHOOK')
    production_alarm_webhook = os.environ.get('PRODUCTION_ALARM_WEBHOOK')
    env = os.environ.get('ENV')

    print('Slack alert:', message)

    # Don't send unless running in prod
    if 'prod' not in env.lower():
        print("Skipping slack")
        return

    if 'error' in message.lower() or 'failed' in message.lower():
        payload = {
            "text": "Error in TON Staking Rewards File Delivery:  " + message,
            "mrkdwn": 'true'
        }
        payload = str(payload)
        requests.post(production_alarm_webhook, payload)

    payload = {
        "text": "TON Staking Rewards File Delivery:  " + message,
        "mrkdwn": 'true'
    }

    payload = str(payload)
    requests.post(slack_alert_webhook, payload)