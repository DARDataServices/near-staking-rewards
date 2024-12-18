from datetime import datetime

import requests
class Near:
    def __init__(self):
        self.url = "https://rpc.mainnet.near.org"
        self.headers = {"Content-Type": "application/json"}

    def _fetch_validator_data(self):
        payload = {
            "jsonrpc": "2.0",
            "id": "dontcare",
            "method": "validators",
            "params": [None]
        }
        response = requests.post(self.url, headers=self.headers, json=payload)
        return response.json()

    def calculate_metrics(self):
        validator_data = self._fetch_validator_data()
        total_stake = sum(int(validator['stake']) for validator in validator_data['result']['current_validators'])
        active_validators = len(validator_data['result']['current_validators'])
        epoch_start_height = validator_data['result']['epoch_start_height']

        annual_inflation_rate = 0.045  # 4.5%
        daily_inflation_rate = annual_inflation_rate / 365
        epoch_inflation_rate = daily_inflation_rate / 2  # Two epochs per day
        epoch_rewards = total_stake * epoch_inflation_rate

        return {
            "epoch": epoch_start_height,
            "total_staked_near": total_stake / (10**24),
            "active_validators": active_validators,
            "epoch_rewards": epoch_rewards / (10**24),
            "timestamp": datetime.utcnow().isoformat()
        }