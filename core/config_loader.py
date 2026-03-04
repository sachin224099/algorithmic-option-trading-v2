import yaml


class Config:

    def __init__(self, path="config/config.yaml"):
        with open(path, "r") as f:
            self.data = yaml.safe_load(f)

    def get(self, *keys):
        ref = self.data
        for key in keys:
            ref = ref[key]
        return ref

    def get_api_credentials(self):
        return (
            self.get("api", "api_key"),
            self.get("api", "access_token")
        )
    
    def get_expiry_date(self):
        return self.get("strategy", "expiry_date")

    def get_timeframe_minutes(self):
        return self.get("strategy", "timeframe_minutes")
    
    def get_lookback_candles(self):
        return self.get("strategy", "lookback_candles")