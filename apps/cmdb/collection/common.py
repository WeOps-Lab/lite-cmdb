import os

import requests
from dotenv import load_dotenv

load_dotenv()


class Collection:
    def __init__(self):
        self.url = os.getenv("COLLECTION_URL")

    def query(self, sql):
        """查询数据"""
        if "FORMAT JSON" not in sql.upper():
            sql = f"{sql} FORMAT JSON"
        resp = requests.post(self.url, data=sql)
        if resp.status_code != 200:
            raise Exception(f"request error！{resp.text}")
        return resp.json()
