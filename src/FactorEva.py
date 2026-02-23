import os, json, json5
import pandas as pd
import dolphindb as ddb
from src.entity.Source import Source

class FactorEva(Source):
    def __init__(self, startDate: pd.Timestamp, endDate: pd.Timestamp, session: ddb.session):
        super().__init__(session)
        self.startDate = startDate
        self.endDate = endDate

    def upload(self):
        """上传数据"""