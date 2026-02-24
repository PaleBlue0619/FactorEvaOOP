import os, json, json5
import pandas as pd
import dolphindb as ddb
from typing import List, Dict
from src.entity.Source import Source

class FactorEva(Source):
    def __init__(self, session: ddb.session, startDate: pd.Timestamp, endDate: pd.Timestamp, factorList: List[str]):
        super().__init__(session)
        self.startDate = startDate
        self.endDate = endDate
        if factorList:
            self.factorList = factorList
        else:
            self.factorList = self.getFactorList()

    def upload(self):
        """上传数据"""