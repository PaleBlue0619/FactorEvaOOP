import os, json, json5
import pandas as pd
import dolphindb as ddb
from typing import List, Dict
from src.entity.Source import Source
from src.entity.Result import Result

class FactorEva(Result):
    def __init__(self, session: ddb.session, factorList: List[str]):
        super().__init__(session)
        if factorList:
            self.factorList = factorList
        else:
            self.factorList = self.getFactorList()

if __name__ == "__main__":
    session = ddb.session("localhost", 8848, "admin", "123456")
    with open(r".\cons\eva.json5", "r", encoding="utf-8") as f:
        evaCfg = json5.load(f)
    EvaObj = FactorEva(session, factorList=["Test_lightGBM"])
    EvaObj.init(factorDict=evaCfg["factor"],
                labelDict=evaCfg["label"],
                resultDict=evaCfg["result"])
    EvaObj.initResDB(dropDB=True)
