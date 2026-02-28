import os, sys, json, json5, tqdm
import pandas as pd
import dolphindb as ddb
from typing import List, Dict
# Add project root to Python path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)
from src.entity.Source import Source
from src.entity.Result import Result, Stats
from src.entity.Eva import Eva
from src.utils.utils import split_list

class FactorEva(Eva, Stats):
    def __init__(self, session: ddb.session):
        super().__init__(session)

    @staticmethod
    def run(cfg: Dict[str, str], factorList: List[str], dropDB: bool = False):
        """
        运行评价函数
        """
        EvaObj = FactorEva(session)
        EvaObj.init(factorDict=cfg["factor"],
                    labelDict=cfg["label"],
                    resultDict=cfg["result"])
        EvaObj.initDef()
        if not factorList:
            factorList = EvaObj.getFactorList()
        factorList_nested = split_list(l=factorList, k=10)
        EvaObj.setConfig(config=cfg["config"])
        EvaObj.initResDB(dropDB=dropDB)
        for factorList in tqdm.tqdm(factorList_nested, desc="Evaluating..."):
            EvaObj.getData(startDate=EvaObj.startDate, endDate=EvaObj.endDate,
                           factorList=factorList, symbolList=None,
                           labelList=[EvaObj.barRetLabelName]+[EvaObj.futRetLabelNames])
            EvaObj.eva(factorList=factorList)

    @staticmethod
    def summaryPlot(cfg: Dict[str, str]):
        EvaObj = FactorEva(session)
        EvaObj.init(factorDict=cfg["factor"],
                    labelDict=cfg["label"],
                    resultDict=cfg["result"])
        EvaObj.setConfig(config=cfg["config"])
        EvaObj.summaryPlot_()

    @staticmethod
    def factorPlot(cfg: Dict[str, str], factorList: List[str]):
        EvaObj = FactorEva(session)
        EvaObj.init(factorDict=cfg["factor"],
                    labelDict=cfg["label"],
                    resultDict=cfg["result"])
        EvaObj.setConfig(config=cfg["config"])
        if not factorList:
            factorList = EvaObj.getFactorList()
        EvaObj.factorPlot_(factorList=factorList)

if __name__ == "__main__":
    session = ddb.session("localhost", 8848, "admin", "123456")
    with open(r"E:\Quant\FactorEva\src\cons\eva.json5", "r", encoding="utf-8") as f:
        evaCfg = json5.load(f)
    # FactorEva.run(cfg=evaCfg, factorList=None, dropDB=True)
    # FactorEva.factorPlot(cfg=evaCfg, factorList=None)
    FactorEva.summaryPlot(cfg=evaCfg)

