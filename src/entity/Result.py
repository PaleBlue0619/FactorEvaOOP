import pandas as pd
import dolphindb as ddb
from typing import Dict, List
from src.entity.Source import Source

class Result(Source):
    def __init__(self, session: ddb.session):
        super().__init__(session)
        self.startDate: pd.Timestamp = None
        self.endDate: pd.Timestamp = None
        self.dailyFreq: bool = True
        self.callBackPeriod: int = 1
        self.returnIntervals: List[int] = []
        self.quantile: int = 5
        self.dailyPnlLimit: float = 0.1
        self.useMinFreqPeriod: bool = False
        self.barRetLabelName: str = ""
        self.futRetLabelNames: List[str] = []

    def setConfig(self, config: Dict):
        """初始化结果配置项"""
        self.startDate = pd.Timestamp(config["start_date"]) if config["start_date"] is not None else pd.Timestamp("20200101")
        self.endDate = pd.Timestamp(config["end_date"]) if config["end_date"] is not None else pd.Timestamp.now().date()
        self.dailyFreq = config["dailyFreq"]
        self.callBackPeriod = int(config["callBackPeriod"])
        self.returnIntervals = [int(i) for i in config["returnIntervals"]]
        self.quantile = int(config["quantile"])
        self.dailyPnlLimit = float(config["dailyPnlLimit"])
        self.useMinFreqPeriod = config["useMinFreqPeriod"]
        self.barRetLabelName = config["barRetLabelName"]
        self.futRetLabelNames = config["futRetLabelNames"]

    def initResDB(self, dropDB: bool = False):
        """
        创建结果数据库
        """
        if dropDB and self.session.existsDatabase(self.resultDBName):
            self.session.dropDatabase(self.resultDBName)
        if not self.session.existsTable(dbUrl=self.resultDBName, tableName=self.resultTBName_Qua):
            colName = ["factor","returnInterval","period"]+["quantileReturn"+str(i) for i in range(1, self.quantile+1)]+["tradeTime"]
            colType = ["SYMBOL","INT","INT"]+["DOUBLE"]*self.quantile+["TIMESTAMP"]
            self.session.run(f"""
            db=database("{self.resultDBName}",RANGE,2010.01M+(0..30)*12,engine="OLAP")
            schemaTb=table(1:0,{colName}, {colType});
            t=db.createDimensionTable(table=schemaTb, tableName="{self.resultTBName_Qua}")
            """)    # DolphinDB 维度表 - 分层回测
        if not self.session.existsTable(dbUrl=self.resultDBName, tableName=self.resultDBName_Reg):
            colName = ["factor","returnInterval","period","indicator","value","tradeTime"]
            colType = ["SYMBOL","INT","INT","SYMBOL","DOUBLE","TIMESTAMP"]
            self.session.run(f"""
                db=database("{self.resultDBName}");
                schemaTb=table(1:0,{colName},{colType});
                t=db.createDimensionTable(table=schemaTb,tableName="{self.resultDBName_Reg}")
            """)