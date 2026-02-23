import numpy as np
import pandas as pd
import dolphindb as ddb
from typing import List, Dict

class Source:
    def __init__(self, session: ddb.session):
        self.session: ddb.session = session
        self.factorDateCol: str = ""
        self.labelDateCol: str = ""
        self.factorSymbolCol: str = ""
        self.labelSymbolCol: str = ""
        self.factorDBName: str = ""
        self.labelDBName: str = ""
        self.factorTBName: str = ""
        self.labelTBName: str = ""
        self.factorIndicatorCol: str = ""
        self.labelIndicatorCol: str = ""
        self.factorSymbolCol: str = ""
        self.labelSymbolCol: str = ""
        self.factorValueCol: str = ""
        self.labelValueCol: str = ""
        self.factorCondition: str = ""
        self.labelCondition: str = ""
        self.dataDateCol: str = "tradeDate"
        self.dataSymbolCol: str = "symbol"
        self.resultDBName: str = ""
        self.resultTBName_IC: str = ""
        self.resultTBName_QU: str = ""

    def init(self, factorDict: Dict[str, str], labelDict: Dict[str, str], resultDict: Dict[str, str]):
        self.factorDBName = factorDict["dbName"]
        self.factorTBName = factorDict["tbName"]
        self.factorDateCol = factorDict["dateCol"]
        self.factorSymbolCol = factorDict["symbolCol"]
        self.factorIndicatorCol = factorDict["indicatorCol"]
        self.factorValueCol = factorDict["valueCol"]
        self.factorCondition = factorDict["condition"]
        self.labelDBName = labelDict["dbName"]
        self.labelTBName = labelDict["tbName"]
        self.labelDateCol = labelDict["dateCol"]
        self.labelSymbolCol = labelDict["symbolCol"]
        self.labelIndicatorCol = labelDict["indicatorCol"]
        self.labelValueCol = labelDict["valueCol"]
        self.labelCondition = labelDict["condition"]
        self.resultDBName = resultDict["dbName"]
        self.resultTBName_IC = resultDict["icTbName"]   # IC结果表
        self.resultTBName_QU = resultDict["quTbName"]   # 分层回测(Quantile BackTest 结果表)

    def getFactorList(self) -> List[str]:
        """
        获取当前库内所有因子列表
        """
        if self.factorCondition not in ["", None]:
            factorDF = self.session.run(f"""
                select count(*) from loadTable("{self.factorDBName}", "{self.factorTBName}")
                    where {self.factorCondition}
                    group by {self.factorIndicatorCol} as factorName 
            """)
        else:
            factorDF = self.session.run(f"""
                select count(*) from loadTable("{self.factorDBName}", "{self.factorTBName}")
                    group by {self.factorIndicatorCol} as factorName
            """)
        factorList = factorDF["factorName"].tolist()
        return factorList

    def checkFactorList(self, factorList: List[str]) -> List[str]:
        """
        确认输入的因子列表是否都在库内->返回在库内的因子列表
        """
        return [i for i in factorList if i in self.getFactorList()]

    def getData(self, startDate: pd.Timestamp = None,
                endDate: pd.Timestamp = None,
                symbolList: List[str] = None,
                labelList: List[str] = None,
                factorList: List[str] = None
                ) -> pd.DataFrame:
        """获取完整的数据集 -> startDate & endDate
        通过LabelSource进行获取
        """
        realStartDate = pd.Timestamp(startDate).strftime("%Y.%m.%d")
        realEndDate = pd.Timestamp(endDate).strftime("%Y.%m.%d")
        if symbolList is None:
            symbolList = []
        self.session.upload({"symbolList": symbolList})
        if labelList is None:
            labelList = []
        self.session.upload({"labelList": labelList})
        if factorList is None:
            factorList = []
        self.session.upload({"factorList": factorList})
        data = self.session.run(f"""
            startDate = {realStartDate}
            endDate = {realEndDate}            
            /* 标签内存表 */
            if (size(symbolList)==0 and size(labelList)==0){{
                labelDF = select value from loadTable("{self.labelDBName}","{self.labelTBName}") 
                where {self.labelDateCol} between startDate and endDate and ({self.labelCondition})
                pivot by {self.labelSymbolCol} as {self.dataSymbolCol}, {self.labelDateCol} as {self.dataDateCol}, {self.labelIndicatorCol}
            }}
            else if(size(symbolList)>0 and size(labelList)==0){{
                labelDF = select value from loadTable("{self.labelDBName}","{self.labelTBName}") 
                where ({self.labelDateCol} between startDate and endDate) and {self.labelSymbolCol} in symbolList and ({self.labelCondition})
                pivot by {self.labelSymbolCol} as {self.dataSymbolCol}, {self.labelDateCol} as {self.dataDateCol}, {self.labelIndicatorCol}
            }}
            else if(size(symbolList)==0 and size(labelList)>0){{
                labelDF = select value from loadTable("{self.labelDBName}","{self.labelTBName}") 
                where ({self.labelDateCol} between startDate and endDate) and {self.labelIndicatorCol} in labelList and ({self.labelCondition})
                pivot by {self.labelSymbolCol} as {self.dataSymbolCol}, {self.labelDateCol} as {self.dataDateCol}, {self.labelIndicatorCol}
            }}
            else{{
                labelDF = select value from loadTable("{self.labelDBName}","{self.labelTBName}") 
                where ({self.labelDateCol} between startDate and endDate) and ({self.labelSymbolCol} in symbolList) and ({self.labelIndicatorCol} in labelList) and ({self.labelCondition}) 
                pivot by {self.labelSymbolCol} as {self.dataSymbolCol}, {self.labelDateCol} as {self.dataDateCol}, {self.labelIndicatorCol}
            }}

            /* 因子内存表 */
            if (size(symbolList)==0 and size(factorList)==0){{
                factorDF = select value from loadTable("{self.factorDBName}","{self.factorTBName}") 
                where {self.factorDateCol} between startDate and endDate and ({self.factorCondition})
                pivot by {self.factorSymbolCol} as {self.dataSymbolCol}, {self.factorDateCol} as {self.dataDateCol}, {self.factorIndicatorCol}
            }}
            else if(size(symbolList)>0 and size(factorList)==0){{
                factorDF = select value from loadTable("{self.factorDBName}","{self.factorTBName}") 
                where ({self.factorDateCol} between startDate and endDate) and {self.factorSymbolCol} in symbolList and ({self.factorCondition})
                pivot by {self.factorSymbolCol} as {self.dataSymbolCol}, {self.factorDateCol} as {self.dataDateCol}, {self.factorIndicatorCol}
            }}
            else if(size(symbolList)==0 and size(factorList)>0){{
                factorDF = select value from loadTable("{self.factorDBName}","{self.factorTBName}") 
                where ({self.factorDateCol} between startDate and endDate) and {self.factorIndicatorCol} in factorList and ({self.factorCondition})
                pivot by {self.factorSymbolCol} as {self.dataSymbolCol}, {self.factorDateCol} as {self.dataDateCol}, {self.factorIndicatorCol}
            }}
            else{{
                factorDF = select value from loadTable("{self.factorDBName}","{self.factorTBName}") 
                where ({self.factorDateCol} between startDate and endDate) and ({self.factorSymbolCol} in symbolList) and ({self.factorIndicatorCol} in factorList) and ({self.factorCondition})
                pivot by {self.factorSymbolCol} as {self.dataSymbolCol}, {self.factorDateCol} as {self.dataDateCol}, {self.factorIndicatorCol}
            }}

            /* 进行合并 */
            matchingCols = ["{self.dataSymbolCol}", "{self.dataDateCol}"]
            labelDF = select * from lj(labelDF, factorDF, matchingCols);

            /* 清理内存并返回结果 */
            undef(`factorDF)
            labelDF;
        """.replace("and ()", ""))
        return data
