import pandas as pd
import dolphindb as ddb
import streamlit as st
from functools import lru_cache
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
        self.startDate = pd.Timestamp(config["startDate"]) if config["startDate"] is not None else pd.Timestamp("20200101")
        self.endDate = pd.Timestamp(config["endDate"]) if config["endDate"] is not None else pd.Timestamp.now().date()
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
        if not self.session.existsTable(dbUrl=self.resultDBName, tableName=self.resultTBName_Reg):
            colName = ["factor","returnInterval","period","indicator","value","tradeTime"]
            colType = ["SYMBOL","INT","INT","SYMBOL","DOUBLE","TIMESTAMP"]
            self.session.run(f"""
                db=database("{self.resultDBName}");
                schemaTb=table(1:0,{colName},{colType});
                t=db.createDimensionTable(table=schemaTb,tableName="{self.resultTBName_Reg}")
            """)

class Stats(Result):    # for EvaPlot
    def __init__(self, session: ddb.session):
        super().__init__(session)

    @lru_cache(maxsize=128)
    def get_summaryData(self, rInterval: int) -> Dict[str, pd.DataFrame]:
        resDict = self.session.run(rf"""
        /* 取数 */
        pt = select ReturnInterval,period,factor,indicator,value,tradeTime 
            from loadTable("{self.resultDBName}","{self.resultTBName_Reg}") 
            where ReturnInterval == int({rInterval}) and indicator in ["IC","RankIC"]
        update pt set yearInt = year(tradeTime);
        update pt set yearStr = "Year"+string(yearInt)
        year_list = sort(exec distinct(yearInt) from pt)

        /* avg(IC) */
        TotalIC_pt = select avg(value) as Total from pt where indicator == "IC" group by factor
        sortBy!(TotalIC_pt,`factor)
        YearIC_pt = select avg(value) as value from pt where indicator == "IC" pivot by factor, yearStr
        YearIC_pt = sql(select=[sqlCol(`factor)].append!(sqlCol("Year"+string(year_list))), from=YearIC_pt).eval()
        TotalIC_pt = lj(TotalIC_pt, YearIC_pt, `factor)

        /* avg(RankIC) */
        TotalRankIC_pt = select avg(value) as Total from pt where indicator == "RankIC" group by factor 
        sortBy!(TotalRankIC_pt,`factor)
        YearRankIC_pt = select avg(value) as value from pt where indicator == "RankIC" pivot by factor, yearStr
        YearRankIC_pt = sql(select=[sqlCol(`factor)].append!(sqlCol("Year"+string(year_list))), from=YearRankIC_pt).eval()
        TotalRankIC_pt = lj(TotalRankIC_pt, YearRankIC_pt, `factor)

        /* avg(IC)\std(IC) */
        TotalICIR_pt = select avg(value)\std(value) as Total from pt where indicator == "IC" group by factor
        sortBy!(TotalICIR_pt,`factor)
        YearICIR_pt = select avg(value)\std(value) as value from pt where indicator == "IC" pivot by factor, yearStr
        YearICIR_pt = sql(select=[sqlCol(`factor)].append!(sqlCol("Year"+string(year_list))), from=YearICIR_pt).eval()
        TotalICIR_pt = lj(TotalICIR_pt, YearICIR_pt, `factor)

        /* avg(RankIC)\std(RankIC) */
        TotalRankICIR_pt = select avg(value)\std(value) as Total from pt where indicator == "RankIC" group by factor
        sortBy!(TotalRankICIR_pt,`factor)
        YearRankICIR_pt = select avg(value)\std(value) as value from pt where indicator == "RankIC" pivot by factor, yearStr
        YearRankICIR_pt = sql(select=[sqlCol(`factor)].append!(sqlCol("Year"+string(year_list))), from=YearRankICIR_pt).eval()
        TotalRankICIR_pt = lj(TotalRankICIR_pt, YearRankICIR_pt, `factor)

        /* 返回结果 */
        res_dict = dict(["TotalIC","TotalRankIC","TotalICIR","TotalRankICIR"], 
                        [TotalIC_pt,TotalRankIC_pt,TotalICIR_pt,TotalRankICIR_pt])
        res_dict
        """)
        return resDict

    def summaryPlot_(self) -> None:
        """
        所有因子横向比较可视化
        including: avg(IC), avg(RankIC), ICIR
        """
        rInterval = st.selectbox(
            label="请输入未来收益率区间长度",
            options=(i for i in self.returnIntervals),
            index=0,
            format_func=str,
            help='即ReturnModel中的returnIntervals'
        )
        st.title("_Total Factor Performance Comparison_")
        Dict = self.get_summaryData(rInterval=rInterval)
        TotalIC_df = Dict["TotalIC"]
        TotalRankIC_df = Dict["TotalRankIC"]
        TotalICIR_df = Dict["TotalICIR"]
        TotalRankICIR_df = Dict["TotalRankICIR"]
        st.subheader("All Factors' avg(IC)", divider=True)
        st.dataframe(data=TotalIC_df, height=1000)
        st.subheader("All Factors' avg(RankIC)", divider=True)
        st.dataframe(data=TotalRankIC_df, height=1000)
        st.subheader("All Factors' ICIR", divider=True)
        st.dataframe(data=TotalICIR_df, height=1000)
        st.subheader("All Factors' RankICIR", divider=True)
        st.dataframe(data=TotalRankICIR_df, height=1000)

    @lru_cache(maxsize=128)
    def get_factorData(self, factor: str, rInterval: int) -> Dict[str, pd.DataFrame]:
        resDict = self.session.run(rf"""
            pt=select * from loadTable("{self.resultDBName}","{self.resultTBName_Reg}") 
                where factor == "{factor}" and ReturnInterval=={rInterval}
            quantile_pt=select * from loadTable("{self.resultDBName}","{self.resultTBName_Qua}") 
                where factor == "{factor}";

            /* 因子收益率&累计因子收益率 */
            R=select value from pt where indicator ="R_OLS" pivot by tradeTime,indicator;
            R_cumsum=R.copy();
            L=R_cumsum["tradeTime"];
            dropColumns!(R_cumsum,`tradeTime);
            R_cumsum=cumsum(R_cumsum);
            R_cumsum=select L as tradeTime,* from R_cumsum;

            // Reg_stat
            Obs=select value from pt where indicator == "Obs" pivot by tradeTime,indicator;
            Std_Error=select value from pt where indicator == "Std_Error" pivot by tradeTime,indicator;  // 残差标准差
            R_square=select value from pt where indicator == "R_square" pivot by tradeTime,indicator;
            Adj_square=select value from pt where indicator == "Adj_square" pivot by tradeTime,indicator;

            // Tstat
            t_stat = select value from pt where indicator == "R_tstat" pivot by tradeTime,indicator;
            // alpha_tStat = select value from pt where indicator == "Alpha_tstat" pivot by tradeTime,indicator;

            // IC & 累计IC
            IC=select value from pt where indicator="IC" pivot by tradeTime,indicator;
            IC_cumsum=IC.copy();
            L=IC_cumsum["tradeTime"];
            dropColumns!(IC_cumsum,`tradeTime);
            IC_cumsum=cumsum(IC_cumsum);
            IC_cumsum=select L as tradeTime,* from IC_cumsum;

            // RankIC & 累计RankIC
            RankIC=select value from pt where indicator="RankIC" pivot by tradeTime,indicator;
            RankIC_cumsum=RankIC.copy();
            L=RankIC_cumsum["tradeTime"];
            dropColumns!(RankIC_cumsum,`tradeTime);
            RankIC_cumsum=cumsum(RankIC_cumsum);
            RankIC_cumsum=select L as tradeTime,* from RankIC_cumsum;

            // Yearly avg(IC)&IR
            data=unpivot(IC,keyColNames="tradeTime",valueColNames=columnNames(IC)[1:])
            rename!(data,`tradeTime`factor`factor_IC);
            avg_IC=select avg(factor_IC) from data pivot by year(tradeTime) as year,factor;
            IR=select avg(factor_IC)/std(factor_IC) from data pivot by year(tradeTime) as year,factor;

            // Yearly avg(RankIC)&RankIR
            data=unpivot(RankIC,keyColNames="tradeTime",valueColNames=columnNames(RankIC)[1:])
            rename!(data,`tradeTime`factor`factor_RankIC);
            avg_RankIC=select avg(factor_RankIC) from data pivot by year(tradeTime) as year,factor;
            RankIR=select avg(factor_RankIC)/std(factor_RankIC) from data pivot by year(tradeTime) as year,factor;

            // 返回为字典格式
            Dict=dict(["R_square","Adj_square","Obs","Std_Error","R","R_cumsum","t_stat",
                        "IC","IC_cumsum","RankIC","RankIC_cumsum","avg_IC","IR","avg_RankIC","RankIR"],
                        [R_square,Adj_square,Obs,Std_Error,R,R_cumsum,t_stat,
                        IC,IC_cumsum,RankIC,RankIC_cumsum,avg_IC,IR,avg_RankIC,RankIR]);

            /* Quantile Return & Quantile Cumsum Return */
            returnIntervals = {self.returnIntervals}
            for (r_interval in returnIntervals){{  // 这里只统计累计值(cumsum)
                df = sql(select=[sqlCol(`TradeTime)].append!(sqlCol("QuantileReturn"+string(1..{self.quantile}))),
                        from=quantile_pt, where=<ReturnInterval == r_interval>).eval()
                ts_list = df[`tradeTime];
                dropColumns!(df,`tradeTime);
                df = cumsum(df) + 1
                Dict["Return"+string(r_interval)] = select ts_list as `tradeTime, * from df
            }}
            undef(`pt); // 清除缓存
            Dict
            """)
        return resDict

    def factorPlot_(self, factorList: List[str]) -> None:
        """单因子评价可视化"""
        rInterval = st.selectbox(
            label="请输入未来收益率区间长度",
            options=(i for i in self.returnIntervals),
            index=0,
            format_func=str,
            help='即ReturnModel中的returnIntervals'
        )
        factor = st.selectbox(
            label="请选择因子",
            options=factorList,
            index=0,
            format_func=str,
            help="选择当前因子进行因子分层收益展示"
        )
        st.title("_Single Factor BackTest Analysis_")
        tabReg, tabIC, tabQuantile, tabStats = st.tabs(["回归法", "IC法", "分层回测", "其他指标"])
        Dict = self.get_factorData(factor=factor, rInterval=rInterval)
        R_square = Dict["R_square"]
        Adj_square = Dict["Adj_square"]
        Obs = Dict["Obs"]
        Std_Error = Dict["Std_Error"]
        R = Dict["R"]
        R_cumsum = Dict["R_cumsum"]
        t_stat = Dict["t_stat"]
        # IC=Dict["IC"]
        IC_cumsum = Dict["IC_cumsum"]
        # RankIC=Dict["RankIC"]
        RankIC_cumsum = Dict["RankIC_cumsum"]
        avg_IC = Dict["avg_IC"]
        IR = Dict["IR"]
        avg_RankIC = Dict["avg_RankIC"]
        RankIR = Dict["RankIR"]
        with tabReg:
            st.subheader("Single Factor Return", divider=True)
            st.line_chart(data=R, x="tradeTime", y=None)
            st.subheader("Single Factor Return(cumsum)", divider=True)
            st.line_chart(data=R_cumsum, x="tradeTime", y=None)
            st.subheader("Factor Tstat", divider=True)
            st.bar_chart(data=t_stat, x="tradeTime", y=None, stack=False)
            st.write("T值绝对值大于等于2的比例")
            t_stat = t_stat.set_index("tradeTime")
            t_stat = (t_stat.abs() >= 2).mean()  # .mean()计算|T|≥2的比例
            st.dataframe(data=t_stat)
        with tabIC:
            # st.subheader("Factor IC",divider=True)
            # st.bar_chart(data=IC,x="tradeTime",y=None,stack=False)
            # st.subheader("Factor RankIC",divider=True)
            # st.bar_chart(data=RankIC,x="tradeTime",y=None,stack=False)
            st.subheader("Factor IC(cumsum)", divider=True)
            st.line_chart(data=IC_cumsum, x="tradeTime", y=None)
            st.subheader("Factor RankIC(cumsum)", divider=True)
            st.line_chart(data=RankIC_cumsum, x="tradeTime", y=None)
            st.subheader("Factor avg(IC)", divider=True)
            st.bar_chart(data=avg_IC, x="year", y=None, stack=False)
            st.dataframe(data=avg_IC)
            st.write("Total avg(IC):")
            st.dataframe(data=avg_IC.set_index("year").mean())
            st.subheader("Factor IR", divider=True)
            st.bar_chart(data=IR, x="year", y=None, stack=False)
            st.dataframe(data=IR)
            st.subheader("Factor avg(RankIC)", divider=True)
            st.bar_chart(data=avg_RankIC, x="year", y=None, stack=False)
            st.dataframe(data=avg_RankIC)
            st.write("Total avg(RankIC):")
            st.dataframe(data=avg_RankIC.set_index("year").mean())
            st.subheader("Factor RankIR", divider=True)
            st.bar_chart(data=RankIR, x="year", y=None, stack=False)
            st.dataframe(data=RankIR)
        with tabQuantile:
            for rInterval in self.returnIntervals:
                st.subheader(f"Single Factor Quantile Return(ReturnInterval={rInterval})", divider=True)
                st.line_chart(data=Dict["Return" + str(rInterval)], x="tradeTime", y=None)
        with tabReg:
            st.subheader("R square", divider=True)
            st.bar_chart(data=R_square, x="tradeTime", y=None, stack=False)
            st.subheader("Adj R suqare", divider=True)
            st.bar_chart(data=Adj_square, x="tradeTime", y=None, stack=False)
            st.subheader("Std Error(残差标准差)", divider=True)
            st.bar_chart(data=Std_Error, x="tradeTime", y=None, stack=False)
            st.subheader("Num of Obs", divider=True)
            st.line_chart(data=Obs, x="tradeTime", y=None)