import pandas as pd
import dolphindb as ddb
from typing import Dict, List
from src.entity.Result import Result

class Eva(Result):
    def __init__(self, session: ddb.session):
        super().__init__(session)

    def initDef(self):
        """初始化定义"""
        self.session.run(rf"""
        def InsertData(DBName, TBName, data, batchsize){{
            // 预防Out of Memory，分批插入数据，batchsize为每次数据的记录数
            start_idx = 0
            end_idx = batchsize
            krow = rows(data)
            do{{ 
                slice_data = data[start_idx:min(end_idx,krow),]
                if (rows(slice_data)>0){{
                loadTable(DBName, TBName).append!(slice_data);
                print(start_idx);
                }}
                start_idx = start_idx + batchsize
                end_idx = end_idx + batchsize
            }}while(start_idx < krow)
        }};

        def RegStats(df, factor_list, ReturnInterval, callBackPeriod, currentPeriod){{
            /* ICIR & 回归法统计函数 */
            // 统计函数(peach并行内部)
            if (callBackPeriod != 1){{
                data = select * from df where currentPeriod-callBackPeriod < period <= currentPeriod;
            }}else{{
                data = select * from df where period = currentPeriod;
            }};
            rename!(data,`Ret+string(ReturnInterval), `Ret);
            counter = 0;
            for (factorName in factor_list){{
                data[`factor] = double(data[factorName])  // 为了避免数据类型发生变化
                // 有可能因子存在大量空值/Inf/返回一样的值, 使得回归统计量报错
                if (countNanInf(double(data[`factor]),true)<size(data[`factor])*0.9 and all(data[`factor]==data[`factor][0])==0){{
                    // OLS回归统计量(因子收益率/T值/R方/调整后的R方/StdError/样本数量)
                    result_OLS=ols(data[`Ret], data[`factor], intercept=true, mode=2); 
                    if (not isVoid(result_OLS[`RegressionStat])){{
                        beta_df = select factor as indicator, beta as value from result_OLS[`Coefficient];
                        beta_df[`indicator] = ["Alpha_OLS","R_OLS"] // 截距项/回归系数
                        tstat_df= select factor as indicator, tstat as value from result_OLS[`Coefficient];
                        tstat_df[`indicator] = ["Alpha_tstat", "R_tstat"] // 截距项T值/回归系数T值
                        RegDict=dict(result_OLS[`RegressionStat][`item], result_OLS[`RegressionStat][`statistics]); 
                        R_square=RegDict[`R2];
                        Adj_square=RegDict[`AdjustedR2];
                        Std_error=RegDict[`StdError];
                        Obs=RegDict['Observations'];

                        // IC统计量
                        IC_df = select `IC as indicator, corr(zscore(factor), zscore(Ret)) as value from data
                        RankIC_df = select `RankIC as indicator, spearmanr(factor, Ret) as value from data

                        // 合并结果
                        summary_result=table([`R_square,`Adj_square,`Std_Error,`Obs] as `indicator, 
                                [R_square, Adj_square, Std_error, Obs] as `value);
                        summary_result.append!(beta_df)
                        summary_result.append!(tstat_df)
                        summary_result.append!(IC_df)
                        summary_result.append!(RankIC_df)
                        if (counter == 0){{
                            res = select factorName as factor, ReturnInterval as returnInterval, 
                                    currentPeriod as period, indicator, value from summary_result;
                        }}else{{
                            res.append!(select factorName as factor, ReturnInterval as returnInterval, 
                                    currentPeriod as period, indicator, value from summary_result);
                        }};
                        counter += 1
                    }};
                }}:
                dropColumns!(data,`factor)
            }};
            if (counter>0){{
                return res 
            }}
        }}

        def QuantileStats(df, idCol, factor_list, ReturnInterval, quantiles, currentPeriod){{
            // 分层统计函数
            // 统计函数(peach并行内部)
            data = select * from df where period == currentPeriod;
            // 按照这一个时刻数据(quantile_df)的因子值进行分组
            quantile_df = select * from df where quantilePeriod == currentPeriod-currentPeriod%ReturnInterval
            quantile_df[`id] = quantile_df[idCol]
            quantile_df = select * from quantile_df context by id limit 1; // 取第一个因子值进行分组
            bins = (1..(quantiles-1)*(1.0\quantiles)); // quantile bins

            counter = 0
            for (factorName in factor_list){{
                // 分层测试
                quantileFunc = quantile{{quantile_df[factorName],,"midpoint"}}; // 函数部分化应用
                split = each(quantileFunc, bins); // 按照阈值得到分割点
                quantile_df[`Quantile] = 1+digitize(quantile_df[factorName], split, right=true);
                quantile_dict = dict(quantile_df[`id], quantile_df[`Quantile])  // 当前因子的分组情况
                data[`Quantile] = quantile_dict[data[idCol]]
                quantile_return = select factorName as factor, nullFill(avg(period_return),0.0) as value from data group by Quantile
                tab = select value from quantile_return pivot by factor, Quantile
                rename!(tab, [`factor].append!(`QuantileReturn+string(columnNames(tab)[1:])))
                quantile_list = `QuantileReturn+string(1..quantiles)
                for (col in quantile_list){{
                    if (not (col in columnNames(tab))){{
                        tab[col] = 0.0; // 说明没有当前分组的数据
                    }};
                }};        
                // 合并结果
                QuantileReturn_df = sql(select=[sqlCol(`factor)].append!(sqlCol(quantile_list)), from=tab).eval()
                if (counter == 0){{        
                    qes = sql(select=[sqlCol(`factor), sqlColAlias(<ReturnInterval>, `returnInterval), sqlColAlias(<currentPeriod>, `period)].append!(sqlCol(quantile_list)), from=QuantileReturn_df).eval()           
                }}else{{
                    qes.append!(sql(select=[sqlCol(`factor), sqlColAlias(<ReturnInterval>, `returnInterval), sqlColAlias(<currentPeriod>, `period)].append!(sqlCol(quantile_list)), from=QuantileReturn_df).eval())     
                }};
                counter += 1
            }};
            if (counter>0){{
                return qes // 返回分层回测结果 
            }}
        }}

        def SingleFactorAnalysis(df, factor_list, idCol, timeCol, barReturnCol, futureReturnCols, returnIntervals, dailyFreq, callBackPeriod=1, quantiles=5, 
            dailyPnlLimit=NULL, useMinFreqPeriod=true){{
            /*单因子测试, 输出一张窄表
            totalData: GPLearnProcessing输出的因子结果+行情数据
            factor_list: 单因子列表
            idCol: 标的列
            timeCol: 时间列
            barReturnCol: 1根Bar的区间收益率(For 分层回测法)
            futureReturnCols: 未来区间收益率列名list(For IC法&回归法)
            returnIntervals: 收益率计算间隔
            dailyFreq: 表示当前因子输入是否为日频, false表示输入分钟频因子回测
            callBackPeriod: 回看周期, 默认为1(即只使用当前period数据进行因子统计量计算)
            quantiles: 分组数量, 每个period中标的会根据当前因子的值从小到大分成quantiles个数的分组去统计分组收益率
            dailyPnlLimit: 当且仅当dailyFreq=true时生效, 表示日涨跌幅限制
            useMinFreqPeriod: 仅当分钟频因子评价时有效, true表示计算因子统计量时按照分钟频聚合计算，false则按照日频聚合计算
            */
            totalData = df
            if (dailyFreq==true or (dailyFreq==false and useMinFreqPeriod==true)){{ // 分钟频->分钟频 & 日频->日频
                // for ICIR & 回归法, 使用原始时间频率生成period
                time_list = sort(distinct(totalData[timeCol]),true) // 分钟时间列/日时间列
                period_dict = dict(time_list, cumsum(take(1, size(time_list))))
                time_dict = dict(values(period_dict), keys(period_dict))
                totalData[`period] = period_dict[totalData[timeCol]]  // timeCol -> period
                period_list = values(period_dict) // 所有period组成的list

                // for 分层回测法，与回归法一致
                totalData[`quantilePeriod] = totalData[`period]
                qperiod_list = period_list
                qtime_dict = time_dict
            }}else{{ // 分钟频->日频
                // for 分层回测法, 依然使用原始分钟频生成period
                qtime_list = sort(distinct(totalData[timeCol]),true) // 分钟时间列
                qperiod_dict = dict(qtime_list, cumsum(take(1, size(qtime_list))))
                qtime_dict = dict(values(qperiod_dict), keys(qperiod_dict))
                totalData[`quantilePeriod] = qperiod_dict[totalData[timeCol]]  // timeCol -> qperiod
                qperiod_list = values(qperiod_dict)

                // for ICIR & 回归法, 生成日频period
                time_list = sort(distinct(sql(select=sqlColAlias(makeCall(date, sqlCol(timeCol)),"time"), from=totalData).eval()["time"]), true) // 日期时间列
                period_dict = dict(time_list, cumsum(take(1, size(time_list))))
                time_dict = dict(values(period_dict), keys(period_dict))
                totalData[`period] = period_dict[date(totalData[timeCol])]  // timeCol -> period
                period_list = values(period_dict) // 所有period组成的list
            }};

            // 计算不同周期的收益率(这里已经提前计算好了然后把列名传进来了)
            for (i in 0..(size(futureReturnCols)-1)){{
                rename!(totalData, futureReturnCols[i], `Ret+string(returnIntervals[i]))
            }}
            returnCol = `Ret+string(returnIntervals);
            rename!(totalData, barReturnCol, `period_return);

            // 分层回测 \ ICIR法&回归法
            sortBy!(totalData, timeCol, 1)            
            if (dailyPnlLimit!=NULL and dailyFreq==true){{
                update totalData set period_return = clip(period_return, -dailyPnlLimit, dailyPnlLimit)
            }}
            colList = returnCol.copy().append!(idCol).append!(timeCol).append!([`period,`quantilePeriod]).append!(factor_list)
            regData = sql(sqlCol(colList),from=totalData).eval()  // for ICIR法 & 回归法
            quantileData = sql(sqlCol(colList).append!(sqlCol(`period_return)), from=totalData).eval() // for 分层回测法
            counter = 0
            for (interval in returnIntervals){{
                print("processing ReturnInterval:"+string(interval))

                // 分层回测
                print("Start Quantile BackTesting...")
                QuantileFunc = QuantileStats{{quantileData, idCol, factor_list, interval, quantiles, }} // DolphinDB函数部分化应用
                qes = peach(QuantileFunc, qperiod_list).unionAll(false)
                print("End Quantile BackTesting...")

                // ICIR法&回归法
                RegStatsFunc = RegStats{{regData, factor_list, interval, callBackPeriod, }}; // DolphinDB函数部分化应用
                res = peach(RegStatsFunc, period_list).unionAll(false)

                if (counter == 0){{
                    summary_res = res
                    quantile_res = qes
                }}else{{
                    summary_res.append!(res)
                    quantile_res.append!(qes)
                }}
                counter += 1
            }}
            print("SingleFactor Evaluation End")
            summary_res[`TradeTime] = time_dict[summary_res[`period]]     // 添加时间
            quantile_res[`TradeTime] = qtime_dict[quantile_res[`period]]
            sortBy!(summary_res,[`TradeTime,`factor,`period],[1,1,1])
            sortBy!(quantile_res,[`TradeTime,`factor,`period],[1,1,1])
            return summary_res, quantile_res
        }}
        """)

    def eva(self, factorList: List[str]):
        """运行评价"""
        self.session.upload({"factorList": factorList})
        self.session.run(rf"""
        // 配置项
        idCol = "{self.dataSymbolCol}";
        timeCol = "{self.dataDateCol}"; // 这里由于后续就算引入分钟频，也是降频为日频因子，就直接写死为日期列
        barReturnCol = "{self.barRetLabelName}";
        futureReturnCols = {self.futRetLabelNames};
        callBackPeriod = {int(self.callBackPeriod)};
        quantiles = {int(self.quantile)};
        returnIntervals = {self.returnIntervals};
        if ({int(self.dailyPnlLimit is not None)}==1){{
            dailyPnlLimit = {self.dailyPnlLimit};
        }}else{{
            dailyPnlLimit = NULL;
        }}
        if ({int(self.dailyFreq)}==1){{
            dailyFreq = true;
        }}else{{
            dailyFreq = false;
        }}
        if ({int(self.useMinFreqPeriod)}==1){{
            useMinFreqPeriod = true;
        }}else{{
            useMinFreqPeriod = false;
        }}
        
        // 获取数据
        pt = select * from {self.dataObjName} order by {self.dataSymbolCol},{self.dataDateCol};
        
        // 执行单因子评价
        summary_res, quantile_res = SingleFactorAnalysis(pt, factorList, idCol, timeCol, barReturnCol, futureReturnCols,
         returnIntervals, dailyFreq, callBackPeriod=callBackPeriod, quantiles=quantiles, 
            dailyPnlLimit=dailyPnlLimit, useMinFreqPeriod=useMinFreqPeriod)
        
        // 插入至数据库
        InsertData(DBName="{self.resultDBName}", TBName="{self.resultTBName_Reg}", 
                            data=summary_res, batchsize=1000000);
        print("IC法&回归法结果插入完毕")
        InsertData(DBName="{self.resultDBName}", TBName="{self.resultTBName_Qua}", 
                            data=quantile_res, batchsize=1000000);
        print("分层回测法结果插入完毕")
        undef(`summary_res`quantile_res`pt); // 释放内存
        """)