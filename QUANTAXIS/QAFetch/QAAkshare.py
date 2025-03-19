# coding: utf-8
#
# The MIT License (MIT)
#
# Copyright (c) 2016-2021 yutiansut/QUANTAXIS
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import json
import pandas as pd
import time
import datetime
import akshare as ak
import requests
from QUANTAXIS.QAUtil import (
    QA_util_log_info,
    QA_util_to_json_from_pandas
)

def QA_fetch_get_swindex_day_1(code, start_date, end_date):
    """获取申万一级及二级行业历史行情
    """
    def fetch_swindex_day_1(code, start_date, end_date):
        sw_day_1 = None
        try:
            sw_day_1 = ak.index_level_one_hist_sw(code)
            sw_day_1['发布日期'] = sw_day_1['发布日期'].apply(lambda x: x.strftime('%Y-%m-%d'))
            sw_day_1 = sw_day_1.loc[(sw_day_1['发布日期'] >= start_date) & (sw_day_1['发布日期'] <= end_date),['发布日期', '指数代码', '开盘指数', '收盘指数', '最高指数', '最低指数', '成交量', '成交额', '换手率', '涨跌幅']]
            sw_day_1.columns=['date','code','open','close','high','low','vol','amount','up_count','down_count']
            #将vol和amount的单位亿换算成元
            sw_day_1['vol'] = sw_day_1['vol'] * 100000000
            sw_day_1['amount'] = sw_day_1['amount'] * 100000000
            #2023/7/4更新， date_stamp精确到日,不需要到时分秒
            # sw_day_1['date_stamp'] = int(round(datetime.datetime.now().timestamp()))
            sw_day_1['date_stamp'] = int(time.mktime(time.strptime( datetime.date.today().strftime("%Y-%m-%d"), '%Y-%m-%d')))
        except Exception as e:
            print(e)

        return sw_day_1

    return fetch_swindex_day_1(code, start_date, end_date)

def QA_fetch_get_swindex_day_2(code, start_date, end_date):
    """ 获取申万一级/二级/三级行业历史行情
    """
    def fetch_swindex_day_2(code, start_date, end_date):
        sw_day_2 = None
        try:
            sw_day_2 = ak.index_hist_sw(code)
            sw_day_2['日期'] = sw_day_2['日期'].apply(lambda x: x.strftime('%Y-%m-%d'))
            sw_day_2 = sw_day_2.loc[(sw_day_2['日期'] >= start_date) & (sw_day_2['日期'] <= end_date),['日期', '代码', '开盘', '收盘', '最高', '最低', '成交量', '成交额']]
            sw_day_2['up_count'] = ''
            sw_day_2['down_count'] = ''
            sw_day_2.columns=['date','code','open','close','high','low','vol','amount','up_count','down_count']
             #将vol和amount的单位亿换算成元
            sw_day_2['vol'] = sw_day_2['vol'] * 100000000
            sw_day_2['amount'] = sw_day_2['amount'] * 100000000
            #2023/7/4更新， date_stamp精确到日,不需要到时分秒
            # sw_day_2['date_stamp'] = int(round(datetime.datetime.now().timestamp()))
            # sw_day_2['date_stamp'] =int(time.mktime(time.strptime( datetime.date.today().strftime('%Y-%m-%d')), '%Y-%m-%d'))
            sw_day_2['date_stamp'] = sw_day_2['date'].apply(lambda x: int(time.mktime(time.strptime( x, '%Y-%m-%d'))))
        except Exception as e:
            print(e)

        return sw_day_2

    return fetch_swindex_day_2(code, start_date, end_date)

def QA_fetch_get_swindex_list():
    """获取申万指数列表
    """

    def fetch_swindex_list():
        index_info = None
        try:
            index_info_1 = ak.sw_index_first_info()
            index_info_1['decimal_point'] = 1
            index_info_2 = ak.sw_index_second_info()
            index_info_2['decimal_point'] = 2
            index_info_3 = ak.sw_index_third_info()
            index_info_3['decimal_point'] = 3
            index_info = pd.concat([
                index_info_1,
                index_info_2,
                index_info_3
                ])
            index_info = index_info.drop_duplicates()
            index_info['sec'] = 'index_cn'
            index_info['sse'] = 'sw'
            index_info = index_info.loc[:,['行业代码','成份个数','decimal_point','行业名称','sse', 'sec']]
            index_info.rename(columns={'行业代码': 'code', '成份个数': 'volunit', '行业名称': 'name'}, inplace=True)
            index_info['code'] = index_info['code'].map(lambda x: str(x)[0:6])
            index_info['pre_close'] = ""
            index_info.set_index(
                ['code', 'sse'], drop=False, inplace=True)

        except Exception as e:
            print(e)
            print('Akshare except when fetch sw index info')
            time.sleep(1)
            index_info = fetch_swindex_list()
        
        return index_info

    return fetch_swindex_list()

def QA_fetch_get_swindex_component(code):
    """获取申万指数成份股
    """
    def fetch_swindex_component(code):
        sw_com = None
        try:
            sw_com = ak.index_component_sw(code)
            sw_com['code'] = code
            sw_com['date'] = str(datetime.date.today().strftime("%Y-%m-%d"))
            sw_com = sw_com.loc[:,['code', 'date', '证券代码','证券名称','最新权重','计入日期']]
            sw_com['计入日期' ]= sw_com['计入日期'].apply(lambda x: x.strftime('%Y-%m-%d'))
            sw_com.rename(columns={'证券代码': 'sec_code', '证券名称': 'sec_name', '最新权重': 'latest_weight', '计入日期': 'include_date'}, inplace=True)

        except Exception as e:
            print(e)

        return sw_com

    return fetch_swindex_component(code)

def QA_fetch_get_daily_extend(name='', start='', end='', td= '', type_='pd'):
    def fetch_val_data():
        data = None
        try:
            time.sleep(0.3)
            data = ak.stock_a_indicator_lg(symbol=str(name))
            print('fetch stock daily extend value done: ' + str(name))
        except Exception as e:
            print(e)
            print('except when fetch daily value data of ' + str(name))
            time.sleep(1)
            data = fetch_val_data()
        return data
    
    def fetch_quote_data(symbol, start_date, end_date):
        data = None
        try:
            time.sleep(0.3)
            data = ak.stock_zh_a_hist(symbol=str(symbol), period="daily", start_date=start_date, end_date=end_date)
            print('fetch stock daily extend turnover done: ' + str(name))
        except Exception as e:
            print(e)
            print('except when fetch quota data of ' + str(name))
            time.sleep(1)
            data = fetch_quote_data(symbol, start_date, end_date)
        return data

    data = fetch_val_data()
    data_turn = fetch_quote_data(name, start.replace("-",""), end.replace("-",""))
    #注意单位是万股
    data_turn["float_share"] = data_turn["成交量"] / data_turn["换手率"]
    data_turn.rename(columns={'日期': 'trade_date','换手率': 'turnover_rate'}, inplace=True)
    #print(data_turn)
    #拼接数据
    data = pd.merge(data, data_turn[["trade_date", "float_share", "turnover_rate"]], how="outer",on="trade_date")
    #print(data)
    data.rename(columns={'trade_date': 'date'}, inplace=True)
    data['date'] = data['date'].apply(lambda x: x.strftime("%Y-%m-%d"))
    data['date_stamp'] = data['date'].apply( lambda x: time.mktime(time.strptime(x, '%Y-%m-%d')) )
    data['code'] =  str(name)
    #过滤开始和结束时间内的数据
    data = data.loc[(data['date'] >= start)
                     & (data['date'] <= end)]
    if type_ in ['json']:
        data_json = QA_util_to_json_from_pandas(data)
        return data_json
    elif type_ in ['pd', 'pandas', 'p']:
        data = data.set_index('date', drop=False)
        return data

def QA_fetch_get_stock_list():
    stk_list = ak.stock_info_a_code_name()
    return stk_list

def QA_fetch_get_stock_block():
    """Akshare的版块数据
    
    Returns:
        [type] -- [description]
    """
    #补充获取板块数据：[中证500成分股]
    zz500_cons = ak.index_stock_cons(symbol="000905")
    try:
        zz500_cons['blockname'] = '中证500'
        zz500_cons['source'] = 'akshare'
        zz500_cons['type'] = 'csindex'
        zz500_cons = zz500_cons.drop(['品种名称', '纳入日期'], axis=1)
        zz500_cons.rename(columns={'品种代码': 'code'}, inplace=True)
        return zz500_cons.set_index('code', drop=False)
    except:
        return None
        
if __name__ == '__main__':
    #print(QA_fetch_get_swindex_list())
    #ak.sw_index_first_info()
    #print(QA_fetch_get_swindex_day_1('801010', '2023-06-30', '2023-07-20'))
    #print(QA_fetch_get_swindex_day_2('850122', '2023-07-17', '2023-07-22'))
    #print(ak.index_hist_sw('850351'))
    #print(QA_fetch_get_swindex_component('801770'))
    #print(QA_fetch_get_daily_extend("000001", "2024-02-19", "2024-02-20", "", "pd"))
    print(QA_fetch_get_stock_block())
    #print(ak.stock_profit_forecast_em())
    #print(QA_fetch_get_stock_list())