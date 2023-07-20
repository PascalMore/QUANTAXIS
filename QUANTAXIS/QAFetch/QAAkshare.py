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


if __name__ == '__main__':
    #print(QA_fetch_get_swindex_list())
    #print(QA_fetch_get_swindex_day_1('801010', '2023-01-01', '2023-06-01'))
    #print(QA_fetch_get_swindex_day_2('801012', '2023-01-01', '2023-06-01'))
    #print(ak.index_hist_sw('850351'))
    print(QA_fetch_get_swindex_component('801770'))
