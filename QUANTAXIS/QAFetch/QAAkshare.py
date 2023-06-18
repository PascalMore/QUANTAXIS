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
import akshare as ak

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

if __name__ == '__main__':
    print(QA_fetch_get_swindex_list())
