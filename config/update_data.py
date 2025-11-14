#!/usr/local/bin/python

#coding :utf-8
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

import datetime
import multiprocessing
from QUANTAXIS import (QA_SU_save_etf_list, QA_SU_save_etf_day, QA_SU_save_index_list, QA_SU_save_index_day, QA_SU_save_extension_index_list, QA_SU_save_swindex_list, QA_SU_save_swindex_day_1, QA_SU_save_swindex_component, QA_SU_save_extension_index_day, QA_SU_save_stock_min,
                       QA_SU_save_stock_block, QA_SU_save_stock_day,QA_SU_save_stock_day_extend,QA_SU_save_etf_min,
                       QA_SU_save_stock_list, QA_SU_save_stock_xdxr)



def err_call_back(err):
    print("进程池error: " + str(err))

def process(l):
    if l == 'stock_day':
        print("开始stock_day的数据更新")
        QA_SU_save_stock_day('tdx')
        print("完成stock_day的数据更新")
    elif l == 'stock_xdxr':
        print("开始stock_xdxr的数据更新")
        QA_SU_save_stock_xdxr('tdx')
        print("完成stock_xdxr的数据更新")
    elif l == 'stock_day_extend':
        print("开始stock_day_extend的数据更新")
        QA_SU_save_stock_day_extend('ak')
        print("完成stock_day_extend的数据更新")
    elif l == 'etf_day':
        print("开始etf_day的数据更新")
        QA_SU_save_etf_day('tdx')
        print("完成etf_day的数据更新")
    elif l == 'index_day':
        print("开始index_day的数据更新")
        QA_SU_save_index_day('tdx')
        print("完成index_day的数据更新")
    elif l == 'extension_index_day':
        print("开始extension_index_day的数据更新")
        QA_SU_save_extension_index_day('tdx')
        print("完成extension_index_day的数据更新")
    elif l == 'swindex_day_1':
        print("开始swindex_day的数据更新")
        QA_SU_save_swindex_day_1('ak')
        print("完成swindex_day的数据更新")
    else:
        return
    
data_list = [
    'stock_day',
    'stock_xdxr',
    'stock_day_extend',
    'etf_day',
    'index_day',
    'extension_index_day',
    'swindex_day_1'
    ]

def main():
    print('SAVE/UPDATE {}'.format(datetime.datetime.now()))
    
    wk = datetime.datetime.now().weekday() + 1
    if wk == 6 or wk ==7:
        print('周{}不更新A股数据'.format(wk))
        
    else:
        #1. 更新最新股票、板块、ETF、指数列表
        try:
            QA_SU_save_stock_list('tdx')
            QA_SU_save_stock_block('tdx')
            QA_SU_save_etf_list('tdx')
            QA_SU_save_index_list('tdx')
            # 新增扩展指数列表
            QA_SU_save_extension_index_list('tdx')
            # 新增申万行业指数以及成份股
            QA_SU_save_swindex_list('ak')
            QA_SU_save_swindex_component('ak')
        except Exception as e:
            print("更新股票/板块/ETF/指数等清单出现异常：", e)

        pool = multiprocessing.Pool(2) # 两个进程执行
        for l in data_list:
            pool.apply_async(func=process, args=(l,), error_callback=err_call_back)
            
        pool.close()
        pool.join()

if __name__ == '__main__':
    main()