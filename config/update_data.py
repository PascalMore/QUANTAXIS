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
from QUANTAXIS.QASU.main import (QA_SU_save_etf_list, QA_SU_save_etf_day, QA_SU_save_index_list, QA_SU_save_index_day, QA_SU_save_stock_min,
                       QA_SU_save_stock_block, QA_SU_save_stock_day,QA_SU_save_stock_day_extend,QA_SU_save_etf_min,
                       QA_SU_save_stock_list, QA_SU_save_stock_xdxr)


print('SAVE/UPDATE {}'.format(datetime.datetime.now()))

wk = datetime.datetime.now().weekday() + 1
if wk == 6 or wk ==7:
    print('周{}不更新A股数据'.format(wk))

else:
    #1. 更新最新股票、板块、ETF、指数列表
    QA_SU_save_stock_list('tdx')
    QA_SU_save_stock_block('tdx')
    QA_SU_save_etf_list('tdx')
    QA_SU_save_index_list('tdx')
    # 新增扩展指数列表
    QA_SU_save_extension_index_list('tdx')

    #2. 更新每日的股票行情（日、分红除权、分钟）
    QA_SU_save_stock_day('tdx')
    QA_SU_save_stock_xdxr('tdx')

    #3. 更新ETF和指数日行情
    QA_SU_save_etf_day('tdx')
    QA_SU_save_index_day('tdx')
    
    #5. 更新币安数字货币所数据
    #QA_SU_save_binance_symbol()
    #QA_SU_save_binance_1day()
    #QA_SU_save_binance_1hour()
    #QA_SU_save_binance('30m')