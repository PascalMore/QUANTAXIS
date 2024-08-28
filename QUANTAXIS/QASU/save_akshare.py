# coding:utf-8
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
import json
import re
import time
import pymongo

import akshare as ak

from QUANTAXIS.QAUtil import (
    QA_util_to_json_from_pandas,
    QA_util_get_next_day,
    QA_util_log_info
)

from QUANTAXIS.QAUtil.QASetting import DATABASE
from QUANTAXIS.QAFetch.QAAkshare import QA_fetch_get_swindex_list, QA_fetch_get_swindex_day_1, QA_fetch_get_swindex_day_2,QA_fetch_get_swindex_component,QA_fetch_get_daily_extend,QA_fetch_get_stock_list


def QA_SU_save_swindex_list(client=DATABASE, ui_log=None, ui_progress=None):
    index_list = QA_fetch_get_swindex_list()
    coll_index_list = client.index_list
    coll_index_list.create_index("code", unique=True)

    try:
        coll_index_list.insert_many(
            QA_util_to_json_from_pandas(index_list),
            ordered=False
        )
    except Exception as e:
        #2023/12/23 fix： 忽略重复主键引起的异常日志
        pass

def QA_SU_save_swindex_component(client=DATABASE, ui_log=None, ui_progress=None):
    '''
    save swindex component
    保存申万行业成份股
    :param client:
    :param ui_log:  给GUI qt 界面使用
    :param ui_progress: 给GUI qt 界面使用
    :param ui_progress_int_value: 给GUI qt 界面使用
    '''
    __index_list = QA_fetch_get_swindex_list()
    coll = client['index_component']
    coll.create_index(
        [('code',
          pymongo.ASCENDING),
         ('date',
          pymongo.ASCENDING)]
    )
    err = []

    def __saving_work(code, coll):

        try:
            QA_util_log_info(
                '##JOB23 Now Saving SW INDEX COMPONENT==== \n Trying updating {} of {}'
                    .format(code,
                            str(datetime.date.today().strftime("%Y-%m-%d"))),
                ui_log=ui_log
            )
            coll.insert_many(
                QA_util_to_json_from_pandas(
                    QA_fetch_get_swindex_component(str(code))
                )
            )
        except Exception as e:
            QA_util_log_info(e, ui_log=ui_log)
            err.append(str(code))
            QA_util_log_info(err, ui_log=ui_log)

    for i_ in range(len(__index_list)):
        # __saving_work('000001')
        QA_util_log_info(
            'The {} of Total {}'.format(i_,
                                        len(__index_list)),
            ui_log=ui_log
        )

        strLogProgress = 'DOWNLOAD PROGRESS {} '.format(
            str(float(i_ / len(__index_list) * 100))[0:4] + '%'
        )
        intLogProgress = int(float(i_ / len(__index_list) * 10000.0))
        QA_util_log_info(
            strLogProgress,
            ui_log=ui_log,
            ui_progress=ui_progress,
            ui_progress_int_value=intLogProgress
        )
        __saving_work(__index_list.index[i_][0], coll)
    if len(err) < 1:
        QA_util_log_info('SUCCESS', ui_log=ui_log)
    else:
        QA_util_log_info(' ERROR CODE \n ', ui_log=ui_log)
        QA_util_log_info(err, ui_log=ui_log)

def QA_SU_save_swindex_day_1(client=DATABASE, ui_log=None, ui_progress=None):
    '''
    save swindex_day_1
    保存申万一级、二级、三级行业历史行情数据
    :param client:
    :param ui_log:  给GUI qt 界面使用
    :param ui_progress: 给GUI qt 界面使用
    :param ui_progress_int_value: 给GUI qt 界面使用
    '''
    __index_list = QA_fetch_get_swindex_list()
    #2023/06/29更新，使用QA_fetch_get_swindex_day_2可以获取申万三级行情，因此不需要再筛选了
    #__index_list = __index_list.loc[__index_list['decimal_point'] <= 2]
    coll = client.index_day
    coll.create_index(
        [('code',
          pymongo.ASCENDING),
         ('date_stamp',
          pymongo.ASCENDING)]
    )
    err = []

    def __saving_work(code, coll):

        try:
            ref_ = coll.find({'code': str(code)[0:6]})
            ref_count = coll.count_documents({'code': str(code)[0:6]})
            end_time = str(datetime.date.today().strftime("%Y-%m-%d"))[0:10]
            if ref_count > 0:
                start_time = ref_[ref_count - 1]['date']

                QA_util_log_info(
                    '##JOB22 Now Saving SW L1&L2&L3 INDEX_DAY==== \n Trying updating {} from {} to {}'
                        .format(code,
                                start_time,
                                end_time),
                    ui_log=ui_log
                )

                if start_time != end_time:
                    coll.insert_many(
                        QA_util_to_json_from_pandas(
                            QA_fetch_get_swindex_day_2(
                                str(code),
                                QA_util_get_next_day(start_time),
                                end_time
                            )
                        )
                    )
            else:
                try:
                    start_time = '1990-01-01'
                    QA_util_log_info(
                        '##JOB22 Now Saving SW L1&L2&L3 INDEX_DAY==== \n Trying updating {} from {} to {}'
                            .format(code,
                                    start_time,
                                    end_time),
                        ui_log=ui_log
                    )
                    coll.insert_many(
                        QA_util_to_json_from_pandas(
                            QA_fetch_get_swindex_day_2(
                                str(code),
                                start_time,
                                end_time
                            )
                        )
                    )
                except:
                    start_time = '2009-01-01'
                    QA_util_log_info(
                        '##JOB22 Now Saving SW L1&L2&L3 INDEX_DAY==== \n Trying updating {} from {} to {}'
                            .format(code,
                                    start_time,
                                    end_time),
                        ui_log=ui_log
                    )
                    coll.insert_many(
                        QA_util_to_json_from_pandas(
                            QA_fetch_get_swindex_day_2(
                                str(code),
                                start_time,
                                end_time
                            )
                        )
                    )
        except Exception as e:
            QA_util_log_info(e, ui_log=ui_log)
            err.append(str(code))
            QA_util_log_info(err, ui_log=ui_log)

    for i_ in range(len(__index_list)):
        # __saving_work('000001')
        QA_util_log_info(
            'The {} of Total {}'.format(i_,
                                        len(__index_list)),
            ui_log=ui_log
        )

        strLogProgress = 'DOWNLOAD PROGRESS {} '.format(
            str(float(i_ / len(__index_list) * 100))[0:4] + '%'
        )
        intLogProgress = int(float(i_ / len(__index_list) * 10000.0))
        QA_util_log_info(
            strLogProgress,
            ui_log=ui_log,
            ui_progress=ui_progress,
            ui_progress_int_value=intLogProgress
        )
        __saving_work(__index_list.index[i_][0], coll)
    if len(err) < 1:
        QA_util_log_info('SUCCESS', ui_log=ui_log)
    else:
        QA_util_log_info(' ERROR CODE \n ', ui_log=ui_log)
        QA_util_log_info(err, ui_log=ui_log)

def QA_SU_save_stock_day_extend(client=DATABASE, ui_log=None, ui_progress=None):
    '''
     save stock_day extend indicator
    保存每日衍生指标数据
    :param client:
    :param ui_log:  给GUI qt 界面使用
    :param ui_progress: 给GUI qt 界面使用
    :param ui_progress_int_value: 给GUI qt 界面使用
    '''
    def  _saving_work(code, coll_stock_day_extend, ui_log=None, err=[]):
        try:
            QA_util_log_info(
                '##JOB01 Now Saving STOCK_DAY_EXTEND==== {}'.format(str(code)),
                ui_log
            )

            # 首选查找数据库 是否 有 这个代码的数据
            ref = coll_stock_day_extend.find({'code': str(code)[0:6]})
            ref_count = coll_stock_day_extend.count_documents({'code': str(code)[0:6]})
            end_date = str(datetime.date.today().strftime("%Y-%m-%d"))[0:10]

            # 当前数据库已经包含了这个代码的数据， 继续增量更新
            # 加入这个判断的原因是因为如果股票是刚上市的 数据库会没有数据 所以会有负索引问题出现
            if ref_count > 0:

                # 接着上次获取的日期继续更新
                start_date = ref[ref_count - 1]['date']
                #start_date = date_conver_to_new_format(ref[ref.count() - 1]['date'])

                QA_util_log_info(
                    'UPDATE_STOCK_DAY_EXTEND \n Trying updating {} from {} to {}'
                    .format(code,
                            start_date,
                            end_date),
                    ui_log
                )
                if start_date != end_date:
                    coll_stock_day_extend.insert_many(
                        QA_util_to_json_from_pandas(
                            QA_fetch_get_daily_extend(
                                str(code),
                                QA_util_get_next_day(start_date)
                                ,
                                end_date,
                                '',
                                'pd'
                            )
                        )
                    )

            # 当前数据库中没有这个代码的股票数据， 从1990-01-01 开始下载所有的数据
            else:
                start_date = '1990-01—01'
                QA_util_log_info(
                    'UPDATE_STOCK_DAY_EXTEND\n Trying updating {} from {} to {}'
                    .format(code,
                            start_date,
                            end_date),
                    ui_log
                )
                if start_date != end_date:
                    coll_stock_day_extend.insert_many(
                        QA_util_to_json_from_pandas(
                            QA_fetch_get_daily_extend(
                                str(code),
                                start_date,
                                end_date,
                                '',
                                'pd'
                            )
                        )
                    )
        except Exception as e:
            print(e)
            err.append(str(code))

    stock_list = QA_fetch_get_stock_list().code.unique().tolist()
    #stock_list= list(["000001"])
    coll_stock_day_extend = client.stock_day_extend
    coll_stock_day_extend.create_index(
        [("code",
          pymongo.ASCENDING),
         ("date_stamp",
          pymongo.ASCENDING)]
    )

    err = []
    num_stocks = len(stock_list)
    for index, ts_code in enumerate(stock_list):
        QA_util_log_info('The {} of Total {}'.format(index, num_stocks))

        strProgressToLog = 'DOWNLOAD PROGRESS {} {}'.format(
            str(float(index / num_stocks * 100))[0:4] + '%',
            ui_log
        )
        intProgressToLog = int(float(index / num_stocks * 100))
        QA_util_log_info(
            strProgressToLog,
            ui_log=ui_log,
            ui_progress=ui_progress,
            ui_progress_int_value=intProgressToLog
        )
        _saving_work(ts_code,
                     coll_stock_day_extend,
                     ui_log=ui_log,
                     err=err)
        # 日线行情每分钟内最多调取200次，超过5000积分无限制
        time.sleep(0.05)

    if len(err) < 1:
        QA_util_log_info('SUCCESS save stock day extend ^_^', ui_log)
    else:
        QA_util_log_info('ERROR CODE \n ', ui_log)
        QA_util_log_info(err, ui_log)

if __name__ == '__main__':
    # from pymongo import MongoClient
    # client = MongoClient('localhost', 27017)
    # db = client['quantaxis']
    #QA_SU_save_swindex_list()
    #QA_SU_save_swindex_component()
    #QA_SU_save_swindex_day_1()
    QA_SU_save_stock_day_extend()