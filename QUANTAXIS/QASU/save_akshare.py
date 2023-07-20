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
from QUANTAXIS.QAFetch.QAAkshare import QA_fetch_get_swindex_list, QA_fetch_get_swindex_day_1, QA_fetch_get_swindex_day_2,QA_fetch_get_swindex_component


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
        print(e)

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
            end_time = str(datetime.date.today().strftime("%Y-%m-%d"))[0:10]
            if ref_.count() > 0:
                start_time = ref_[ref_.count() - 1]['date']

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

if __name__ == '__main__':
    # from pymongo import MongoClient
    # client = MongoClient('localhost', 27017)
    # db = client['quantaxis']
    #QA_SU_save_swindex_list()
    QA_SU_save_swindex_component()
    #QA_SU_save_swindex_day_1()