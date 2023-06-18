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
    QA_util_to_json_from_pandas
)

from QUANTAXIS.QAUtil.QASetting import DATABASE
from QUANTAXIS.QAFetch.QAAkshare import QA_fetch_get_swindex_list


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


if __name__ == '__main__':
    from pymongo import MongoClient
    client = MongoClient('localhost', 27017)
    db = client['quantaxis']
    QA_SU_save_swindex_list(client=db)