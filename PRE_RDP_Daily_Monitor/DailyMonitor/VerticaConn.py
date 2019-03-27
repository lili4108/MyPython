# -*- coding: utf-8 -*-

import PRE_RDP_Daily_Monitor.commonconnectionmanager as cn
import PRE_RDP_Daily_Monitor.commonconfigdb as config
import time as tm
import datetime as dt
import PRE_RDP_Daily_Monitor.commonDBOperations as dbop
from sqlalchemy import update, exists
from sqlalchemy.orm import sessionmaker, aliased, scoped_session
from sqlalchemy.orm.exc import MultipleResultsFound
import pyodbc as py
import linecache
import sys
import os
import datetime
from random import randint

class Transformer:
    def __init__(self, rdp_server, app_id, job_number):
        self.__rdp_server = rdp_server
        self.__app_id = app_id
        self.__job_number = job_number
        self.__transformer_workflow = TransformerWorkFlow(rdp_server, app_id, job_number)

    def transformer_main(self):
        self.__transformer_workflow.workflow_process()

if __name__ == '__main__':
    tran = Transformer('RDP_TARGET', 'engv2hsdbqa1\db3', 1)
    tran.transformer_main()