#!python3
#-*- coding : utf-8 -*-

import pymssql
import time
import MSSQLCONN

class DB_OPER:
    def __init__(self,job_name):
        self.job_name = job_name
        pass

    def start_job(self):
        CONN = MSSQLCONN.MSSQL(host = '10.172.36.55\db6', db = 'RDP_WALGREENS_R', user = 'nxgvtcuser' , pwd = '2pZGY5ij')


