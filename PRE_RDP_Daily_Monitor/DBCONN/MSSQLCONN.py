#!python3
# -*- coding: utf-8 -*-


import pymssql
import pyodbc
import time
import smtplib



class MSSQL:

    def __init__(self,host,db,user,pwd):
        self.host = host
        self.user = user
        # self.pwd = pwd
        # self.db = db

    def __GetConnection(self):
        if not self.db:
            raise(NameError,"there is no sqlserver config info")
        #self.conn = pymssql.connect(host = self.host,database = self.db ,charset = "utf8",user = self.user,password = self.pwd  )
        self.conn = pymssql.connect(host=self.host, database=self.db, charset="utf8")
        cur = self.conn.cursor()
        if not cur :
            raise(NameError,"connect to sqlserver failed")
        else :
            return cur

    def ExecQuery(self,sql):
        cur = self.__GetConnection()
        cur.execute(sql)
        resList = cur.fetchall()

        self.conn.close()
        return resList

    def ExecNonQuery(self,sql):
        cur = self.__GetConnection()
        cur.execute(sql)
        self.conn.commit()
        self.conn.close()


    def job_start(self,job_name):
        job_start_sql = "exec msdb.dbo.sp_start_job N'"+job_name+"'"
        self.ExecNonQuery(job_start_sql)


    def job_stop(self,job_name):
        job_start_sql = "exec msdb.dbo.sp_stop_job N'" + job_name + "'"
        self.ExecNonQuery(job_start_sql)

    def job_running_check(self,job_name):
        job_running_check = "Declare @Job_ID as UNIQUEIDENTIFIER   select @Job_ID =Job_ID from msdb.dbo.sysjobs where name = '" + job_name + "'    Exec master..sp_MSget_jobstate @Job_ID "
        return self.ExecQuery(job_running_check)[0][0]

    def job_status(self,job_name):
        job_status_sql = "select  top 1 run_status from [msdb].[dbo].[sysjobhistory] where job_id in (select job_id from msdb.dbo.sysjobs where name like '%" + job_name + "%') order by instance_id desc"
        job_status = self.ExecQuery(job_status_sql)[0][0]
        return job_status





def main():
    ms = MSSQL(host = '10.172.36.55\db6', db = 'RDP_WALGREENS_R', user = 'nxgvtcuser' , pwd = '2pZGY5ij')
    resList = ms.ExecQuery("select * FROM [dbo].[RSI_DIM_SILO]")
    print(resList)

    job_name = 'RDP_WALGREENS_R#SyncDatafromCMMT'

    job_status = ms.job_status(job_name)
    job_running = ms.job_running_check(job_name)
    print(job_status)
    print(job_running)

    if job_running == 1 :
        ms.job_stop(job_name)
        print("job " + job_name + " is running , stop and rerun")
        time.sleep(3)
        ms.job_start(job_name)
        print("start job " + job_name)
    else :
        ms.job_start(job_name)
        print("start job " + job_name)

    while job_running == 1 :
        time.sleep(2)
        print("job " + job_name + " is running")
        job_running = ms.job_running_check(job_name)

    job_status = ms.job_status(job_name)
    if job_status == 1:
        print("job completed")
    elif job_status == 3 :
        print("job be cancelled")
    else :
        print("job failed")




    #Wait unit job complet



    #sendmail ? if the job was failed

    #Check DB






    #ms1 = MSSQL(host = 'engp3qa4', db = 'FD_ODS_REG')


if __name__ == '__main__' :
    main()