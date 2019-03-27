#!python3
# -*- coding: utf-8 -*-

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.common.action_chains import ActionChains
import base64
import pytest
import traceback
import time

import unittest

from selenium.webdriver.support.ui import Select

from PRE_RDP_Daily_Monitor.DBCONN.MSSQLCONN import  MSSQL



class RDP(unittest.TestCase):
    def setUp(self):
        try:
            self.drive = webdriver.Chrome()
            self.drive.maximize_window()
            self.drive.get('http://prez2capp1.prod.rsicorp.local:8082/cmmt/route')
            Username = self.drive.find_element_by_id('Username')
            Pass = self.drive.find_element_by_id('Password')

            Username.send_keys('charlie.zhang')
            Pass.send_keys('888888_Zx')
            self.drive.find_element_by_class_name('mdl-button__ripple-container').click()
            # s1 = base64.encode('777777_Zx')
            # print(s1)


            # wait = WebDriverWait(self.drive,10)
            # wait.until()
        finally:
            pass
            #self.drive.close()

    def test_a_AddNewFilemoverServer(self):
        try:
            self.drive.find_element_by_xpath('//*[@id="admin_body"]/div/table/tbody/tr[1]/td/a/div').click()
            #RDP_S = Select(self.drive.find_element_by_xpath('//*[@id="clientInstanceName"]'))
            RDP_S = Select(self.drive.find_element_by_id('clientInstanceName'))
            #RDP_ID = RDP_S.select_by_index(2)
            RDP_S.select_by_index(2)
            self.drive.find_element_by_xpath('//*[@id="btnNew"]').click()
            self.drive.find_element_by_xpath('//*[@id="serverNameValue"]').send_keys('test_filemover_server')
            File_Type = Select(self.drive.find_element_by_xpath('//*[@id="serverTypeValue"]'))
            File_Type.select_by_index(2)
            self.drive.find_element_by_xpath('//*[@id="renameVal"]').click()
            save_add = self.drive.find_element_by_xpath('//button[@onclick="doSaveServer()"]')
            save_add.click()
            Save_test = self.drive.find_element_by_xpath('//*[@id="theMessageText"]')
            self.assertEqual(Save_test.text, 'Saved test_filemover_server', 'not same?')
        except:
            traceback.print_exc()
        finally:
            pass
            #self.drive.close()

    def test_a_filemover_db(self):

        ms = MSSQL(host='prez2cdb2\db1', db='PREP_WM_RDP')

        job_name = 'PREP_WM_RDP#SyncDatafromCMMT'

        job_status = ms.job_status(job_name)
        job_running = ms.job_running_check(job_name)
        print(job_status)
        print(job_running)

        if job_running == 1:
            print("job " + job_name + " is running , stop and rerun")
            time.sleep(1)
            ms.job_stop(job_name)
            time.sleep(3)
            ms.job_start(job_name)
            print("start job " + job_name)

        else:
            print("start job " + job_name)
            time.sleep(2)
            ms.job_start(job_name)


        job_running = ms.job_running_check(job_name)

        while job_running == 1:
            time.sleep(2)
            print("job " + job_name + " is running")
            job_running = ms.job_running_check(job_name)

        job_status = ms.job_status(job_name)
        if job_status != 0:
            print("job completed")
            filemover_name_sql = "select top 1 name from filemover_server"
            filemover_name = ms.ExecQuery(filemover_name_sql)[0][0]
            self.assertEqual(filemover_name,'test_filemover_server',"filemover server add not correct")
        # elif job_status == 3:
        #     print("job be cancelled")
        else:
            print("job failed")
            return









    def test_b_UpdateFilemoverServer(self):
        try:
            self.drive.find_element_by_xpath('//*[@id="admin_body"]/div/table/tbody/tr[1]/td/a/div').click()
            RDP_S = Select(self.drive.find_element_by_id('clientInstanceName'))
            RDP_S.select_by_index(2)
            #Click Edit
            self.drive.find_element_by_xpath('//*[@id="filemoverServer_row1"]/td[2]/a[2]').click()
            #disable 'Append Timestamp'
            self.drive.find_element_by_xpath('//*[@id="renameVal"]').click()
            #Click Save button
            save_add = self.drive.find_element_by_xpath('//button[@onclick="doSaveServer()"]')
            save_add.click()
            Save_test = self.drive.find_element_by_xpath('//*[@id="theMessageText"]')
            self.assertEqual(Save_test, 'Saved test	', 'same of not same?')

        except:
            pass

        finally:
            pass


    def test_c_UndeployFilemoverServer(self):
        self.drive.find_element_by_xpath('//*[@id="admin_body"]/div/table/tbody/tr[1]/td/a/div').click()
        RDP_S = Select(self.drive.find_element_by_id('clientInstanceName'))
        RDP_S.select_by_index(2)
        #Click Deploy button
        self.drive.find_element_by_xpath('//*[@id="filemoverServer_row1"]/td[2]/a[1]').click()
        #driver to new window
        #self.drive.current_window_handle
        #Undeploy on all RDP instance
        #time.sleep(4)
        # self.drive.implicitly_wait(30)
        # deploy = self.drive.find_element_by_xpath('//form[@id="addDeployForm"]/div/div/table/tbody[2]/tr/td[3]/input')
        # deploy.click()

        #Explicit Wait
        deploy = WebDriverWait(self.drive,10).until(lambda cmmt:self.drive.find_element_by_xpath('//form[@id="addDeployForm"]/div/div/table/tbody[2]/tr/td[3]/input'))
        s = deploy.is_selected()
        if s :
            deploy.click()
        save = WebDriverWait(self.drive,10).until(lambda cmmt:self.drive.find_element_by_xpath('//*[@id="addDeployCloseDiv"]/button[2]'))
        save.click()
        #self.drive.find_element_by_xpath('//*[@id="addDeployCloseDiv"]/button[2]').click()



    def test_d_Delete_No_RDP_Event(self):
        self.drive.find_element_by_xpath('//*[@id="admin_body"]/div/table/tbody/tr[1]/td/a/div').click()
        time.sleep(2)
        s = self.drive.find_element_by_xpath('//*[@class="filter"]/td[5]').click()
        time.sleep(2)
        ActionChains(self.drive).send_keys('test_filemover_server').perform()
        #self.drive.find_element_by_xpath('//*[@class="filter"]/td[5]/div').send_keys('test_filemover_server').perform()
        time.sleep(2)
        self.drive.find_element_by_xpath('//*[@id="filemoverServer"]/thead/tr[2]/td/table/tbody/tr/td[7]/input').click()
        self.drive.find_element_by_xpath('//*[@id="filemoverServer_row1"]/td[1]/input').click()
        self.drive.find_element_by_xpath('//*[@id="delete"]').click()
        time.sleep(2)
        t = self.drive.switch_to.alert.accept()
        time.sleep(2)

        print(t)
        #t.accept()

    def tearDown(self):
        self.drive.quit()


if __name__ == '__main__' :

    unittest.main()
    # login('http://engv2ahqapp1.eng.rsicorp.local:8086/self.drive/route')
    # # AddNewFilemoverServer()
    # # UpdateFilemoverServer()
    # # UndeployFilemoverServer()
    # Delete_No_RDP_Event()


