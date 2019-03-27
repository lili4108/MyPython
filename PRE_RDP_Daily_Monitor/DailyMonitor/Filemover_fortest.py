#!python3
#-*- coding : utf-8 -*-

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

cmmt = webdriver.Chrome()
cmmt.maximize_window()

def setup():
    try:

        cmmt.get('http://engv2ahqapp1.eng.rsicorp.local:8086/cmmt/route')
        Username = cmmt.find_element_by_id('Username')
        Pass = cmmt.find_element_by_id('Password')

        Username.send_keys('charlie.zhang')
        Pass.send_keys('11111111_Zx')
        cmmt.find_element_by_class_name('mdl-button__ripple-container').click()
        # s1 = base64.encode('777777_Zx')
        # print(s1)



        # wait = WebDriverWait(cmmt,10)
        # wait.until()
    finally:
        pass
        # cmmt.close()

def teardown_module():
    cmmt.close()

def test_a_AddNewFilemoverServer():
    try:
        cmmt.find_element_by_xpath('//*[@id="admin_body"]/div/table/tbody/tr[1]/td/a/div').click()
        # RDP_S = Select(cmmt.find_element_by_xpath('//*[@id="clientInstanceName"]'))
        RDP_S = Select(cmmt.find_element_by_id('clientInstanceName'))
        # RDP_ID = RDP_S.select_by_index(2)
        RDP_S.select_by_value('RDP_WALGREENS_R')
        cmmt.find_element_by_xpath('//*[@id="btnNew"]').click()
        cmmt.find_element_by_xpath('//*[@id="serverNameValue"]').send_keys('test_filemover_server')
        File_Type = Select(cmmt.find_element_by_xpath('//*[@id="serverTypeValue"]'))
        File_Type.select_by_index(2)
        cmmt.find_element_by_xpath('//*[@id="renameVal"]').click()
        save_add = cmmt.find_element_by_xpath('//button[@onclick="doSaveServer()"]')
        save_add.click()
        Save_test = cmmt.find_element_by_xpath('//*[@id="theMessageText"]')
        print(Save_test.text)
        #cmmt.assertEqual(Save_test, 'Saved test	', 'same of not same?')


    except:
        traceback.print_exc()


    finally:
        pass
        cmmt.close()


def test_b_UpdateFilemoverServer():
    try:
        cmmt.find_element_by_xpath('//*[@id="admin_body"]/div/table/tbody/tr[1]/td/a/div').click()
        RDP_S = Select(cmmt.find_element_by_id('clientInstanceName'))
        RDP_S.select_by_value('RDP_WALGREENS_R')
        # Click Edit
        cmmt.find_element_by_xpath('//*[@id="filemoverServer_row1"]/td[2]/a[2]').click()
        # disable 'Append Timestamp'
        cmmt.find_element_by_xpath('//*[@id="renameVal"]').click()
        # Click Save button
        save_add = cmmt.find_element_by_xpath('//button[@onclick="doSaveServer()"]')
        save_add.click()
        Save_test = cmmt.find_element_by_xpath('//*[@id="theMessageText"]')
        self.assertEqual(Save_test, 'Saved test	', 'same of not same?')

    except:
        pass

    finally:
        pass


def test_c_UndeployFilemoverServer():
    cmmt.find_element_by_xpath('//*[@id="admin_body"]/div/table/tbody/tr[1]/td/a/div').click()
    RDP_S = Select(cmmt.find_element_by_id('clientInstanceName'))
    RDP_S.select_by_value('RDP_WALGREENS_R')
    # Click Deploy button
    cmmt.find_element_by_xpath('//*[@id="filemoverServer_row1"]/td[2]/a[1]').click()
    # driver to new window
    # cmmt.current_window_handle
    # Undeploy on all RDP instance
    # time.sleep(4)
    # cmmt.implicitly_wait(30)
    # deploy = cmmt.find_element_by_xpath('//form[@id="addDeployForm"]/div/div/table/tbody[2]/tr/td[3]/input')
    # deploy.click()

    # Explicit Wait
    deploy = WebDriverWait(cmmt, 10).until(lambda cmmt: cmmt.find_element_by_xpath(
        '//form[@id="addDeployForm"]/div/div/table/tbody[2]/tr/td[3]/input'))
    s = deploy.is_selected()
    if s:
        deploy.click()
    save = WebDriverWait(cmmt, 10).until(
        lambda cmmt: cmmt.find_element_by_xpath('//*[@id="addDeployCloseDiv"]/button[2]'))
    save.click()
    # cmmt.find_element_by_xpath('//*[@id="addDeployCloseDiv"]/button[2]').click()


def test_d_Delete_No_RDP_Event():
    cmmt.find_element_by_xpath('//*[@id="admin_body"]/div/table/tbody/tr[1]/td/a/div').click()
    RDP_S = Select(cmmt.find_element_by_id('clientInstanceName'))
    RDP_S.select_by_index(0)
    time.sleep(2)
    s = cmmt.find_element_by_xpath('//*[@class="filter"]/td[5]').click()
    time.sleep(2)
    ActionChains(cmmt).send_keys('test_filemover_server').perform()
    # cmmt.find_element_by_xpath('//*[@class="filter"]/td[5]/div').send_keys('test_filemover_server').perform()
    time.sleep(2)
    cmmt.find_element_by_xpath('//*[@id="filemoverServer"]/thead/tr[2]/td/table/tbody/tr/td[7]/input').click()
    cmmt.find_element_by_xpath('//*[@id="filemoverServer_row1"]/td[1]/input').click()
    cmmt.find_element_by_xpath('//*[@id="delete"]').click()
    time.sleep(2)
    t = cmmt.switch_to.alert.accept()

    print(t)
    # t.accept()

if __name__ == '__main__' :

    #unittest.main()
    # login('http://engv2ahqapp1.eng.rsicorp.local:8086/cmmt/route')
    setup()
    #test_a_AddNewFilemoverServer()
    # test_b_UpdateFilemoverServer()
    # test_c_UndeployFilemoverServer()
    test_d_Delete_No_RDP_Event()
