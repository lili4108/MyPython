# -*- coding: utf-8 -*-

import PRE_RDP_Daily_Monitor.DailyMonitor.rdp_main as mn
import unittest
import time
import os.path
import os
#import HTMLTestRunner
import PRE_RDP_Daily_Monitor.DailyMonitor.HTMLTestRunner as HTMLTestRunner


casepath = ".\\"
result = ".\\result\\"

def CreatSuite():
    suite = unittest.TestSuite()
    discover = unittest.defaultTestLoader.discover(casepath,pattern = 'rdp_main*.py',top_level_dir=None)

    for test_case in discover:
        suite.addTest(test_case)
    print(suite)
    return suite

if __name__ ==  "__main__":
    all_test = CreatSuite()

    now = time.strftime('%Y-%m-%d-%H_%M_%S', time.localtime(time.time()))
    day = time.strftime('%Y-%m-%d', time.localtime(time.time()))

    tdresult = result + day

    if os.path.exists(tdresult):
        filename = tdresult + "\\" + now + "_result.html"
        fp = open(filename,'wb')
        runner = HTMLTestRunner.HTMLTestRunner(stream = fp , title = u'test report',description=u'run RDP test case')
        runner.run(all_test)
        fp.close()

    else :
        os.mkdir(tdresult)
        filename = tdresult + "\\" + now + "_result.html"
        fp = open(filename,'wb')
        runner = HTMLTestRunner.HTMLTestRunner(stream=fp,title=u'test report',description=u'run RDP test cases')
        runner.run(all_test)
        fp.close()

    # mail = cm.Email('RSi Profile','tet report',filename,attachment='')
    # mail.send_mail()



