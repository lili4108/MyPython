import threading
import time
import datetime
import os
import shutil

def hello(name,name1):
    print("hello %s\n %s\n" % (name,name1))

    global timer
    timer = threading.Timer(2.0, hello, ["Hawk","yy"])
    timer.start()

def transTime(assignTime):
    timeList = assignTime.replace(' ', '-').replace(':', '-').split('-')
    timeList = map(int, timeList)  # [2016, 12, 3, 10, 30]
    timeStr = datetime.datetime(*timeList)  # 2016-12-03 10:30:00
    timeLong = time.mktime(timeStr.timetuple())  # 1480732200.0
    return timeLong

def checkfolderexist(path):
    print(os.path.exists(path))
    print(os.access(path,os.R_OK))
    save_path = os.getcwd()
    os.chdir(path)
    print(os.getcwd())

def shutilcopy(f,file_d):
    shutil.copy2(f, file_d)

def sanyuan():
    a,b = 2,3
    c = a if a>b else b
    print('a = %d , b = %d , c = %d' % (a,b,c))


if __name__ == "__main__":
    # timer = threading.Timer(2.0, hello, ["Hawk","zz"])
    # timer.start()
    #hello('111','222')
    #print(transTime('2018:06:29 10:30:00'))
    # checkfolderexist(r'\\wal1wnnas1\Engine_Z\Boots_UK\DIRECT_FEED\Phase1')
    # checkfolderexist(r'\\wal1wnnas1.colo.retailsolutions.com\projects\New Retailers\Boots\Adv Card Restatements')
    # checkfolderexist(r'\\10.172.4.165\ftp_internal ')
    # checkfolderexist(r'\\PRODV1ARLNK8\D')
    # checkfolderexist(r'\\PRODV1ARLNK2\D')
    #checkfolderexist('D:\PYCHARM')
    # source_file = r'D:\test\test.txt'
    # d_file = r'd:\test1\test1\test1\test.txt'
    # d_folder = os.path.dirname(d_file)
    # if os.access(d_folder,os.R_OK) == False:
    #     os.makedirs(d_folder,0o777)
    # shutilcopy(source_file,d_file)
    #dic = {'a':31, 'bc':5, 'c':3}
    #dic = sorted(dic.items(),key = lambda d:d[0])
    L = [('b', 2), ('c', 1), ('a', 3), ('d', 4)]
    M = sorted(L, key=lambda x: x[0] , reverse = False  )
    print(M)


