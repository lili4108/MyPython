# -*- coding: utf-8 -*-

import os.path , datetime ,time, os
import shutil
import json

def copy_wm_sub(source_loc,target_loc,files,updatetime):
    maxtime = updatetime
    for f in files:
        mtime = os.path.getmtime(f)
        if mtime > maxtime:
            maxtime = mtime
        if mtime > updatetime:
            file_d = f.replace(source_loc,target_loc)
            print("copy file %s from folder %s to folder %s" % (f,source_loc,target_loc))
            shutil.copy2(f, file_d)
            print('new max_updated_time is ' , maxtime)
    if maxtime == updatetime:
        print("no file found in folder %s" % source_loc)
        print('current max_updated_time is ' , updatetime)
        print('the final max_updated_time is ', maxtime)
    return maxtime

def copy_wm_main(assignTime):
    with open(r'D:\PyCharm\WM_HF_COPY\updatetime.txt','r') as up:
        updatetime = float('%.7f' % float(up.read()))

    with open(r'D:\PyCharm\WM_HF_COPY\location.json') as js:
        d = json.load(js)
        max_time = 0
        for i in d:
            # print(i.replace("\\","/"))
            subtime = copy_wm_sub(i, d[i], getspecifyfiles(i, assignTime), updatetime)
            if subtime > max_time:
                max_time = subtime
        updatetime = max_time

    with open(r'D:\PyCharm\WM_HF_COPY\updatetime.txt','w') as wr :
        wr.write(str(updatetime))

def getspecifyfiles(source_loc,assignTime):
    file = []
    for root,dirs,files in os.walk(source_loc):
        for name in files:
            f = os.path.join(root,name)
            mtime = os.path.getmtime(f)
            if mtime > transTime(assignTime) :
                file.append(f)
    return file


def transTime(assignTime):
    """
    @summary:将给定时间转换为长整形
    @param assignTime:给定的时间     如：'2016-12-3 10:30'
    @return: timeLong 长整形时间
    """
    timeList = assignTime.replace(' ','-').replace(':','-').split('-')
    timeList = map(int,timeList)  #[2016, 12, 3, 10, 30]
    timeStr = datetime.datetime(*timeList) #2016-12-03 10:30:00
    timeLong = time.mktime(timeStr.timetuple()) #1480732200.0
    return timeLong

if __name__ == '__main__' :
    now_time = datetime.datetime.now()
    yesterday = now_time + datetime.timedelta(days = -1)
    assignTime = yesterday.strftime("%Y-%m-%d %H:%M")

    copy_wm_main(assignTime)


