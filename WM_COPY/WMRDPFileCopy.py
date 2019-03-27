# -*- coding: utf-8 -*-

import os.path, datetime, time, os
import shutil
import json
import os
import smtplib
from email.mime.text import MIMEText
from email.header import Header


def copy_wm_sub(source_loc, target_loc, files, updatetime):
    maxtime = updatetime
    with open(r"log.txt", 'a') as log2:
        for i in files:
            log2.write('file list : ' + i + ' | file updated time is : ' + str(os.path.getmtime(i)) + '\n')
    for f in files:
        try:
            mtime = os.path.getmtime(f)
        except:
            continue
        if mtime > maxtime:
            maxtime = mtime
        if mtime > updatetime:
            with open(r"log.txt", 'a') as log1:
                log1.write(str(datetime.datetime.now()) + '\n')
                file_d = f.replace(source_loc, target_loc)
                log1.write("copy file (" + str(os.path.basename(f)) + " )from folder (" + str(
                    os.path.dirname(f)) + ") to folder (" + str(target_loc) + " ) " + '\n')
                print("copy file %s from folder %s to folder %s" % (f, source_loc, target_loc))
                try:
                    d_folder = os.path.dirname(file_d)
                    if os.access(d_folder,os.R_OK) == False:
                        os.makedirs(d_folder)
                    shutil.copy2(f, file_d)
                except FileNotFoundError as e:
                    print(e)
                    continue

                log1.write('new max_updated_time is ' + str(maxtime) + '\n')
                print('new max_updated_time is ', maxtime)
                log1.write('\n')
                log1.write('\n')
    if maxtime == updatetime:
        with open(r"log.txt", 'a') as log:
            log.write(str(datetime.datetime.now()) + '\n')
            log.write("there is no file found in folder %s" + str(source_loc) + '\n')
            log.write('current max_updated_time is ' + str(updatetime))
            log.write('the final max_updated_time is ' + str(maxtime))
            log.write('\n')
            log.write('\n')
            print("no files found in folder %s" % source_loc)
            print('current max_updated_time is ', updatetime)
            print('the final max_updated_time is ', maxtime)
    return maxtime


def copy_wm_main(assignTime):
    with open(r'updatetime.txt', 'r') as up:
        updatetime = float('%.7f' % float(up.read()))

    with open(r'location.json') as js:
        d = json.load(js)
        max_time = 0
        for i in d:
            # print(i.replace("\\","/"))
            if os.access(i, os.R_OK) == False:
                subject = 'can\'t open' + i
                print(subject)
                sendmail(subject, mail_from, mail_to, mail_server, subject)
                with open(r"log.txt", 'a') as log3:
                    log3.write(subject)
                continue
            else:
                subject = 'open folder success' + i
                print(subject)
                #sendmail(subject, mail_from, mail_to, mail_server, subject)
                subtime = copy_wm_sub(i, d[i], getspecifyfiles(i, assignTime), updatetime)
                if subtime > max_time:
                    max_time = subtime
        updatetime = max_time

    with open(r'updatetime.txt', 'w') as wr:
        wr.write(str(updatetime))


def getspecifyfiles(source_loc, assignTime):
    file = []
    for root, dirs, files in os.walk(source_loc):
        for name in files:
            try:
                f = os.path.join(root, name)
                mtime = os.path.getmtime(f)
            except FileNotFoundError as e:
                print(f)
                continue

            if mtime > transTime(assignTime):
                file.append(f)

    return file


def cleanlogfile(file):
    with open(file, 'a+') as f:
        n = getDocSize(file)
        print(file)
        print("n = ", n)
        if 'M' in n:
            with open(file, 'w+') as f:
                print('clean log file complete')
        # if lines > 10000:
        #     f.write('\n')


def formatSize(bytes):
    try:
        bytes = float(bytes)
        kb = bytes / 1024
    except:
        print("the pass byte format not correct")
        return "Error"

    if kb >= 1024:
        M = kb / 1024
        if M >= 1024:
            G = M / 1024
            return "%fG" % (G)
        else:
            return "%fM" % (M)
    else:
        return "%fkb" % (kb)


def getDocSize(file):
    try:
        size = os.path.getsize(file)
        return formatSize(size)
    except Exception as err:
        print(err)


def transTime(assignTime):
    timeList = assignTime.replace(' ', '-').replace(':', '-').split('-')
    timeList = map(int, timeList)  # [2016, 12, 3, 10, 30]
    timeStr = datetime.datetime(*timeList)  # 2016-12-03 10:30:00
    timeLong = time.mktime(timeStr.timetuple())  # 1480732200.0
    return timeLong


def sendmail(subject1, fr, to, host, content):
    sender = fr
    receivers = [to]

    message = MIMEText(content, 'plain', 'utf-8')
    # message['From'] = Header(fr, 'utf-8')
    message['To'] = Header(to, 'utf-8')

    subject = 'Server can\'t connect'

    message['Subject'] = Header(subject1, 'utf-8')

    try:
        smtpObj = smtplib.SMTP(host)
        smtpObj.sendmail(sender, receivers, message.as_string())
        print("Mail delivery success")
    except smtplib.SMTPException:
        print("Error: Mail delivery failed")


if __name__ == '__main__':
    path = os.path.dirname(__file__)
    os.chdir(path)
    now_time = datetime.datetime.now()
    yesterday = now_time + datetime.timedelta(days=-4)
    assignTime = yesterday.strftime("%Y-%m-%d %H:%M")
    mail_from = 'scott.pei@retailsolutions.com'
    mail_to = 'charlie.zhang@retailsolutions.com'
    mail_server = '192.168.36.63'

copy_wm_main(assignTime)

cleanlogfile(r'log.txt')


