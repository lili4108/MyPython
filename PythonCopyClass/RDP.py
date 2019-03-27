# -*- coding: utf-8 -*-
import os.path, time, json, os, shutil, smtplib,sys
from email.mime.text import MIMEText
from email.header import Header
from PythonCopyClass.Clean_FTP import Clean_FTP as Clean


class Rdp:

    def __init__(self,rdp_id):
        self.__rdp_id = rdp_id

    __mail_from = 'scott.pei@retailsolutions.com'
    __mail_to = 'charlie.zhang@retailsolutions.com'
    __mail_server = '192.168.36.63'

    def copy_process(self):

        self.__location_path = self.__rdp_id + '_location.json'
        self.__absolute_location_path = self.__get_file_absolute_path('Location',self.__location_path)
        self.__updatetime_path = self.__rdp_id + '_updatetime.txt'
        self.__absolute_updatetime_path = self.__get_file_absolute_path('Updatetime',self.__updatetime_path)
        self.__log_path =  self.__rdp_id + '_log.txt'
        self.__absolute_log_path = self.__get_file_absolute_path('Log',self.__log_path)

        self.__up_time = self.__read_updatetime(self.__absolute_updatetime_path)

        try:
            self.__compare_t = self.__up_time
            self.__write_log(time.ctime() , self.__absolute_log_path)
            self.__write_log('Start copy file !!' + '\n', self.__absolute_log_path)
            self.__s = self.__load_location(self.__absolute_location_path)
            for i in self.__s:
                if os.access(i,os.R_OK) == False:
                    subject = "Can't access file path : {0}".format(i)
                    self.__write_log(subject,self.__absolute_log_path)
                    self.__sendmail(subject,self.__mail_from,self.__mail_to,self.__mail_server,subject)
                    continue
                else:
                    f = self.__get_new_files(i)
                    for file in f :
                        target = self.__convert_target_path(i,self.__s[i],file)
                        self.__copy_file(file,target)
                        self.__write_log('copy file {0} to {1} '.format(file,target) , self.__absolute_log_path)
                        self.__up_time = self.__get_max_update_time(self.__get_file_mtime(file),self.__up_time)
            if self.__compare_time(self.__compare_t,self.__up_time) == 2:
                self.__write_log('No file copy in this time period!' + '\n', self.__absolute_log_path)
                print('%s : No file found in source folder!' % self.__rdp_id)
            else:
                self.__write_updatetime(self.__absolute_updatetime_path, self.__up_time)
                self.__write_log('copy file all complete' + '\n', self.__absolute_log_path)
            self.__clean_log(self.__absolute_log_path)
            self.__clean_ftp(self.__rdp_id)

        except Exception as e:
            print(e)
            self.__write_log(time.ctime() + '\n',self.__absolute_log_path)
            self.__write_log(e,self.__absolute_log_path)

        finally:
            pass

    def __clean_ftp(self,rdp_id):
        folder = ['backup', 'archive']
        ssh_c = Clean('config.ini')
        for f in folder:
            cmd_ls = 'ls /snooper/' + rdp_id + '/' + f
            cmd_rm = 'rm -f /snooper/' + rdp_id + '/' + f + '/*'
            result = ssh_c.run_ssh(cmd_ls)
            if len(result) == 0:
                print('no {0} file in RDP : {1}'.format(f, rdp_id))
            else:
                ssh_c.run_ssh(cmd_rm)

    def __get_max_update_time(self,file_mtime,update_time):
        file_t = time.mktime(time.strptime(file_mtime))
        update_t = time.mktime(time.strptime(update_time))
        if file_t > update_t :
            return file_mtime
        else :
            return update_time

    def __compare_time(self,time1,time2):
        time_1 = time.mktime(time.strptime(time1))
        time_2 = time.mktime(time.strptime(time2))
        if time_1 > time_2:
            return 1
        elif time_1 == time_2 :
            return 2
        else:
            return 0

    def __get_new_files(self,source):
        #__update_time = self.__up_time
        file = []
        for root, dirs,files in os.walk(source):
            for name in files:
                if name:
                    location = os.path.join(root, name)
                    if len(location) > 259 :
                        continue
                    if self.__compare_time(self.__get_file_mtime(location),self.__up_time) == 1:
                        file.append(location)
        return file

    def __convert_target_path(self,source,target,file_full):
        target_path = file_full.replace(source,target)
        if not os.path.exists(os.path.dirname(target_path)):
            os.makedirs(os.path.dirname(target_path))
        return target_path


    def __copy_file(self,source_file,target_file):
        shutil.copy2(source_file,target_file)

    def __read_updatetime(self,up_file):
        if not os.path.exists(up_file):
            with open(up_file, 'w'):
                pass
        with open(up_file,'r') as update:
            update_time = update.readline()
            if not update_time:
                update_time = time.ctime()
            return update_time

    def __get_file_absolute_path(self,type,file_name):
        return os.path.join(r"\\10.172.36.87\charlie_data\RCT_DATA_COPY",type,file_name)

    def __write_updatetime(self,up_file,time):
        with open(up_file, 'w+') as update:
            update.write(time)

    def __write_log(self,log,log_path):
        with open(log_path,'a+') as log_f:
            log_f.write(str(log) + '\n')

    def __clean_log(self,log_path):
        if os.path.getsize(log_path) > 10000000:
            with open(log_path,'w'):
                pass

    def __load_location(self,j_location):
        try:
            with open(j_location, 'r') as ja:
                return json.load(ja) #return dict
        except FileNotFoundError:
            self.__write_log("Can't find file {0}".format(j_location) , self.__absolute_log_path)

    def __get_file_mtime(self,f_path):
        return time.ctime(os.path.getmtime(f_path))

    def __sendmail(self,subject1, fr, to, host, content):
        sender = fr
        receivers = [to]

        message = MIMEText(content, 'plain', 'utf-8')
        message['To'] = Header(to, 'utf-8')
        message['Subject'] = Header(subject1, 'utf-8')

        try:
            smtpObj = smtplib.SMTP(host)
            smtpObj.sendmail(sender, receivers, message.as_string())
            print("Mail delivery success")
        except smtplib.SMTPException:
            print("Error: Mail delivery failed")

    def __check_path_access(self,path):
        if os.access(path, os.R_OK) == False:
            return False
        else:
            return True


if __name__ == '__main__':
    if len(sys.argv) > 1:
        print('%s : %s' % (sys.argv[1],time.ctime()))
        print('%s : Start copy file !!' % sys.argv[1])
        rdp_instance = Rdp(sys.argv[1])
        rdp_instance.copy_process()
        print('%s : File copy complete !! \n' % sys.argv[1])
    else :
        print('only one system argument!')
        rdp_ins = Rdp('PREP_WM_RDP')
        rdp_ins.copy_process()



