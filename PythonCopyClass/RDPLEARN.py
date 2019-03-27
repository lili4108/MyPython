#coding:utf8

import json
import os
import time
import sys
import paramiko
from PythonCopyClass.Clean_FTP import Clean_FTP as Clean


def clean_ftp(rdp_id):
    folder = ['backup', 'archive']
    ssh_c = Clean('config.ini')
    for f in folder:
        cmd_ls = 'ls /snooper/' + rdp_id + '/' + f
        cmd_rm = 'rm -rf /snooper/' + rdp_id + '/' + f + '/*'
        if ssh_c.run_ssh(cmd_ls) == 0:
            print('no {0} file in RDP : {1}'.format(f, rdp_id))
        else:
            print(cmd_rm)
            ssh_c.run_ssh(cmd_rm)


def connect_linux(hostname,port,username,pw):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(hostname = hostname,port = port,username = username,password= pw )
    #while True:
    a = 'backup'
    b = 'PREP_WM_RDP'
    c = 'ls /snooper'
    input_command = 'rm -rf /snooper/PREP_WM_RDP/backup/WALMRT_DPSG/test1/*'
    #rm -rf /snooper/PREP_WM_RDP/backup/WALMRT_DPSG/test2
    #input_command = 'ls /snooper/PREP_WM_RDP/backup'
    #if input_command == 'quit':
        #break
    stdin,stdout,stderr = ssh.exec_command(input_command)
    result = stdout.read()
    print(len(result))
    # if len(result) == 0:
    #     print(stderr.read())
    #     print('result:{0}'.format(result))
    # else:
    #     print(str(result,'utf-8'))
    ssh.close()

def linux_main(hostname,port,username,pw):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(hostname = hostname,port = port,username = username,password= pw )

    command = 'ls /snooper/PREP_WM_RDP/backup/WMTCAT_NBTYVI'
    print(execute_command(ssh,command))


def execute_command(ssh,command_str):
    input,output,err = ssh.exec_command(command_str)
    resp,error = output.read(),err.read()
    if resp:
        return resp
    print('error ocurs : {0}'.format(error))
    raise Exception(error)



def read_location(j_location):
    with open(j_location, 'r') as ja:
        return json.load(ja)

def get_file_size(f_path):
    return os.path.getsize(f_path)

def get_file_mtime(f_path):
    return time.ctime(os.path.getmtime(f_path))
    ctime = time.ctime(os.path.getctime(f_path))
    print('mtime is {0}'.format(mtime))
    print('ctime is {0}'.format(ctime))
    if mtime > 'Thu Aug 16 14:32:42 2018':
        print('big')

def get_file_location():
    s = read_location('location_prep_wm_rdp.json')
    file = []
    for i in s:
        #print(s[i])
        for root,dirs,files in os.walk(i):
            for name in files :
                if name :
                    location = os.path.join(root,name)
                    file.append(location)
    return file

def read_updatetime(up_file):
    with open(up_file,'w+') as update:
        update_time = update.readline()
        curr_time = time.ctime()
        if not update_time:
            update.write(curr_time)

def get_new_files(source):
        #__update_time = self.__up_time
    file = []
    for root, dirs, files in os.walk(source):
        for name in files:
            print(name)
            # if name:
            #     location = os.path.join(root, name)
            #     file.append(location)
    return file


#get_new_files("d:\\test1\\test1\\test2.txt")
#print(os.path.dirname("d:\\test1\\test1\\test2.txt"))

# t1 = 'Fri Aug 17 15:36:48 2018'
# t2 = time.strptime(t1)
# print(t2)
# sec = time.mktime(t2)
# print(sec)

#print(time.ctime())

if __name__ == '__main__':
    #connect_linux('10.170.130.201',22,'snooper','5n00per')
    clean_ftp('PREP_WM_RDP')




