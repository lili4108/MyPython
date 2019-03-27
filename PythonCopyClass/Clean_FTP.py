# -*- coding: utf-8 -*-

import paramiko,configparser

class Clean_FTP:
    def __init__(self,ini_file):
        self.config = configparser.ConfigParser()
        self.config.read(ini_file)
        self.host = self.config.get('ssh','host')
        self.port = self.config.get('ssh', 'port')
        self.user = self.config.get('ssh', 'user')
        self.pwd = self.config.get('ssh', 'pwd')
        self.timeout = self.config.get('ssh', 'timeout')
        self.client = paramiko.SSHClient()
        self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.client.connect(hostname=self.host,port=self.port,username=self.user,password=self.pwd,timeout=float(self.timeout))

    def run_ssh(self,cmd):
        stdin,stdout,stderr = self.client.exec_command(cmd)
        result = stdout.read()
        return result

    def close(self):
        self.client.close()










