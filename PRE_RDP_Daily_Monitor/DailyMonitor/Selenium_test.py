#-*- coding : utf-8 -*-


import time
import random
import json
import os
import logging.config
import os.path


import PRE_RDP_Daily_Monitor as RDP


rootdir = os.path.dirname(__file__)
config = os.path.join(rootdir, 'ENV.json')
print(config)
file = open(config,'r')
config_j = json.load(file)

#config = os.path.join(rootdir,'ENV.json')

print(rootdir)
print(type(rootdir))

print(config_j)

# dict = {'size':10,'length':12}
# json_dict = json.dumps(dict)
# print(dict)
# print(json_dict)


# def setup_logging(default_path = "..\Configurations\logging.json",default_level = logging.INFO , env_key = "LOG_CFG"):
#     path = default_path
#     value = os.getenv(env_key,None)
#     if value:
#         path = value
#     if os.path.exists(path):
#         with open(path,"r") as f :
#             config = json.load(f)
#             logging.config.dictConfig(config)
#     else:
#         logging.basicConfig(level= default_level)
#
# def func():
#     logging.info("start func")
#     logging.info("Exec func")
#     logging.info("end func")
#
# if __name__ == "__main+__":
#     setup_logging(default_path = "..\Configurations\logging.json")
#     func()

# import smtplib
# from email.mime.text import MIMEText
# from email.header import Header
#
# sender = 'qa.support@retailsolutions.com'
# receivers = ['Charlie.Zhang@retailsolutions.com']
#
# message = MIMEText('Python send mail test...','plain','utf-8')
# message['From'] = Header("From",'utf-8')
# message['To'] = Header("To",'utf-8')
#
# subject = 'Python SMTP mail test'
# message['Subject'] = Header(subject,'utf-8')
#
# # try:
# smtpObj = smtplib.SMTP('relay.rsicorp.local',25)
# smtpObj.sendmail(sender,receivers,message.as_string())
# print('mail send complete')
#
#

# except smtplib.SMTPException:
#     print(r"Error:can't send mail")

# dr = webdriver.Chrome()
#
# dr.get("file:///D:/Charlie/CMMT%20Automate/Radio_checkbox.html")
# dr.implicitly_wait(10)
#
# dr.find_element_by_id("boy").click()
# dr.find_element_by_
# time.sleep(1)
# dr.find_element_by_id("girl").click()
#
# time.sleep(2)
#
#
# dr.quit()

'''
import logging
logging.basicConfig(level= logging.WARNING , format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

logger.info("Start print log")
logger.debug("Do something")
logger.warning("warning")
logger.info("Finish")
'''





