import os
import datetime
import threading
from multiprocessing import Process

def copysch():
    os.system(r'"D:\\PyCharm\Boots_copy\WMRDPFileCopy.py"')

def copysch(RDP):
    switch = {
        "BOOTS": r'"D:\PyCharm\Boots_copy\WMRDPFileCopy.py"',
        "DPSG": r'"D:\PyCharm\PREP_WM_RDP_COPY\WMRDPFileCopy.py"',
        "PEPSICO": r'"D:\PyCharm\WM_COPY\WMRDPFileCopy.py"',
        "DPSG_HF": r'"D:\PyCharm\WM_HF_COPY\WMRDPFileCopy.py"',
            }

    print(switch[RDP] + 'start')
    print(RDP, "--------------------start time : ", datetime.datetime.now())
    os.system(switch[RDP])
    print(switch[RDP] + 'end')
    print(RDP, "--------------------end time : ", datetime.datetime.now())

    global timer
    timer = threading.Timer(180.0, copysch, [RDP])
    timer.start()


def tm(rdp_id):
    timer = threading.Timer(3.0, copysch , [rdp_id])
    timer.start()


if __name__ == '__main__' :
    #copysch('BOOTS')

    p1 = Process(target = tm, args = ('BOOTS',))
    p2 = Process(target = tm, args=('DPSG',))
    p3 = Process(target = tm, args=('PEPSICO',))

    p1.start()
    p2.start()
    p3.start()





