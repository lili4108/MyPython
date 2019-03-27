# -*- coding: utf-8 -*-

import os
import threading
from multiprocessing import Process

def copysch(RDP):
    PY_Location = os.path.join(os.getcwd(),'RDP.py')
    os.system("python %s %s" % (PY_Location,RDP) )

    global timer
    timer = threading.Timer(180.0, copysch, [RDP])
    timer.start()

def tm(rdp_id):
    timer = threading.Timer(3.0, copysch , [rdp_id])
    timer.start()


if __name__ == '__main__' :

    p1 = Process(target = tm, args = ('PREP_WM_RDP',))
    p2 = Process(target = tm, args=('PREP_BOOTS_RDP',))
    p3 = Process(target = tm, args=('PREP_PepsiCo_RDP',))
    p4 = Process(target=tm, args=('PREP_TARGET_RDP',))
    p5 = Process(target=tm, args=('PREP_MARS_RDP',))
    p6 = Process(target=tm, args=('PREP_AUX_RDP',))
    p7 = Process(target=tm, args=('PREP_HALEWOOD_RDP',))
    p8 = Process(target=tm, args=('PREP_PG_RDP',))


    p1.start()
    p2.start()
    p3.start()
    p4.start()
    p5.start()
    p6.start()
    p7.start()
    p8.start()






