# -*- coding: utf-8 -*-

from olap.xmla import xmla as xmla

olapServerURL = 'http://ENGV2AAAIOQA1.ENG.RSICORP.LOCAL/LIVE3/msmdpump.dll'
siloID = 'JNJ_TARGET_VICKY'
query = 'select {[Total Sales Amount]} on 0 , {[CALENDAR].[445].[Date]} on 1 from [JNJ_TARGET_VICKY]'

def Get_Measure_Value_From_Cube(olapServerURL,userName,pd,siloID,query):
    provider = xmla.XMLAProvider()
    con = provider.connect(location = olapServerURL,username = userName , password = pd)
    source = con.getOLAPSource()
    res = source.Execute(query, Catalog = siloID)
    return res.getSlice(properties ="FmtValue")


s = Get_Measure_Value_From_Cube(olapServerURL,'charlie.zhyang','88888888_Zx',siloID,query)
print(s)

