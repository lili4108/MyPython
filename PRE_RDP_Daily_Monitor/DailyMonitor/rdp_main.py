# -*- coding: utf-8 -*-

import PRE_RDP_Daily_Monitor.common.connectionmanager as cn
from sqlalchemy import Column, Integer, String, DateTime, Boolean, func, PrimaryKeyConstraint, ForeignKey
from sqlalchemy.orm import sessionmaker, aliased, scoped_session
import PRE_RDP_Daily_Monitor.common.DBOperations as DBO
import PRE_RDP_Daily_Monitor.common.configdb as cd
import unittest
import datetime
import json
import os

class RDPTest(unittest.TestCase):

    def setUp(self):

        self.defaultconfig = {
            'PREP_BOOTS_RDP':'PREZ2CDB2\DB1',
            'PREP_TARGET_RDP': 'PREZ2CDB2\DB1',
            'PREP_WM_RDP': 'PREZ2CDB2\DB1',
            'PREP_AUX_RDP': 'PREZ2CDB2\DB1',
            'PREP_MR_RDP': 'PREZ2CDB2\DB1',
            'PREP_PEPSICO_RDP': 'PREZ2CDB2\DB1'
        }

        rootdir = os.path.dirname(__file__)
        config = os.path.join(rootdir, 'ENV.json')
        file = open(config, 'r')
        config_j = json.load(file)
        self.defaultconfig.update(config_j)

        self.today = datetime.date.today()
        self.oneday = datetime.timedelta(days = 1)
        self.yesterday = self.today - self.oneday

    def tearDown(self):
        pass

    def test_rdp_fileloader(self):
            #con_vertica = DBO.DWAccessLayer(silo_id=conn._db_name, app_connection=conn)

        for db_name,rdp_server in self.defaultconfig.items():
            self.conn = DBO.AppAccessLayer(rdp_server=rdp_server, db_name=db_name)
            self.con_vertica = DBO.DWAccessLayer(silo_id=self.conn._db_name, app_connection=self.conn)
            self.session = self.conn.get_session()
            sql = 'select count(*) from ' + self.conn._db_name
            self.load_id = self.session.query(cd.DataStreamAudit).filter(cd.DataStreamAudit.CREATE_DATE > self.yesterday).filter(cd.DataStreamAudit.STATUS == 'complete').all()
            print('RDP ID : %s ---------------------------------' % self.conn._db_name)
            if len(self.load_id) == 1:
                print('there is %d file loaded complete today !!\n ' % len(self.load_id))
            else:
                print('there are %d files loaded complete today !!\n ' % len(self.load_id))
            for row in self.load_id:
                sql1 =  sql + '.' + row.RAW_TABLE + ' where load_id  = ' + str(row.LOAD_ID)
                count = self.con_vertica.query_with_result(sql1)[0]['count']
                if count == 0:
                    print('[0_ROW_RAW_COUNT]VERTICA  :load_id  %d , row_count  %d' % (row.LOAD_ID, count))
                    print('[0_ROW_RAW_COUNT]SQLSERVER:load_id  %d , row_count  %s' % (row.LOAD_ID, row.ROW_COUNT))
                elif row.ROW_COUNT != count:
                    print('[ROW_COUNT_NOT_SAME] VERTICA  :load_id  %d , row_count  %d' % (row.LOAD_ID,count))
                    print('[ROW_COUNT_NOT_SAME] SQLSERVER:load_id  %d , row_count  %s' % (row.LOAD_ID, row.ROW_COUNT))
                #self.assertEqual(row.ROW_COUNT,count)
            self.conn.close_connection()

            # variable = self.session.query(cd.RSIVariable).one()
            # print(variable.VARIABLE_NAME)
            # print(self.load_id.VARIABLE_NAME)


    def test_rdp_transformer(self):
            # _sql1 = 'select distinct da.load_id,tf.EVENT_KEY,lo.TABLE_NAME from datastream_audit da\
            #         join \
            #         TRANSFORMER_FILE_PERIOD_KEY  tf\
            #         ON da.LOAD_ID = tf.LOAD_ID\
            #         and da.DATASTREAM_SUB = tf.DATASTREAM_SUB\
            #         join FACT_TABLE_LOOKUP lo\
            #         on da.DATASTREAM_SUB = lo.FACT_TYPE\
            #         and tf.DATASTREAM_SUB = lo.FACT_TYPE\
            #         where da.create_date > convert(varchar,getdate() - 1,112)\
            #         and lo.VENDOR_KEY in (select VENDOR_KEY from DATASTREAM_AUDIT ' \
            #        'where create_date > convert(varchar,getdate() - 1,112))'
        for db_name, rdp_server in self.defaultconfig.items():
            self.conn = DBO.AppAccessLayer(rdp_server=rdp_server, db_name=db_name)
            self.con_vertica = DBO.DWAccessLayer(silo_id=self.conn._db_name, app_connection=self.conn)
            self.session = self.conn.get_session()
            #self.table_config_id = self.session.query(cd.MetaTableConfig).filter(cd.MetaT > self.yesterday).filter(cd.DataStreamAudit.STATUS == 'complete').all()
            self.load_id = self.session.query(cd.DataStreamAudit).filter(cd.DataStreamAudit.CREATE_DATE > self.yesterday).filter(cd.DataStreamAudit.STATUS == 'complete').all()
            _sql_wm = '''
                select distinct da.load_id,sm.VENDOR_SNAME,sm.RETAILER_SNAME,max(tf.EVENT_KEY) as EVENT_KEY,fl.TABLE_NAME 
                    from datastream_audit da
                    join 
                    TRANSFORMER_FILE_PERIOD_KEY  tf
                    ON da.LOAD_ID = tf.LOAD_ID
                    and da.DATASTREAM_SUB = tf.DATASTREAM_SUB
                    join transformer_event te
                    on tf.EVENT_KEY = te.EVENT_KEY 
                    join META_FILESET_FACTTYPE_MAPPING lo
                    on lo.file_set = te.FILE_SET	
                    join FACT_TABLE_LOOKUP fl
                    on lo.fact_type = fl.FACT_TYPE	
					join META_SUBVENDOR_MAPPING sm
					on  sm.VENDOR_KEY =  te.VENDOR_KEY
						and te.RETAILER_KEY = sm.RETAILER_KEY			
                    where da.create_date > convert(varchar,getdate()-1 ,112)
                    and da.VENDOR_KEY = (case da.VENDOR_KEY when -1 then -1 else fl.VENDOR_KEY  end  )
                    and (da.RETAILER_KEY = fl.RETAILER_KEY or da.DC_RETAILER_KEY = fl.RETAILER_KEY) 
                    and fl.TABLE_NAME LIKE '%fact%'
                    group by da.load_id,fl.TABLE_NAME,sm.VENDOR_SNAME,sm.RETAILER_SNAME
                    order by da.LOAD_ID
                    '''

            _sql = '''
                    select distinct '52' as vendor_key,te.RETAILER_KEY,tf.EVENT_KEY,te.file_set,fl.TABLE_NAME 
                    from datastream_audit da
                    join 
                    TRANSFORMER_FILE_PERIOD_KEY  tf
                    ON da.LOAD_ID = tf.LOAD_ID
                    and da.DATASTREAM_SUB = tf.DATASTREAM_SUB
                    join transformer_event te
                    on tf.EVENT_KEY = te.EVENT_KEY 
                    join META_FILESET_FACTTYPE_MAPPING lo
                    on lo.file_set = te.FILE_SET	
                    join FACT_TABLE_LOOKUP fl
                    on lo.fact_type = fl.FACT_TYPE	
					where da.create_date > convert(varchar,getdate()-1,112)
                    and da.VENDOR_KEY = (case da.VENDOR_KEY when -1 then -1 else fl.VENDOR_KEY  end  )
                    and (da.RETAILER_KEY = fl.RETAILER_KEY or da.DC_RETAILER_KEY = fl.RETAILER_KEY) 
            '''

            # _sql_event = '''
            #         select distinct max(tf.EVENT_KEY) as EVENT_KEY,fl.TABLE_NAME
            #         from datastream_audit da
            #         join
            #         TRANSFORMER_FILE_PERIOD_KEY  tf
            #         ON da.LOAD_ID = tf.LOAD_ID
            #         and da.DATASTREAM_SUB = tf.DATASTREAM_SUB
            #         join transformer_event te
            #         on tf.EVENT_KEY = te.EVENT_KEY
            #         join META_FILESET_FACTTYPE_MAPPING lo
            #         on lo.file_set = te.FILE_SET
            #         join FACT_TABLE_LOOKUP fl
            #         on lo.fact_type = fl.FACT_TYPE
            #         where da.create_date > convert(varchar,getdate() ,112)
            #         and da.VENDOR_KEY = (case da.VENDOR_KEY when -1 then -1 else fl.VENDOR_KEY  end  )
            #         group by fl.TABLE_NAME
            #             '''


            quer = self.conn.query_with_result(_sql)
            quer_wm = self.conn.query_with_result(_sql_wm)
            #quer_event = self.conn.query_with_result(_sql_event)
            # RETURN THE NUMBER OF LOAD_ID (COMPARE WITH GLOBAL VARIABLE WHETHER THE NUMBER IS SAME)
            COUNT_LOAD = quer.__len__()
            #COUNT_TRANSFORMER = quer_event.__len__()
            print('RDP_ID : %s   RDP_SERVER %s  !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!' % (db_name,rdp_server) )
            print('The count of event_key   : %d ' % COUNT_LOAD )
            #self.assertGreater(COUNT_LOAD, 1)

            sql = 'select count(*) from ' + self.conn._db_name
            RDP_TYPE = self.session.query(cd.RSICoreConfigProperty).filter(cd.RSICoreConfigProperty.Name == 'dw.db.rdptype').first()
            if RDP_TYPE.Value != 'wm' :
                for ROW in quer:
                    TABLE_NAME = ROW['TABLE_NAME']
                    EVENT_KEY = str(ROW['EVENT_KEY'])
                    FILE_SET = str(ROW['file_set'])
                    SQL_DS = sql + '.' + TABLE_NAME + ' where event_key = ' + EVENT_KEY
                    DS_COUNT = self.con_vertica.query_with_result(SQL_DS)[0]['count']
                    if DS_COUNT == 0:
                        print('[0_ROW_DS_COUNT]Event_key - %s , Table %s , File_set : %s : Count %d ' % (EVENT_KEY, TABLE_NAME, FILE_SET, DS_COUNT))
                    else:
                        print('[ROW_DS_COUNT]Event_key : %s , Table : %s , File_set : %s ' % (EVENT_KEY, TABLE_NAME , FILE_SET))
                        DS_COLUMNS = self.session.query(cd.MetaTableConfig).filter(
                            cd.MetaTableConfig.TABLE_NAME_FORMAT == TABLE_NAME).all()
                        for i in DS_COLUMNS:
                            print(i.TABLE_NAME_FORMAT,'     ',i.TABLE_CONFIG_ID)
                            Int_Columns = self.session.query(cd.MetaTableColumns).filter(cd.MetaTableColumns.TABLE_CONFIG_ID == i.TABLE_CONFIG_ID).all()
                            sql_p = ''
                            for c in Int_Columns:
                                if c.COLUMN_DATA_TYPE != 'varchar':
                                    #print('---------------------', c.COLUMN_NAME)
                                    sql_p = sql_p + 'MAX("' + c.COLUMN_NAME + '") "' + c.COLUMN_NAME + '",'
                            #print('select ',sql_p[0:-1],' from ', )
                            #print('select %s from %s.%s where event_key = %s ' % (sql_p[0:-1],self.conn._db_name,TABLE_NAME,EVENT_KEY))

                            sql_column = 'select ' + sql_p[0:-1] + ' from  ' + self.conn._db_name + '.' + TABLE_NAME + ' where event_key = ' + EVENT_KEY
                            print(sql_column)
                            sql_column_check = self.con_vertica.query_with_result(sql_column)
                            for co in sql_column_check:
                                print(co)
                            print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~')

            else:
                for ROW in quer_wm:
                    TABLE_NAME = ROW['TABLE_NAME']
                    EVENT_KEY = str(ROW['EVENT_KEY'])
                    VENDOR_SNAME = str(ROW['VENDOR_SNAME'])
                    RETAILER_SNAME = str(ROW['RETAILER_SNAME'])
                    SQL_DS = sql + '.' + TABLE_NAME + ' where event_key = ' + EVENT_KEY
                    DS_COUNT =  self.con_vertica.query_with_result(SQL_DS)[0]['count']
                    if DS_COUNT == 0 :
                        print('[0_ROW_DS_COUNT]Event_key - %s , Table %s , File_set : %s  : Count %d ' % (EVENT_KEY,TABLE_NAME, FILE_SET,DS_COUNT))
                    else:
                        print('[ROW_DS_COUNT]Event_key : %s , Table : %s , File_set : %s ' % (EVENT_KEY, TABLE_NAME, FILE_SET))
                        DS_TABLE_NAME = TABLE_NAME.replace(VENDOR_SNAME,'{VENDOR_KEY}').replace(RETAILER_SNAME,'{RETAILER_KEY}')
                        DS_COLUMNS = self.session.query(cd.MetaTableConfig).filter(cd.MetaTableConfig.TABLE_NAME_FORMAT == DS_TABLE_NAME).all()
                        for i in DS_COLUMNS:
                            print(i.TABLE_NAME_FORMAT,'     ',i.TABLE_CONFIG_ID)
                            Int_Columns = self.session.query(cd.MetaTableColumns).filter(
                                cd.MetaTableColumns.TABLE_CONFIG_ID == i.TABLE_CONFIG_ID).all()
                            sql_p = ''
                            for c in Int_Columns:
                                if c.COLUMN_DATA_TYPE != 'varchar':
                                    # print('---------------------', c.COLUMN_NAME)
                                    sql_p = sql_p + 'MAX("' + c.COLUMN_NAME + '") "' + c.COLUMN_NAME + '",'
                            # print('select ',sql_p[0:-1],' from ', )
                            # print('select %s from %s.%s where event_key = %s ' % (
                            # sql_p[0:-1], self.conn._db_name, TABLE_NAME, EVENT_KEY))

                            sql_column = 'select ' + sql_p[0:-1] + ' from  ' + self.conn._db_name + '.' + TABLE_NAME + ' where event_key = ' + EVENT_KEY
                            print(sql_column)
                            sql_column_check = self.con_vertica.query_with_result(sql_column)
                            for co in sql_column_check :
                                print(co)
                            print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~')


                    # self.table_config_id = self.session.query(cd.MetaTableConfig).filter(cd.MetaT > self.yesterday).filter(cd.DataStreamAudit.STATUS == 'complete').all()

                    #self.assertGreater(DS_COUNT,1)
            print()

            self.conn.close_connection()

    # def test_rdp_factupdate(self):
    #     try:
    #         pass
    #     except Exception as e:
    #         print(e)
    #     finally:
    #         pass




if __name__ == '__main__':
    # conn = cn.Connections(R'PREZ2CDB2\DB1','PREP_WM_RDP')
    # print(conn)
    #
    # Session = sessionmaker(conn.config_db_engine)
    # session = Session()
    # variable = session.query(RSIVariable).one()
    # name = session.query(FileMoverServer).all()
    # print(variable.VARIABLE_NAME)
    # for row in name:
    #     print(row.NAME,row.TYPE)

    # unittest.main()

    ts = unittest.TestSuite()
    ts1 = unittest.TestSuite([RDPTest("test_rdp_fileloader"),])
    ts2 = unittest.TestSuite([RDPTest("test_rdp_transformer"),])
    ts3 = unittest.TestSuite([RDPTest("test_rdp_fileloader"),RDPTest("test_rdp_transformer")])
    ts4 = unittest.TestSuite([ts1,ts2])

    runner = unittest.TextTestRunner()
    runner.run(ts2)

    # conn = DBO.AppAccessLayer(rdp_server = r'PREZ2CDB2\DB1',db_name = 'PREP_WM_RDP')
    # #conn = DBO.AppAccessLayer()
    # session = conn.get_session()
    # print(conn._rdp_server)
    # variable = session.query(cd.RSIVariable).one()
    # print(variable.VARIABLE_NAME)
    #
    # con_vertica = DBO.DWAccessLayer(silo_id = conn._db_name , app_connection= conn)
    # print(con_vertica)
    # sql = 'select * from ' + conn._db_name + '.DIM_VENDOR'
    # print(sql)
    # count = con_vertica.execute(sql)
    # print(count)
    # print(type(con_vertica))

    #def __init__(self, silo_id, app_connection, context=""):
