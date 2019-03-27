from scripts.common.DBOperations import AppAccessLayer
from scripts.common.DBOperations import DWAccessLayer
from scripts.common.Config import Config
from scripts.common.common import Log


class CreateDWTables:
    def __init__(self, rdp_server, app_id):
        self._app = AppAccessLayer(db_name=app_id, rdp_server=rdp_server)
        self._dw = DWAccessLayer(silo_id=app_id, app_connection=self._app)
        cfg = Config(app_connection=self._app, silo_id=app_id)
        self._rdp_schema = cfg.get_config(["dw.schema.name"])["dw.schema.name"]
        self._log = Log(self._app)

    def create_table(self):
        self._log.write_log("Create Vertica Tables", 9999999, "Create Vertica Tables", "STARTED")
        get_rdp_type = "SELECT UPPER(VALUE) AS RDP_TYPE FROM DBO.RSI_CORE_CFGPROPERTY " \
                       "WHERE NAME = 'dw.db.rdptype'"
        rdp_type = self._app.query_with_result(get_rdp_type)[0].get('RDP_TYPE')
        get_tables_wm_extra=''
        if rdp_type == 'WM':
            get_tables_wm_extra = "UNION SELECT DISTINCT ''	,''	,- 1	,- 1	,MTC.TABLE_CONFIG_ID," \
                                  "REPLACE(REPLACE(REPLACE(MTC1.TABLE_NAME_FORMAT, '{VENDOR_KEY}', " \
                                  "CASE WHEN ISNULL(WF.VENDOR_KEY, GT.VENDOR_KEY) = - 1 THEN '' " \
                                  "ELSE ISNULL((SELECT TOP 1 MSM.VENDOR_SNAME FROM DBO.META_SUBVENDOR_MAPPING MSM " \
                                  "WHERE MSM.VENDOR_KEY = ISNULL(WF.VENDOR_KEY, GT.VENDOR_KEY) ), '') " \
                                  "END), '{RETAILER_KEY}', ISNULL((SELECT TOP 1 MSM.RETAILER_SNAME " \
                                  "FROM DBO.META_SUBVENDOR_MAPPING MSM " \
                                  "WHERE MSM.RETAILER_KEY = ISNULL(WF.RETAILER_KEY, GT.RETAILER_KEY)),'')), " \
                                  "'{PARALLEL_INSTANCES}', MW.PARALLEL_INSTANCES) TABLE_NAME," \
                                  "'Create_Month' AS PARTITION_CLAUSE, MTC1.TABLE_TYPE " \
                                  "FROM dbo.META_WORKFLOW_FILESET wf INNER JOIN dbo.GROUPER_FILESET_TIMEWINDOW ft " \
                                  "ON ft.FILE_SET = wf.FILE_SET INNER JOIN dbo.GROUPER_TIMEWINDOW gt " \
                                  "ON gt.WINDOW_NAME = ft.WINDOW_NAME INNER JOIN META_WORKFLOW_TABLES ta " \
                                  "ON ta.WORKFLOW_ID = wf.WORKFLOW_ID JOIN expanded mw " \
                                  "ON mw.WORKFLOW_ID = wf.WORKFLOW_ID INNER JOIN dbo.META_TABLE_CONFIG mtc " \
                                  "ON mtc.TABLE_CONFIG_ID = ta.TABLE_CONFIG_ID " \
                                  "INNER JOIN ( SELECT DISTINCT CASE WHEN TABLE_NAME_FORMAT LIKE '%WMSSC%'	" \
                                  "AND TABLE_TYPE = 'ds' THEN 'WMSSC' " \
                                  "WHEN TABLE_TYPE = 'ds' AND TABLE_NAME_FORMAT LIKE '%WMINTL%' THEN 'WMINTL' " \
                                  "WHEN TABLE_TYPE = 'ds' AND TABLE_NAME_FORMAT LIKE '%SASSC%' THEN 'SASSC' " \
                                  "WHEN TABLE_TYPE = 'ds' AND TABLE_NAME_FORMAT LIKE '%WMCAT%' THEN 'WMCAT' " \
                                  "WHEN TABLE_TYPE = 'ds' AND (TABLE_NAME_FORMAT NOT LIKE '%WMSSC%' " \
                                  "AND TABLE_NAME_FORMAT NOT LIKE '%WMINTL%' " \
                                  "AND TABLE_NAME_FORMAT NOT LIKE '%SASSC%' " \
                                  "AND TABLE_NAME_FORMAT NOT LIKE '%WMCAT%' ) " \
                                  "THEN 'OTHER' END AS type ,TABLE_NAME_FORMAT FROM META_TABLE_CONFIG " \
                                  "WHERE TABLE_TYPE = 'DS' ) TTT " \
                                  "ON MTC.TABLE_NAME_FORMAT LIKE '%' + TTT.type + '%' " \
                                  "INNER JOIN dbo.META_TABLE_CONFIG MTC1 " \
                                  "ON MTC1.TABLE_NAME_FORMAT LIKE '%' + TTT.type + '%' " \
                                  "WHERE WF.ACTIVE = 1 AND MTC1.TABLE_TYPE = 'DS' AND MTC.TABLE_TYPE = 'DS' " \
                                  "UNION SELECT DISTINCT  '','',-1, -1,MTC.TABLE_CONFIG_ID," \
                                  "REPLACE(REPLACE(REPLACE(MTC.TABLE_NAME_FORMAT, '{VENDOR_KEY}', CASE " \
                                  "WHEN ISNULL(WF.VENDOR_KEY, GT.VENDOR_KEY) = - 1 THEN '' ELSE ISNULL(( " \
                                  "SELECT TOP 1 VENDOR_SNAME FROM DBO.META_SUBVENDOR_MAPPING " \
                                  "WHERE VENDOR_KEY = ISNULL(WF.VENDOR_KEY, GT.VENDOR_KEY) ), '') " \
                                  "END), '{RETAILER_KEY}', ISNULL((SELECT TOP 1 RETAILER_SNAME " \
                                  "FROM DBO.META_SUBVENDOR_MAPPING " \
                                  "WHERE RETAILER_KEY = ISNULL(WF.RETAILER_KEY, GT.RETAILER_KEY) ),'')), " \
                                  "'{PARALLEL_INSTANCES}',  MW.PARALLEL_INSTANCES) TABLE_NAME," \
                                  "'Create_Month' AS PARTITION_CLAUSE, " \
                                  "MTC.TABLE_TYPE FROM dbo.META_WORKFLOW_FILESET wf " \
                                  "INNER JOIN dbo.GROUPER_FILESET_TIMEWINDOW ft ON ft.FILE_SET = wf.FILE_SET " \
                                  "INNER JOIN dbo.GROUPER_TIMEWINDOW gt ON gt.WINDOW_NAME = ft.WINDOW_NAME " \
                                  "JOIN META_WORKFLOW_TABLES ta ON ta.WORKFLOW_ID = wf.WORKFLOW_ID " \
                                  "INNER JOIN expanded mw ON mw.WORKFLOW_ID = wf.WORKFLOW_ID " \
                                  "JOIN dbo.META_TABLE_CONFIG mtc ON mtc.TABLE_CONFIG_ID = ta.TABLE_CONFIG_ID " \
                                  "JOIN (SELECT DISTINCT CASE WHEN TABLE_NAME_FORMAT like '%WMSSC%' " \
                                  "and TABLE_TYPE = 'ds' THEN 'WMSSC' " \
                                  "WHEN TABLE_TYPE = 'ds' AND TABLE_NAME_FORMAT like '%WMINTL%' THEN 'WMINTL' " \
                                  "WHEN TABLE_TYPE = 'ds' AND TABLE_NAME_FORMAT like '%SASSC%' THEN 'SASSC' " \
                                  "WHEN TABLE_TYPE = 'ds' AND TABLE_NAME_FORMAT like '%WMCAT%' THEN 'WMCAT' " \
                                  "WHEN TABLE_TYPE = 'ds' AND (TABLE_NAME_FORMAT NOT like '%WMSSC%' " \
                                  "AND TABLE_NAME_FORMAT NOT like '%WMINTL%' " \
                                  "AND TABLE_NAME_FORMAT NOT like '%SASSC%' " \
                                  "AND TABLE_NAME_FORMAT NOT like '%WMCAT%') THEN 'OTHER' " \
                                  "END AS type, TABLE_NAME_FORMAT " \
                                  "FROM META_TABLE_CONFIG WHERE TABLE_TYPE = 'DS') TTT " \
                                  "ON mtc.TABLE_NAME_FORMAT = TTT.TABLE_NAME_FORMAT " \
                                  "AND TTT.type = 'OTHER' "\

        get_tables = "WITH expanded AS ( SELECT WORKFLOW_ID, PARALLEL_INSTANCES  FROM dbo.META_WORKFLOW " \
                     "UNION ALL SELECT WORKFLOW_ID, PARALLEL_INSTANCES -1 FROM expanded " \
                     "WHERE PARALLEL_INSTANCES > 1) " \
                     "SELECT DISTINCT A.*  INTO #AllTables FROM (SELECT wf.WORKFLOW_ID, wf.FILE_SET, ta.TABLE_CONFIG_ID, " \
                     "wf.VENDOR_KEY, wf.RETAILER_KEY, REPLACE(REPLACE(REPLACE(MTC.TABLE_NAME_FORMAT,'{VENDOR_KEY}', " \
                     "CASE WHEN ISNULL (WF.VENDOR_KEY,GT.VENDOR_KEY) = -1 " \
                     "THEN '' ELSE ISNULL((SELECT TOP 1 VENDOR_SNAME FROM DBO.META_SUBVENDOR_MAPPING " \
                     "WHERE VENDOR_KEY = ISNULL (WF.VENDOR_KEY,GT.VENDOR_KEY)), '') END), '{RETAILER_KEY}', " \
                     "ISNULL((SELECT TOP 1 RETAILER_SNAME FROM DBO.META_SUBVENDOR_MAPPING  " \
                     "WHERE RETAILER_KEY = ISNULL (WF.RETAILER_KEY,GT.RETAILER_KEY)),'')),  '{PARALLEL_INSTANCES}'," \
                     "MW.PARALLEL_INSTANCES) TABLE_NAME, PARTITION_CLAUSE, mtc.TABLE_TYPE " \
                     "FROM dbo.META_WORKFLOW_FILESET wf INNER JOIN dbo.GROUPER_FILESET_TIMEWINDOW ft " \
                     "ON ft.FILE_SET = wf.FILE_SET INNER JOIN dbo.GROUPER_TIMEWINDOW gt " \
                     "ON gt.WINDOW_NAME = ft.WINDOW_NAME JOIN META_WORKFLOW_TABLES ta " \
                     "ON ta.WORKFLOW_ID = wf.WORKFLOW_ID  JOIN dbo.META_TABLE_CONFIG mtc " \
                     "ON mtc.TABLE_CONFIG_ID = ta.TABLE_CONFIG_ID JOIN expanded mw " \
                     "ON mw.WORKFLOW_ID = wf.WORKFLOW_ID  WHERE wf.ACTIVE=1) A  " \
                     "UNION SELECT '','',-1, -1,TABLE_CONFIG_ID,TABLE_NAME_FORMAT, " \
                     "CASE WHEN EXISTS (SELECT 1 FROM META_TABLE_COLUMNS C " \
                     "WHERE C.TABLE_CONFIG_ID=T.TABLE_CONFIG_ID AND COLUMN_NAME='LOAD_ID')  " \
                     "THEN NULLIF(PARTITION_CLAUSE, ' ') ELSE 'Create_Month' END AS PARTITION_CLAUSE,  " \
                     "TABLE_TYPE  FROM META_TABLE_CONFIG T  WHERE TABLE_TYPE='RAW' "
        # IF RDP TYPE is WM then append the WM related Query with the regular query.
        if get_tables_wm_extra:
            get_tables += get_tables_wm_extra
        self._log.write_log("Create Vertica Tables", 9999999, "Create Vertica Tables: " + get_tables, "QUERY INFO")
        self._app.execute(get_tables)
        get_tables = "SELECT * FROM #AllTables"
        # Get the distinct list of TABLES
        tables = self._app.query_with_result(get_tables)
        all_tables = list(set([i.get('TABLE_NAME') for i in tables]))
        comma_separated_tables = "'" + "','".join(all_tables) + "'"
        insert_table_config_qry = "WITH expanded AS (SELECT WORKFLOW_ID, PARALLEL_INSTANCES " \
                                  "FROM dbo.META_WORKFLOW UNION ALL SELECT WORKFLOW_ID, PARALLEL_INSTANCES-1 " \
                                  "FROM expanded WHERE PARALLEL_INSTANCES > 1 ) " \
                                  "SELECT A.*  FROM ( SELECT  DISTINCT " \
                                  "mtc.TABLE_CONFIG_ID, REPLACE(REPLACE(REPLACE(MTC.TABLE_NAME_FORMAT, " \
                                  "'{VENDOR_KEY}', CASE WHEN ISNULL(WF.VENDOR_KEY, GT.VENDOR_KEY) = -1 " \
                                  "THEN '' ELSE ISNULL((SELECT TOP 1 VENDOR_SNAME FROM DBO.META_SUBVENDOR_MAPPING " \
                                  "WHERE VENDOR_KEY = ISNULL (WF.VENDOR_KEY,GT.VENDOR_KEY)), '') END), " \
                                  "'{RETAILER_KEY}', " \
                                  "ISNULL((SELECT TOP 1 RETAILER_SNAME FROM DBO.META_SUBVENDOR_MAPPING " \
                                  "WHERE RETAILER_KEY = ISNULL (WF.RETAILER_KEY,GT.RETAILER_KEY)),'')), " \
                                  "'{PARALLEL_INSTANCES}'," \
                                  "MW.PARALLEL_INSTANCES) TABLE_NAME, TABLE_TYPE, PARTITION_CLAUSE, " \
                                  "LTRIM(RTRIM(COLUMN_NAME)) AS COLUMN_NAME, COLUMN_DATA_TYPE, " \
                                  "COLUMN_DATA_STYLE, COLUMN_CONSTRAINT, " \
                                  "COLUMN_DEFAULT, COLUMN_POSITION, IS_PRIMARY_KEY, SEGMENTATION_POSITION, " \
                                  "ORDER_BY_POSITION FROM dbo.META_WORKFLOW_FILESET wf " \
                                  "INNER JOIN dbo.GROUPER_FILESET_TIMEWINDOW ft ON ft.FILE_SET = wf.FILE_SET " \
                                  "INNER JOIN dbo.GROUPER_TIMEWINDOW gt ON gt.WINDOW_NAME = ft.WINDOW_NAME " \
                                  "JOIN META_WORKFLOW_TABLES ta ON ta.WORKFLOW_ID = wf.WORKFLOW_ID " \
                                  "JOIN dbo.META_TABLE_CONFIG mtc ON mtc.TABLE_CONFIG_ID = ta.TABLE_CONFIG_ID " \
                                  "JOIN dbo.META_TABLE_COLUMNS tc ON tc.TABLE_CONFIG_ID = mtc.TABLE_CONFIG_ID " \
                                  "JOIN expanded mw ON mw.WORKFLOW_ID = wf.WORKFLOW_ID " \
                                  "WHERE wf.ACTIVE = 1" \
                                  ") A " \
                                  "WHERE a.TABLE_NAME IN (SELECT DISTINCT TABLE_NAME FROM #AllTables)" " " \
                                  " UNION SELECT T.TABLE_CONFIG_ID,T.TABLE_NAME_FORMAT,T.TABLE_TYPE," \
                                  "'Create_Month' PARTITION_CLAUSE, 'LOAD_ID' COLUMN_NAME, 'INT' COLUMN_DATA_TYPE," \
                                  "NULL COLUMN_DATA_STYLE, 'NOT NULL ENCODING RLE' COLUMN_CONSTRAINT," \
                                  "NULL COLUMN_DEFAULT,1 COLUMN_POSITION,NULL IS_PRIMARY_KEY," \
                                  "1 SEGMENTATION_POSITION,1 ORDER_BY_POSITION FROM META_TABLE_CONFIG T " \
                                  "WHERE NOT EXISTS (SELECT 1 FROM META_TABLE_COLUMNS C " \
                                  "WHERE C.TABLE_CONFIG_ID=T.TABLE_CONFIG_ID AND COLUMN_NAME='LOAD_ID') " \
                                  "AND T.TABLE_TYPE='RAW' " \
                                  "AND T.TABLE_NAME_FORMAT IN (SELECT DISTINCT TABLE_NAME FROM #AllTables)" " " \
                                  " UNION SELECT T.TABLE_CONFIG_ID,T.TABLE_NAME_FORMAT,T.TABLE_TYPE," \
                                  "'Create_Month' PARTITION_CLAUSE, 'ROW_NUMBER' COLUMN_NAME, " \
                                  "'INT' COLUMN_DATA_TYPE,NULL COLUMN_DATA_STYLE, " \
                                  "'NOT NULL ENCODING RLE' COLUMN_CONSTRAINT, NULL COLUMN_DEFAULT," \
                                  "2 COLUMN_POSITION,NULL IS_PRIMARY_KEY,NULL SEGMENTATION_POSITION," \
                                  "NULL ORDER_BY_POSITION FROM META_TABLE_CONFIG T " \
                                  "WHERE NOT EXISTS (SELECT 1 FROM META_TABLE_COLUMNS C " \
                                  "WHERE C.TABLE_CONFIG_ID=T.TABLE_CONFIG_ID AND COLUMN_NAME='LOAD_ID') " \
                                  "AND T.TABLE_TYPE='RAW' " \
                                  "AND T.TABLE_NAME_FORMAT IN (SELECT DISTINCT TABLE_NAME FROM #AllTables)" " " \
                                  " UNION  SELECT T.TABLE_CONFIG_ID,T.TABLE_NAME_FORMAT,T.TABLE_TYPE," \
                                  "'Create_Month' PARTITION_CLAUSE, 'Create_Month' COLUMN_NAME, " \
                                  "'INT' COLUMN_DATA_TYPE,NULL COLUMN_DATA_STYLE, " \
                                  "'NOT NULL ENCODING RLE' COLUMN_CONSTRAINT, " \
                                  "'CAST(to_char (CURRENT_DATE(),''YYYYMM'') AS INT)' COLUMN_DEFAULT," \
                                  "3 COLUMN_POSITION,NULL IS_PRIMARY_KEY,NULL SEGMENTATION_POSITION," \
                                  "NULL ORDER_BY_POSITION FROM META_TABLE_CONFIG T " \
                                  "WHERE NOT EXISTS (SELECT 1 FROM META_TABLE_COLUMNS C " \
                                  "WHERE C.TABLE_CONFIG_ID=T.TABLE_CONFIG_ID AND COLUMN_NAME='LOAD_ID') " \
                                  "AND T.TABLE_TYPE='RAW' " \
                                  "AND T.TABLE_NAME_FORMAT IN (SELECT DISTINCT TABLE_NAME FROM #AllTables)" " " \
                                  " UNION SELECT T.TABLE_CONFIG_ID, " \
                                  "T.TABLE_NAME_FORMAT,T.TABLE_TYPE,CASE " \
                                  "WHEN EXISTS (SELECT 1 FROM META_TABLE_COLUMNS C " \
                                  "WHERE C.TABLE_CONFIG_ID=T.TABLE_CONFIG_ID AND COLUMN_NAME='LOAD_ID')  " \
                                  "THEN NULLIF(PARTITION_CLAUSE, ' ') ELSE 'Create_Month' END AS PARTITION_CLAUSE, " \
                                  "LTRIM(RTRIM(C.COLUMN_NAME)) AS COLUMN_NAME," \
                                  "C.COLUMN_DATA_TYPE,C.COLUMN_DATA_STYLE,C.COLUMN_CONSTRAINT, C.COLUMN_DEFAULT," \
                                  "C.COLUMN_POSITION+3 COLUMN_POSITION,C.IS_PRIMARY_KEY,C.SEGMENTATION_POSITION," \
                                  "C.ORDER_BY_POSITION+1 ORDER_BY_POSITION FROM META_TABLE_CONFIG T JOIN " \
                                  "META_TABLE_COLUMNS C ON T.TABLE_CONFIG_ID=C.TABLE_CONFIG_ID " \
                                  "WHERE T.TABLE_TYPE='RAW' " \
                                  "AND T.TABLE_NAME_FORMAT IN (SELECT DISTINCT TABLE_NAME FROM #AllTables)" " "
        if rdp_type == 'WM':
            wm_table_type = "SELECT DISTINCT " \
                            "CASE WHEN TABLE_NAME_FORMAT LIKE '%WMSSC%' AND TABLE_TYPE = 'ds' THEN 'WMSSC' " \
                            "WHEN TABLE_TYPE = 'ds' AND TABLE_NAME_FORMAT LIKE '%WMINTL%' THEN 'WMINTL' " \
                            "WHEN TABLE_TYPE = 'ds' AND TABLE_NAME_FORMAT LIKE '%SASSC%' THEN 'SASSC' " \
                            "WHEN TABLE_TYPE = 'ds' AND TABLE_NAME_FORMAT LIKE '%WMCAT%' THEN 'WMCAT' " \
                            "WHEN TABLE_TYPE = 'ds' AND ( TABLE_NAME_FORMAT NOT LIKE '%WMSSC%' " \
                            "AND TABLE_NAME_FORMAT NOT LIKE '%WMINTL%' AND TABLE_NAME_FORMAT NOT LIKE '%SASSC%' " \
                            "AND TABLE_NAME_FORMAT NOT LIKE '%WMCAT%' ) THEN 'OTHER' END AS type, " \
                            "TABLE_NAME_FORMAT INTO #WM_TABLE_TYPE " \
                            "FROM META_TABLE_CONFIG WHERE TABLE_TYPE = 'DS'"
            self._app.execute(wm_table_type)
            wm_all_table = ";WITH expanded AS (SELECT WORKFLOW_ID, PARALLEL_INSTANCES FROM dbo.META_WORKFLOW " \
                           "UNION ALL SELECT WORKFLOW_ID, PARALLEL_INSTANCES-1 FROM expanded " \
                           "WHERE PARALLEL_INSTANCES > 1 ) SELECT DISTINCT mtc1.TABLE_CONFIG_ID, " \
                           "MTC1.TABLE_NAME_FORMAT, MW.PARALLEL_INSTANCES, WF.VENDOR_KEY AS WF_VENDOR_KEY, " \
                           "GT.VENDOR_KEY AS GT_VENDOR_KEY, WF.RETAILER_KEY AS WF_RETAILER_KEY, " \
                           "GT.RETAILER_KEY AS GT_RETAILER_KEY, MTC1.TABLE_TYPE , MTC1.PARTITION_CLAUSE , " \
                           "COLUMN_NAME, COLUMN_DATA_TYPE, COLUMN_DATA_STYLE, COLUMN_CONSTRAINT, COLUMN_DEFAULT, " \
                           "COLUMN_POSITION,IS_PRIMARY_KEY,SEGMENTATION_POSITION,ORDER_BY_POSITION INTO #ALL_WM_TABLES " \
                           "FROM dbo.META_WORKFLOW_FILESET wf JOIN dbo.GROUPER_FILESET_TIMEWINDOW ft " \
                           "ON ft.FILE_SET = wf.FILE_SET JOIN dbo.GROUPER_TIMEWINDOW gt " \
                           "ON gt.WINDOW_NAME = ft.WINDOW_NAME INNER JOIN META_WORKFLOW_TABLES ta " \
                           "ON ta.WORKFLOW_ID = wf.WORKFLOW_ID JOIN expanded mw ON mw.WORKFLOW_ID = wf.WORKFLOW_ID " \
                           "INNER JOIN dbo.META_TABLE_CONFIG mtc ON mtc.TABLE_CONFIG_ID = ta.TABLE_CONFIG_ID " \
                           "INNER JOIN #WM_TABLE_TYPE TTT ON MTC.TABLE_NAME_FORMAT LIKE '%' + TTT.type + '%' " \
                           "INNER JOIN META_TABLE_CONFIG MTC1 ON MTC1.TABLE_NAME_FORMAT LIKE '%' + TTT.type + '%' " \
                           "INNER JOIN dbo.META_TABLE_COLUMNS MTCO ON MTC1.TABLE_CONFIG_ID = MTCO.TABLE_CONFIG_ID " \
                           "WHERE WF.ACTIVE = 1 AND MTC1.TABLE_TYPE = 'DS' AND MTC.TABLE_TYPE = 'DS'"
            self._app.execute(wm_all_table)
            insert_table_config_qry_wm_extras = "UNION SELECT DISTINCT TABLE_CONFIG_ID, " \
                                                "REPLACE(REPLACE(REPLACE(TABLE_NAME_FORMAT, '{VENDOR_KEY}', " \
                                                "CASE WHEN ISNULL(WF_VENDOR_KEY, GT_VENDOR_KEY) = - 1 THEN '' " \
                                                "ELSE ISNULL ( ( SELECT TOP 1 t.VENDOR_SNAME " \
                                                "FROM DBO.META_SUBVENDOR_MAPPING t " \
                                                "WHERE t.VENDOR_KEY = ISNULL(WF_VENDOR_KEY,GT_VENDOR_KEY)),'') END), " \
                                                "'{RETAILER_KEY}', ISNULL(( SELECT TOP 1 t.RETAILER_SNAME " \
                                                "FROM DBO.META_SUBVENDOR_MAPPING t " \
                                                "WHERE t.RETAILER_KEY = ISNULL(WF_RETAILER_KEY,GT_RETAILER_KEY)),'')), " \
                                                "'{PARALLEL_INSTANCES}', PARALLEL_INSTANCES) TABLE_NAME,TABLE_TYPE, " \
                                                "PARTITION_CLAUSE, COLUMN_NAME, COLUMN_DATA_TYPE, COLUMN_DATA_STYLE, " \
                                                "COLUMN_CONSTRAINT, COLUMN_DEFAULT, COLUMN_POSITION, IS_PRIMARY_KEY, " \
                                                "SEGMENTATION_POSITION, ORDER_BY_POSITION " \
                                                "FROM #ALL_WM_TABLES"
            insert_table_config_qry += insert_table_config_qry_wm_extras + " ORDER BY TABLE_CONFIG_ID, COLUMN_POSITION "
        else:
            insert_table_config_qry += " ORDER BY TABLE_CONFIG_ID, COLUMN_POSITION "
        config_res = self._app.query_with_result(insert_table_config_qry)
        self._log.write_log("Create Vertica Tables", 9999999, insert_table_config_qry, "QUERY INFO")
        vertica_existing_table_config = "SELECT UPPER(T.TABLE_NAME) AS TABLE_NAME, " \
                                        "UPPER(C.COLUMN_NAME) AS COLUMN_NAME, " \
                                        "SPLIT_PART(C.DATA_TYPE, '(',1) DATA_TYPE, C.DATA_TYPE_LENGTH " \
                                        "FROM TABLES T JOIN COLUMNS C ON T.TABLE_ID=C.TABLE_ID " \
                                        "WHERE T.TABLE_SCHEMA = '{TABLE_SCHEMA}' " \
                                        "AND T.TABLE_NAME IN ({TABLE_NAME})".format(TABLE_SCHEMA=self._rdp_schema,
                                                                                    TABLE_NAME=comma_separated_tables)
        vertica_table_config_out = self._dw.query_with_result(vertica_existing_table_config)
        # Find the Delta (Both New table and New Column)
        check = set([(d['TABLE_NAME'], d['COLUMN_NAME']) for d in vertica_table_config_out])
        all_config = [d for d in config_res if (d['TABLE_NAME'].upper(), d['COLUMN_NAME'].upper()) not in check]
        # all_config = list(itertools.chain({**t, **c} for c in config for t in tables
                                          #  if t['TABLE_NAME'] == c['TABLE_NAME']))

        # All New Tables
        table_check = set([(d['TABLE_NAME'].upper()) for d in vertica_table_config_out])
        only_new_tables = [d for d in all_config if (d['TABLE_NAME'].upper()) not in table_check]
        # All New Columns
        only_new_columns = [x for x in all_config if x not in only_new_tables]

        # Define the Key List
        key_list = ['WORKFLOW_ID', 'TABLE_CONFIG_ID', 'VENDOR_KEY', 'RETAILER_KEY', 'TABLE_NAME',
                    'PARTITION_CLAUSE']

        # Enter into the Loop if there is any new table to be created
        if only_new_tables:
            new_list = [{k: v for k, v in i.items() if k in key_list} for i in only_new_tables]
            # Contain all distinct list of dict
            final_table_config = [i for n, i in enumerate(new_list) if i not in new_list[n + 1:]]
            for new_tables in final_table_config:
                primary_key_cols = ''
                segmented_clause = {}
                column_string = ''
                primary_keys = ''
                order_by_cond = ''
                segmented_cond = ''
                order_by = {}
                partition_clause = ''
                # Get only the required columns for the input Table (new)
                working_list_of_dict = [d for d in only_new_tables if d['TABLE_NAME'] == new_tables.get('TABLE_NAME')]
                for new_columns in working_list_of_dict:
                    cond_list = '"' + new_columns.get("COLUMN_NAME") + '" ' + new_columns.get("COLUMN_DATA_TYPE") + ""
                    if new_columns.get("COLUMN_DATA_STYLE") is not None:
                        cond_list += "(" + new_columns.get("COLUMN_DATA_STYLE") + ")"
                    # Add Primary Key to teh table if any
                    if new_columns.get("IS_PRIMARY_KEY") is not None:
                        primary_keys += ', "' + new_columns.get("COLUMN_NAME") + '"'
                    # Add Default Constraint to the table if any
                    if new_columns.get("COLUMN_DEFAULT") is not None:
                        cond_list += " DEFAULT " + new_columns.get("COLUMN_DEFAULT") + ""
                    # Add Column Constraint to the table if any
                    if new_columns.get("COLUMN_CONSTRAINT") is not None:
                        cond_list += " " + new_columns.get("COLUMN_CONSTRAINT") + ""
                    # Add all the generated conditions
                    column_string += ", " + cond_list
                    if new_columns.get("ORDER_BY_POSITION") is not None:
                        order_by[new_columns.get("ORDER_BY_POSITION")] = '"' + new_columns.get("COLUMN_NAME") + '"'
                    if new_columns.get("SEGMENTATION_POSITION") is not None:
                        segmented_clause[new_columns.get("SEGMENTATION_POSITION")] = '"' + new_columns.get("COLUMN_NAME") + '"'

                if primary_keys:
                    primary_key_cols = ", PRIMARY KEY (" + primary_keys[2:] + ")"

                for col_position in sorted(order_by):
                    if order_by_cond == "":
                        order_by_cond = order_by[col_position]
                    else:
                        order_by_cond += "," + order_by[col_position]

                for segment_position in sorted(segmented_clause):
                    if segmented_cond == "":
                        segmented_cond = segmented_clause[segment_position]
                    else:
                        segmented_cond += "," + segmented_clause[segment_position]

                if segmented_cond:
                    segmented_columns = 'SEGMENTED BY MODULARHASH (' + segmented_cond + ") ALL NODES"
                else:
                    segmented_columns = 'UNSEGMENTED ALL NODES'

                # Add Partition clause to the Create table script
                if new_tables.get("PARTITION_CLAUSE"):
                    partition_clause = "PARTITION BY " + new_tables.get("PARTITION_CLAUSE")

                # If ORDER BY condition specified
                if order_by_cond:
                    dw_create_table = 'CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.{TABLE_NAME} ({COLUMN_STR} ' \
                                      '{PRIMARY_KEYS}) ORDER BY {ORDER_BY_COND} {SEGMENT} {PARTITION}'. \
                        format(SCHEMA_NAME=self._rdp_schema, TABLE_NAME=new_tables.get("TABLE_NAME"),
                               COLUMN_STR=column_string[2:], PRIMARY_KEYS=primary_key_cols,
                               ORDER_BY_COND=order_by_cond, SEGMENT=segmented_columns, PARTITION=partition_clause)
                else:
                    # If there is no ORDER BY condition specified
                    dw_create_table = 'CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.{TABLE_NAME} ({COLUMN_STR}) ' \
                                      '{SEGMENT} {PARTITION};'. \
                        format(SCHEMA_NAME=self._rdp_schema, TABLE_NAME=new_tables.get("TABLE_NAME"),
                               COLUMN_STR=column_string[2:],
                               SEGMENT=segmented_columns, PARTITION=partition_clause)
                # Create table(s) in DW
                self._dw.execute(dw_create_table)
                # Delete the List of dict
                del working_list_of_dict
                self._log.write_log("Create Vertica Tables", 9999999,
                                    "Table Created: " + self._rdp_schema + "." + new_tables.get("TABLE_NAME"),
                                    "COMPLETE")

        # Enter into the Loop if there is any new column
        if only_new_columns:
            all_tables = [{k: v for k, v in i.items() if k in key_list} for i in only_new_columns]
            # Contain all distinct list of dict
            distinct_tables = [i for n, i in enumerate(all_tables) if i not in all_tables[n + 1:]]
            for table in distinct_tables:
                new_table_columns = [d for d in only_new_columns if d['TABLE_NAME'] == table.get('TABLE_NAME')]
                for table_columns in new_table_columns:
                    cond_list = table_columns.get("COLUMN_DATA_TYPE")
                    if table_columns.get("COLUMN_DATA_STYLE") is not None:
                        cond_list += "(" + table_columns.get("COLUMN_DATA_STYLE") + ")"

                    if table_columns.get("COLUMN_DEFAULT") is not None:
                        cond_list += " DEFAULT " + table_columns.get("COLUMN_DEFAULT") + ""

                    # Add Column Constraint to the table if any
                    if table_columns.get("COLUMN_CONSTRAINT") is not None:
                        cond_list += " " + table_columns.get("COLUMN_CONSTRAINT") + ""

                    alter_stmt = "ALTER TABLE {SCHEMA_NAME}.{TABLE_NAME} " \
                                 "ADD COLUMN \"{COLUMN_NAME}\" {CONDITION}; ".\
                        format(SCHEMA_NAME=self._rdp_schema, TABLE_NAME=table.get('TABLE_NAME'),
                               COLUMN_NAME=table_columns.get('COLUMN_NAME'), CONDITION=cond_list)
                    # Execute the ALTER stmt
                    self._log.write_log("Create Vertica Tables", 9999999,
                                        "Alter Table Statement: " + alter_stmt, "IN PROGRESS")
                    self._dw.execute(alter_stmt)
                    self._log.write_log("Create Vertica Tables", 9999999,
                                        "Alter Table Statement: " + alter_stmt, "COMPLETE")

        # Verify TABLE_AUDIT is sync as per the Tables created in Vertica
        for tab in tables:
            if tab.get("FILE_SET"):
                get_instance_id = "SELECT INSTANCE_ID FROM dbo.META_WORKFLOW_TABLES wf " \
                                  "JOIN dbo.META_WORKFLOW_FILESET f ON f.WORKFLOW_ID = wf.WORKFLOW_ID " \
                                  "WHERE f.ACTIVE = 1 AND wf.WORKFLOW_ID={WORKFLOW_ID} " \
                                  "AND wf.TABLE_CONFIG_ID={CONFIG_ID} " \
                                  "AND f.VENDOR_KEY = {VENDOR_KEY} AND f.RETAILER_KEY = {RETAILER_KEY} " \
                                  "AND f.FILE_SET='{FILE_SET}'".format(WORKFLOW_ID=tab.get("WORKFLOW_ID"),
                                                                       CONFIG_ID=tab.get("TABLE_CONFIG_ID"),
                                                                       VENDOR_KEY=tab.get("VENDOR_KEY"),
                                                                       RETAILER_KEY=tab.get("RETAILER_KEY"),
                                                                       FILE_SET=tab.get("FILE_SET"))
            else:
                get_instance_id = "SELECT INSTANCE_ID FROM dbo.META_WORKFLOW_TABLES wf " \
                                  "JOIN dbo.META_WORKFLOW_FILESET f ON f.WORKFLOW_ID = wf.WORKFLOW_ID " \
                                  "WHERE f.ACTIVE = 1 AND wf.WORKFLOW_ID={WORKFLOW_ID} " \
                                  "AND wf.TABLE_CONFIG_ID={CONFIG_ID} " \
                                  "AND f.VENDOR_KEY={VENDOR_KEY} AND f.RETAILER_KEY={RETAILER_KEY}". \
                    format(WORKFLOW_ID=tab.get("WORKFLOW_ID"), CONFIG_ID=tab.get("TABLE_CONFIG_ID"),
                           VENDOR_KEY=tab.get("VENDOR_KEY"), RETAILER_KEY=tab.get("RETAILER_KEY"))
            instance = self._app.query_with_result(get_instance_id)
            # Verify if the list is empty or not
            if instance:
                instance_id_list = [x['INSTANCE_ID'] for x in instance]
                for instance_id in instance_id_list:
                    # verify it the workflow_id, file_set and table_name combination exists in the audit table.
                    # If not  exists then INSERT.
                    exist_flag = "SELECT (COUNT(1)-(SELECT PARALLEL_INSTANCES FROM dbo.META_WORKFLOW WHERE WORKFLOW_ID={WORKFLOW_ID})) AS CNT FROM dbo.TABLE_AUDIT " \
                                 "WHERE WORKFLOW_ID={WORKFLOW_ID} " \
                                 "AND FILE_SET='{FILE_SET}' " \
                                 "AND INSTANCE_ID={INSTANCE_ID} ". \
                        format(WORKFLOW_ID=tab.get("WORKFLOW_ID"),
                               FILE_SET=tab.get("FILE_SET"),
                               INSTANCE_ID=instance_id)
                    # If not  exists then INSERT.
                    audit_exist_flag = self._app.query_with_result(exist_flag)[0].get('CNT')
                    if audit_exist_flag < 0:
                        exist_table = "SELECT DISTINCT TABLE_NAME FROM dbo.TABLE_AUDIT " \
                                      "WHERE WORKFLOW_ID={WORKFLOW_ID} " \
                                      "AND FILE_SET='{FILE_SET}' " \
                                      "AND INSTANCE_ID={INSTANCE_ID} ". \
                            format(WORKFLOW_ID=tab.get("WORKFLOW_ID"),
                                   FILE_SET=tab.get("FILE_SET"),
                                   INSTANCE_ID=instance_id)
                        audit_exist_tablename = self._app.query_with_result(exist_table)
                        table_list = [d['TABLE_NAME'] for d in audit_exist_tablename]
                        if tab.get("TABLE_NAME") not in table_list:
                            # Insert records into TABLE_AUDIT in APP(SQL Server)
                            insert_stmt = "INSERT INTO dbo.TABLE_AUDIT(WORKFLOW_ID, FILE_SET, TABLE_NAME, " \
                                          "TABLE_CONFIG_ID, INSTANCE_ID) " \
                                          "VALUES({WORKFLOW_ID},'{FILE_SET}','{TABLE_NAME}', " \
                                          "{TABLE_CONFIG_ID}, {INSTANCE_ID})". \
                                format(WORKFLOW_ID=tab.get("WORKFLOW_ID"), FILE_SET=tab.get("FILE_SET"),
                                       TABLE_NAME=tab.get("TABLE_NAME"),
                                       TABLE_CONFIG_ID=tab.get("TABLE_CONFIG_ID"),
                                       INSTANCE_ID=instance_id)
                            self._app.execute(insert_stmt)

        # Grant Permission to the User
        select_access = 'GRANT SELECT ON ALL TABLES IN SCHEMA {SCHEMA_NAME} TO read_internalusers;'. \
            format(SCHEMA_NAME=self._rdp_schema)
        self._dw.execute(select_access)
        dml_access = 'GRANT INSERT, UPDATE, DELETE  ON ALL TABLES IN SCHEMA {SCHEMA_NAME} TO write_internalusers;'. \
            format(SCHEMA_NAME=self._rdp_schema)
        self._dw.execute(dml_access)
        self._log.write_log("Create Vertica Tables", 9999999, "Create Vertica Tables", "COMPLETE")


if __name__ == '__main__':
    tran = CreateDWTables('ENGV2HSDBDEV1', 'RDP_WM_NAMRATA_MASTER')
    tran.create_table()