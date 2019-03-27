from EMD.DBOperations import AppAccessLayer
from EMD.DBOperations import DWAccessLayer
from EMD.Config import Config
from EMD.common import Log
from EMD.CreatedwTables import CreateDWTables



class AlterRawTabColType:
    def __init__(self, rdp_server, app_id, tables_list=()):
        self._app = AppAccessLayer(db_name=app_id, rdp_server=rdp_server)
        self._dw = DWAccessLayer(silo_id=app_id, app_connection=self._app)
        cfg = Config(app_connection=self._app, silo_id=app_id)
        self._rdp_schema = cfg.get_config(["dw.schema.name"])["dw.schema.name"]
        self._log = Log(self._app)
        self.raw_tables = tables_list
        self.rdp_server = rdp_server
        self.app_id = app_id

    def rename_tables(self):
        self._log.write_log("Alter Vertica Raw Tables", 9999999, "Rename tables to bak", "STARTED")
        for table in self.raw_tables:
            dw_rename_table = "alter table {SCHEMA_NAME}.{TABLE_NAME} rename to {TABLE_NAME_BAK}". \
                format(SCHEMA_NAME=self._rdp_schema, TABLE_NAME=table, TABLE_NAME_BAK=table + '_BAK')
            print(dw_rename_table)

            self._dw.execute(dw_rename_table)
            self._log.write_log("Alter Vertica Raw Tables", 9999999,
                                "Rename Table Statement: " + dw_rename_table, "COMPLETE")
        self._log.write_log("Alter Vertica Raw Tables", 9999999, "Rename tables to bak", "COMPLETE")

    def create_new_raw_table(self):
        trf = CreateDWTables(self.rdp_server, self.app_id)
        trf.create_table()

    def get_tables(self):
        mssql_query = "select cof.TABLE_NAME_FORMAT, col.COLUMN_NAME, col.COLUMN_DATA_TYPE " \
                      "from META_TABLE_COLUMNS col, META_TABLE_CONFIG cof " \
                      "where col.TABLE_CONFIG_ID = cof.TABLE_CONFIG_ID " \
                      "and cof.TABLE_TYPE = 'RAW' " \
                      "and col.COLUMN_DATA_TYPE in ('int', 'numeric') " \
                      "order by 1,2,3 "
        dw_query = "select c.table_name, c.column_name, c.data_type " \
                   "from columns c " \
                   "where c.table_schema = '{SCHEMA_NAME}' and c.table_name like 'RAW%' " \
                   "order by 1,2 ".format(SCHEMA_NAME=self._rdp_schema)

        data_config = self._app.query_with_result(mssql_query)
        data_meta = self._dw.query_with_result(dw_query)

        tmp_table_list = []
        for row_conf in data_config:
            name_tab = row_conf.get("TABLE_NAME_FORMAT")
            name_col = row_conf.get("COLUMN_NAME")
            for row_meta in data_meta:
                if name_tab == row_meta.get("table_name") and name_col == row_meta.get("column_name"):
                    tmp_type = row_meta.get("data_type")
                    if tmp_type[0:7].upper() == 'VARCHAR':
                        tmp_table_list.append(name_tab)
        results_tab = tuple(set(tmp_table_list))
        return results_tab

    def get_columns(self, table_name):
        dw_query = " select c.column_name, c.data_type as new_type, d.data_type  as old_type " \
                   " from columns c, columns d "\
                   " where c.column_name = d.column_name "\
                   " and  d.table_schema = '{SCHEMA_NAME}' and d.table_name = '{TABLE_NAME_BAK}' "\
                   " and c.table_schema = '{SCHEMA_NAME}' and c.table_name = '{TABLE_NAME}' ".\
            format(SCHEMA_NAME=self._rdp_schema, TABLE_NAME=table_name, TABLE_NAME_BAK=table_name + '_BAK')
        result = self._dw.query_with_result(dw_query)
        return result

    def data_convert(self):
        self._log.write_log("Data Conversion", 9999999, "Load data from bak to new table", "STARTED")
        for table in self.raw_tables:
            column_string = ''
            select_column_string = ''
            columns_dataset = self.get_columns(table)
            for row_data in columns_dataset:
                str_column = "\"" + row_data.get("column_name") + "\""
                column_string += ", " + str_column
                new_type = row_data.get("new_type")
                old_type = row_data.get("old_type")
                if new_type == old_type:
                    select_column_string += ", " + str_column
                else:
                    if new_type.upper() == 'INT' or new_type[0:7].upper() == 'NUMERIC':
                        expression_str = "CAST(CASE WHEN LENGTH(REPLACE(TRIM(" + str_column + "), ',', '')) = 0 "\
                                         "THEN NULL ELSE TO_NUMBER(REPLACE(TRIM(" + str_column + "), ',', '')) END"\
                                         " AS " + new_type + ")"
                        select_column_string += ", " + expression_str
                    else:
                        print('new_type:', new_type, 'old_type:', old_type, '\n')
                        raise "Got invalid types"

            dw_insert = "Insert into {SCHEMA_NAME}.{TABLE_NAME}({COLUMN_STRING}) "\
                        "SELECT {SELECT_COLUMN_STRING} "\
                        "FROM {SCHEMA_NAME}.{TABLE_NAME_BAK}"\
                        .format(SCHEMA_NAME=self._rdp_schema, TABLE_NAME=table,
                                COLUMN_STRING=column_string[2:], SELECT_COLUMN_STRING=select_column_string[2:],
                                TABLE_NAME_BAK=table + '_BAK')
            print(dw_insert)
            self._log.write_log("Data Conversion", 9999999,
                                "Data insert Statement: " + dw_insert, "Started")
            self._dw.execute(dw_insert)
            self._log.write_log("Data Conversion", 9999999,
                                "Data insert Statement: ", "COMPLETE")

            dw_drop_bak = "Drop table {SCHEMA_NAME}.{TABLE_NAME_BAK} "\
                          .format(SCHEMA_NAME=self._rdp_schema,TABLE_NAME_BAK=table + '_BAK')
            print(dw_drop_bak)
            self._dw.execute(dw_drop_bak)
            self._log.write_log("Data Conversion", 9999999,
                                "Drop backup table: " + dw_drop_bak, "COMPLETE")
        self._log.write_log("Data Conversion", 9999999, "Load data from bak to new table", "COMPLETE")

    def alter_columns(self):
        # Need to execute import config and createDWTable first
        if len(self.raw_tables) == 0:
            self.raw_tables = self.get_tables()
        #print(self.raw_tables)
        self.rename_tables()
        self.create_new_raw_table()
        self.data_convert()

if __name__ == '__main__':
    #tran = AlterRawTabColType('engv2hsdbdev1', 'RDP_9100_JASONL', ('RAW_WM_WMSSC_WEEKLY_FORECAST',))
    # tran.rename_tables()
    # tran.create_new_raw_table()
    # tran.data_convert()
    #tables = tran.get_tables()
    #print(tables)
    tran = AlterRawTabColType('PREZ2CDB2\DB1', 'PREP_WM_RDP')
    tran.alter_columns()


