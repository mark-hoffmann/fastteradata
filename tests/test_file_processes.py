import pytest
import fastteradata as ftd

import pandas as pd


export_path = ""
file_name = ""
env_short = ""
usr = "username"
passw = "password"
db = "database1"
table = "table1"
columns = []
nrows = 10
partition_key = ""
current_partition = ""
partition_type = "year"

meta_df = pd.DataFrame.from_dict({"ColumnName":["col1","  col2 "],
            "ColumnLength":[31,20],
            "FormattedColumnType":["CHAR(31)","CHAR(20)"],
            "ColumnType":["CV","CF"]})

valid_final = ".LOGTABLE database1.fexplog; \n\n.LOGON /username, password; \n\n.BEGIN EXPORT; \n\n .EXPORT OUTFILE /data/ \n MODE RECORD FORMAT TEXT;\n\nSELECT TOP 10 CAST(\nCOALESCE(CAST(col1 AS CHAR(31)),'?')||'|'||\nCOALESCE(CAST(col2 AS CHAR(20)),'?')\n AS CHAR(51))\nFROM database1.table1\n;\n.END EXPORT;\n\n .LOGOFF;"
valid_col_list = ["col1","col2"]

def test_generate_sql_main_output_file_simple_col_list():
    final, col_list = ftd.generate_sql_main(export_path, file_name, env_short, usr, passw, db, table, meta_df,
                                            columns=columns, nrows=nrows, partition_key=partition_key,
                                            current_partition=current_partition, partition_type=partition_type)
    assert valid_col_list == col_list

def test_generate_sql_main_output_file_simple_final():
    final, col_list = ftd.generate_sql_main(export_path, file_name, env_short, usr, passw, db, table, meta_df,
                                            columns=columns, nrows=nrows, partition_key=partition_key,
                                            current_partition=current_partition, partition_type=partition_type)
    assert valid_final == final



meta_df_dates = pd.DataFrame.from_dict({"ColumnName":["col1","  col2 "],
            "ColumnLength":[12,8],
            "FormattedColumnType":["DATE FORMAT 'YYYY-MM-DD') AS CHAR(10)","CHAR(8)"],
            "ColumnType":["DA","CF"]})
valid_final_dates = ".LOGTABLE database1.fexplog; \n\n.LOGON /username, password; \n\n.BEGIN EXPORT; \n\n .EXPORT OUTFILE /data/ \n MODE RECORD FORMAT TEXT;\n\nSELECT TOP 10 CAST(\nCOALESCE(CAST(CAST(col1 AS DATE FORMAT 'YYYY-MM-DD') AS CHAR(10)),'?')||'|'||\nCOALESCE(CAST(col2 AS CHAR(8)),'?')\n AS CHAR(19))\nFROM database1.table1\n;\n.END EXPORT;\n\n .LOGOFF;"

def test_generate_sql_main_output_file_date_columns():
    final, col_list = ftd.generate_sql_main(export_path, file_name, env_short, usr, passw, db, table, meta_df_dates,
                                            columns=columns, nrows=nrows, partition_key=partition_key,
                                            current_partition=current_partition, partition_type=partition_type)
    assert valid_final_dates == final


valid_final_nrows = ".LOGTABLE database1.fexplog; \n\n.LOGON /username, password; \n\n.BEGIN EXPORT; \n\n .EXPORT OUTFILE /data/ \n MODE RECORD FORMAT TEXT;\n\nSELECT TOP 200 CAST(\nCOALESCE(CAST(CAST(col1 AS DATE FORMAT 'YYYY-MM-DD') AS CHAR(10)),'?')||'|'||\nCOALESCE(CAST(col2 AS CHAR(8)),'?')\n AS CHAR(19))\nFROM database1.table1\n;\n.END EXPORT;\n\n .LOGOFF;"
def test_generate_sql_main_output_file_nrows():
    final, col_list = ftd.generate_sql_main(export_path, file_name, env_short, usr, passw, db, table, meta_df_dates,
                                            columns=columns, nrows=200, partition_key=partition_key,
                                            current_partition=current_partition, partition_type=partition_type)
    assert valid_final_nrows == final

valid_final_partition_default = ".LOGTABLE database1.fexplog; \n\n.LOGON /username, password; \n\n.BEGIN EXPORT; \n\n .EXPORT OUTFILE /data/ \n MODE RECORD FORMAT TEXT;\n\nSELECT TOP 10 CAST(\nCOALESCE(CAST(CAST(col1 AS DATE FORMAT 'YYYY-MM-DD') AS CHAR(10)),'?')||'|'||\nCOALESCE(CAST(col2 AS CHAR(8)),'?')\n AS CHAR(19))\nFROM database1.table1\nWHERE EXTRACT(YEAR  FROM col1) = 2016; \n.END EXPORT;\n\n .LOGOFF;"
def test_generate_sql_main_output_file_partition_key_default():
    final, col_list = ftd.generate_sql_main(export_path, file_name, env_short, usr, passw, db, table, meta_df_dates,
                                            columns=columns, nrows=nrows, partition_key="col1",
                                            current_partition="2016", )
    assert valid_final_partition_default, final

valid_final_partition_default = ".LOGTABLE database1.fexplog; \n\n.LOGON /username, password; \n\n.BEGIN EXPORT; \n\n .EXPORT OUTFILE /data/ \n MODE RECORD FORMAT TEXT;\n\nSELECT TOP 10 CAST(\nCOALESCE(CAST(CAST(col1 AS DATE FORMAT 'YYYY-MM-DD') AS CHAR(10)),'?')||'|'||\nCOALESCE(CAST(col2 AS CHAR(8)),'?')\n AS CHAR(19))\nFROM database1.table1\nWHERE EXTRACT(YEAR  FROM col1) = 2016; \n.END EXPORT;\n\n .LOGOFF;"
def test_generate_sql_main_output_file_partition_key_default_explicit():
    final, col_list = ftd.generate_sql_main(export_path, file_name, env_short, usr, passw, db, table, meta_df_dates,
                                            columns=columns, nrows=nrows, partition_key="col1",
                                            current_partition="2016", partition_type="year")
    assert valid_final_partition_default == final

valid_final_partition_month = ".LOGTABLE database1.fexplog; \n\n.LOGON /username, password; \n\n.BEGIN EXPORT; \n\n .EXPORT OUTFILE /data/ \n MODE RECORD FORMAT TEXT;\n\nSELECT TOP 10 CAST(\nCOALESCE(CAST(CAST(col1 AS DATE FORMAT 'YYYY-MM-DD') AS CHAR(10)),'?')||'|'||\nCOALESCE(CAST(col2 AS CHAR(8)),'?')\n AS CHAR(19))\nFROM database1.table1\nWHERE EXTRACT(YEAR  FROM col1) = 2016 AND EXTRACT(MONTH FROM col1) = 11; \n.END EXPORT;\n\n .LOGOFF;"
def test_generate_sql_main_output_file_partition_key_month():
    final, col_list = ftd.generate_sql_main(export_path, file_name, env_short, usr, passw, db, table, meta_df_dates,
                                            columns=columns, nrows=nrows, partition_key="col1",
                                            current_partition="2016D11", partition_type="month")
    assert valid_final_partition_month == final


#def test_generate_sql_main_output_file_explicit_columns():
