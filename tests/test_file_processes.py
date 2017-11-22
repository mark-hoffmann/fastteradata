import pytest
import fastteradata as ftd

import pandas as pd

# TESTING OF THE generate_sql_main METHOD

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
            "ColumnType":["CV","CF"],
            "CharType":[1,1]})

valid_final = ".LOGTABLE database1.fexplog; \n\n.LOGON /username, password; \n\n.BEGIN EXPORT; \n\n .EXPORT OUTFILE /data/ \n MODE RECORD FORMAT TEXT;\n\nSELECT TOP 10 CAST(\nCOALESCE(CAST(col1 AS CHAR(31)),'?')||'|'||\nCOALESCE(CAST(col2 AS CHAR(20)),'?')\n AS CHAR(53))\nFROM database1.table1\n;\n.END EXPORT;\n\n .LOGOFF;"
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
            "ColumnType":["DA","CF"],
            "CharType":[1,1]})
valid_final_dates = ".LOGTABLE database1.fexplog; \n\n.LOGON /username, password; \n\n.BEGIN EXPORT; \n\n .EXPORT OUTFILE /data/ \n MODE RECORD FORMAT TEXT;\n\nSELECT TOP 10 CAST(\nCOALESCE(CAST(CAST(col1 AS DATE FORMAT 'YYYY-MM-DD') AS CHAR(10)),'?')||'|'||\nCOALESCE(CAST(col2 AS CHAR(8)),'?')\n AS CHAR(20))\nFROM database1.table1\n;\n.END EXPORT;\n\n .LOGOFF;"

def test_generate_sql_main_output_file_date_columns():
    final, col_list = ftd.generate_sql_main(export_path, file_name, env_short, usr, passw, db, table, meta_df_dates,
                                            columns=columns, nrows=nrows, partition_key=partition_key,
                                            current_partition=current_partition, partition_type=partition_type)
    assert valid_final_dates == final


valid_final_nrows = ".LOGTABLE database1.fexplog; \n\n.LOGON /username, password; \n\n.BEGIN EXPORT; \n\n .EXPORT OUTFILE /data/ \n MODE RECORD FORMAT TEXT;\n\nSELECT TOP 200 CAST(\nCOALESCE(CAST(CAST(col1 AS DATE FORMAT 'YYYY-MM-DD') AS CHAR(10)),'?')||'|'||\nCOALESCE(CAST(col2 AS CHAR(8)),'?')\n AS CHAR(20))\nFROM database1.table1\n;\n.END EXPORT;\n\n .LOGOFF;"
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

valid_final_partition_default = ".LOGTABLE database1.fexplog; \n\n.LOGON /username, password; \n\n.BEGIN EXPORT; \n\n .EXPORT OUTFILE /data/ \n MODE RECORD FORMAT TEXT;\n\nSELECT TOP 10 CAST(\nCOALESCE(CAST(CAST(col1 AS DATE FORMAT 'YYYY-MM-DD') AS CHAR(10)),'?')||'|'||\nCOALESCE(CAST(col2 AS CHAR(8)),'?')\n AS CHAR(20))\nFROM database1.table1\nWHERE EXTRACT(YEAR  FROM col1) = 2016; \n.END EXPORT;\n\n .LOGOFF;"
def test_generate_sql_main_output_file_partition_key_default_explicit():
    final, col_list = ftd.generate_sql_main(export_path, file_name, env_short, usr, passw, db, table, meta_df_dates,
                                            columns=columns, nrows=nrows, partition_key="col1",
                                            current_partition="2016", partition_type="year")
    assert valid_final_partition_default == final

valid_final_partition_month = ".LOGTABLE database1.fexplog; \n\n.LOGON /username, password; \n\n.BEGIN EXPORT; \n\n .EXPORT OUTFILE /data/ \n MODE RECORD FORMAT TEXT;\n\nSELECT TOP 10 CAST(\nCOALESCE(CAST(CAST(col1 AS DATE FORMAT 'YYYY-MM-DD') AS CHAR(10)),'?')||'|'||\nCOALESCE(CAST(col2 AS CHAR(8)),'?')\n AS CHAR(20))\nFROM database1.table1\nWHERE EXTRACT(YEAR  FROM col1) = 2016 AND EXTRACT(MONTH FROM col1) = 11; \n.END EXPORT;\n\n .LOGOFF;"
def test_generate_sql_main_output_file_partition_key_month():
    final, col_list = ftd.generate_sql_main(export_path, file_name, env_short, usr, passw, db, table, meta_df_dates,
                                            columns=columns, nrows=nrows, partition_key="col1",
                                            current_partition="2016D11", partition_type="month")
    assert valid_final_partition_month == final

valid_final_explicit_columns = ".LOGTABLE database1.fexplog; \n\n.LOGON /username, password; \n\n.BEGIN EXPORT; \n\n .EXPORT OUTFILE /data/ \n MODE RECORD FORMAT TEXT;\n\nSELECT CAST(\nCOALESCE(CAST(CAST(col1 AS DATE FORMAT 'YYYY-MM-DD') AS CHAR(10)),'?')\n AS CHAR(11))\nFROM database1.table1\n;\n.END EXPORT;\n\n .LOGOFF;"
def test_generate_sql_main_output_file_explicit_columns():
    final, col_list = ftd.generate_sql_main(export_path, file_name, env_short, usr, passw, db, table, meta_df_dates, columns=["col1"])

    assert valid_final_explicit_columns == final

def test_generate_sql_main_output_partition_key_mismatch_exception():
    with pytest.raises(Exception):
        ftd.generate_sql_main(export_path, file_name, env_short, usr, passw, db, table, meta_df_dates, columns=["col1"], partition_key="invalid")


# TESTING OF THE coalesce_statement METHOD

valid_coalesce1 = "COALESCE(CAST(CAST(col1 AS DATE FORMAT 'YYYY-MM-DD') AS CHAR(10)),'?')\n"
def test_coalesce_statement_date_end():
    stm = ftd.coalesce_statement("col1","DATE FORMAT 'YYYY-MM-DD') AS CHAR(10)",end=True)
    assert valid_coalesce1 == stm

valid_coalesce2 = "COALESCE(CAST(CAST(col1 AS DATE FORMAT 'YYYY-MM-DD') AS CHAR(10)),'?')||'|'||\n"
def test_coalesce_statement_date_noend():
    stm = ftd.coalesce_statement("col1","DATE FORMAT 'YYYY-MM-DD') AS CHAR(10)",end=False)
    assert valid_coalesce2 == stm

valid_coalesce3 = "COALESCE(CAST(col1 AS CHAR(50)),'?')\n"
def test_coalesce_statement_nodate_end():
    stm = ftd.coalesce_statement("col1","CHAR(50)",end=True)
    assert valid_coalesce3 == stm

valid_coalesce4 = "COALESCE(CAST(col1 AS CHAR(50)),'?')||'|'||\n"
def test_coalesce_statement_nodate_noend():
    stm = ftd.coalesce_statement("col1","CHAR(50)",end=False)
    assert valid_coalesce4 == stm

valid_coalesce5 = "COALESCE(CAST(col1 AS DECIMAL(12,2) FORMAT 'Z99999999999.99') AS CHAR(15)),'?')\n"
def test_coalesce_statement_number_end():
    stm = ftd.coalesce_statement("col1","DECIMAL(12,2) FORMAT 'Z99999999999.99') AS CHAR(15)",end=True)
    assert valid_coalesce5 == stm

valid_coalesce6 = "COALESCE(CAST(col1 AS DECIMAL(12,2) FORMAT 'Z99999999999.99') AS CHAR(15)),'?')||'|'||\n"
def test_coalesce_statement_number_noend():
    stm = ftd.coalesce_statement("col1","DECIMAL(12,2) FORMAT 'Z99999999999.99') AS CHAR(15)",end=False)
    assert valid_coalesce6 == stm
