import sys
import time
import pandas as pd
import numpy as np
import os
import pyodbc as odbc
import teradata
from joblib import Parallel, delayed

from ..auth.auth import read_credential_file, load_db_info

def connect_teradata(env, connector):

    env_n, env_dsn, env_short, usr, passw = load_db_info(env)

    if connector == "pyodbc":
        conn = odbc.connect('DRIVER={Teradata};VERSION=14.10;'+f"DBCNAME={env_n};DSN={env_dsn};UID={usr};PWD={passw};QUIETMODE=YES",autocommit=True)
        return(conn)

    elif connector == "teradata":
        udaExec = teradata.UdaExec(appName="Anomaly Detection", version='1.0', odbcLibPath="/opt/teradata/client/15.10/odbc_64/lib/libodbc.so", logConsole=False)
        session = udaExec.connect(method='odbc', system=env_n, username=usr, password=passw)
        return(session)
    else:
        raise ValueError("Wrong value error: Need to specify connector as either teradata or pyodbc")



def prep_load_table(df, table_name, env, db, connector, clear_table):


    conn = connect_teradata(env, connector)

    col_type_zip = zip(df.columns.tolist(),df.dtypes.tolist())
    cols = ""
    for col, col_type in col_type_zip:
        t = ""

        if col_type.str == "|O":
            t = "varchar(80) "
        elif (("<M8" in col_type.str) or ("datetime" in col_type.str)):
            t = "date format 'YYYY-MM-DD' "
        else:
            "numeric(12,2) "

        cols += f"{col} {t} "
        if col != df.columns.tolist()[-1]:
            cols += ","
    #print(cols)
    if clear_table:
        try:
            conn.execute(f"drop table {db}.{table_name};")
            conn.execute(f"drop table {db}.err1;")
            conn.execute(f"drop table {db}.err2;")
        except:
            pass
        conn.execute(f"create table {db}.{table_name} ({cols}) primary index({df.columns.tolist()[0]});")
    else:
        print("Attempting to load table without clearing. If an error occurs it's most likely due to a column mismatch.")
        print("It is recomended to fast load whole tables")

    return
