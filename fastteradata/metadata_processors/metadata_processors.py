import pandas as pd
import numpy as np
import pyodbc as odbc
import teradata
import json
import os

auth = json.load(open(os.path.expanduser('~/.fastteradata')))
auth_dict = auth["auth_dict"]
env_dict = auth["env_dict"]

def _process_metadata_fexp(df,partition_key=""):
    data_types = [] #let's calculate then put types in a list to easily add on to the df
    #print(df.columns.tolist())
    unique_cols = set()
    #print("before dropping length: " + str(len(df)))
    for i in range(0,len(df)):
        if df.loc[i,"ColumnName"] not in unique_cols:
            unique_cols.add(df.loc[i,"ColumnName"])
            length = df.loc[i,"ColumnLength"] + 1
            col_type = df.loc[i,"ColumnType"]
            char_type = df.loc[i,"CharType"]
            if col_type != "DA" and char_type == 1:
                length = int(length)
                data_types.append(f"CHAR({length})")
            elif col_type != "DA" and char_type == 0:
                data_types.append("FORMAT 'Z(20)Z.ZZ') AS CHAR(15)")
            else:
                #nums = df.loc[i,"ColumnFormat"].replace("-","").replace("(","").strip().split(")")
                #MAKE SURE TO HANDLE DATE CASE WHEN APPENDING STRINGS for parentheses
                data_types.append("DATE FORMAT 'YYYY-MM-DD') AS CHAR(10)")

            #Check for correct data type of parition key of Date
            if df.loc[i,"ColumnName"] == partition_key:
                if col_type == "DA":
                    pass
                else:
                    raise Exception("Partition Key specified is not label as a date in specified database. Parition must be a date.")
        else:
            df.drop(i, inplace=True)

    #print("after dropping length: " + str(len(df)))
    df.reset_index(inplace=True)
    df["FormattedColumnType"] = data_types
    #df.drop(["ColumnFormat","ColumnLength","CharType"], axis=1, inplace=True)

    return(df)

def get_table_metadata(env, db_name, tbl_name,columns = [], auth_dict=auth_dict, custom_auth=False, connector="teradata",partition_key=""):
    """
        Summary:
            Get's the metadata about a specific table for creation of the fast scripts.

        Args:
            env (str): Environment for connecting to. Needs to be a valid value either "ACT" or "PROD"
            db_name (str): Database name to connect to
            tbl_name (str): Table name to connect to
            auth_dict (dict): either use default for using a panda admin account, or else if you want to do custom auth,
                                you must flag custom_auth = True and pass in ("usrname","passw") as such like a tuple into auth_dict
            custom_auth (bool): default False, if you want to pass your own creds in, you must flag this as True and pass in a tuple into the auth dict

        Returns:
            Pandas DataFrame containing columns DatabaseName, TableName, ColumnName, ColumnFormat, ColumnLength, CharType
    """


    env_n = env_dict[env][0]
    env_dsn = env_dict[env][1]

    if not custom_auth:
        usr = auth_dict[env][0]
        passw = auth_dict[env][1]
    else:
        usr = auth_dict[0]
        passw = auth_dict[1]


    if len(columns) == 0:

        sql = f"SELECT DISTINCT T.Tablename, columnname, columnformat, columntype, C.columnlength, decimaltotaldigits, decimalfractionaldigits, chartype \
        FROM dbc.tablesv T, dbc.COLUMNSv C \
        WHERE T.Databasename=C.Databasename \
        AND T.Tablename=C.Tablename \
        AND T.Databasename = '{db_name}' \
        AND T.Tablename = '{tbl_name}'"
    else:
        #sql_list = tuple(columns)
        #print(sql_list)
        sql = f"SELECT DISTINCT T.Tablename, columnname, columnformat, columntype, C.columnlength, decimaltotaldigits, decimalfractionaldigits, chartype \
        FROM dbc.tablesv T, dbc.COLUMNSv C \
        WHERE T.Databasename=C.Databasename \
        AND T.Tablename=C.Tablename \
        AND T.Databasename = '{db_name}' \
        AND T.Tablename = '{tbl_name}' \
        AND columnname "
        if len(columns) == 1:
            sql += f" = '{columns[0]}'"
        else:
            sql += f" in {tuple(columns)}"

    if connector == "pyodbc":
        conn = odbc.connect('DRIVER={Teradata};VERSION=14.10;'+f"DBCNAME={env_n};DSN={env_dsn};UID={usr};PWD={passw};QUIETMODE=YES",autocommit=True)

        df = pd.read_sql(sql,conn)
        df = _process_metadata_fexp(df,partition_key=partition_key)
        return(df)

    elif connector == "teradata":
        udaExec = teradata.UdaExec(appName="Anomaly Detection", version='1.0', odbcLibPath="/opt/teradata/client/15.10/odbc_64/lib/libodbc.so", logConsole=False)
        print("Connecting to ...")
        session = udaExec.connect(method='odbc', system=env_n, username=usr, password=passw)
        print("Connected!")

        df = pd.read_sql(sql, session)
        df = _process_metadata_fexp(df,partition_key=partition_key)
        return(df)
    else:
        raise Exception("Wrong value error: Need to specify connector as either teradata or pyodbc")

    return(df)
