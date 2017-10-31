import pandas as pd
import numpy as np
import pyodbc as odbc
import teradata

import json
import os

from .io_processors import *
from ..metadata_processors.metadata_processors import *

auth = json.load(open(os.path.expanduser('~/.fastteradata')))
auth_dict = auth["auth_dict"]
env_dict = auth["env_dict"]

def get_unique_partitions(env,db,table,auth_dict=auth_dict,custom_auth=False,connector="teradata",partition_key="", partition_type=""):

    env_n = env_dict[env][0]
    env_dsn = env_dict[env][1]

    if not custom_auth:
        usr = auth_dict[env][0]
        passw = auth_dict[env][1]
    else:
        usr = auth_dict[0]
        passw = auth_dict[1]

    sql = ""
    if partition_type == "year":
        sql = f"SELECT DISTINCT EXTRACT(YEAR FROM {partition_key}) as years \
                                from {db}.{table} order by years;"
    elif partition_type == "month":
        sql = f"SELECT DISTINCT EXTRACT(YEAR FROM {partition_key}) as years, \
                                EXTRACT(MONTH FROM {partition_key}) as months \
                                from {db}.{table} order by years, months;"
    else:
        raise Exception("Invalid partition_type: Must either be year or month")


    if connector == "pyodbc":
        conn = odbc.connect('DRIVER={Teradata};VERSION=14.10;'+f"DBCNAME={env_n};DSN={env_dsn};UID={usr};PWD={passw};QUIETMODE=YES",autocommit=True)
        df = pd.read_sql(sql, conn)
        #print(df.head())

        unique_list = []
        if partition_type == "month":
            df["yearmonth"] = df["years"].map(str) + "D" + df["months"].map(str)
            unique_list = df["yearmonth"].tolist()
        elif partition_type == "year":
            unique_list = df["years"].tolist()
        else:
            raise Exception("Invalid partition_type: must be year or month")

        return(unique_list)

    elif connector == "teradata":
        udaExec = teradata.UdaExec(appName="Anomaly Detection", version='1.0', odbcLibPath="/opt/teradata/client/15.10/odbc_64/lib/libodbc.so", logConsole=False)
        #print("Connecting to ...")
        session = udaExec.connect(method='odbc', system=env_n, username=usr, password=passw)
        #print("Connected!")
        df = pd.read_sql(sql, session)

        unique_list = []
        if partition_type == "month":
            df["yearmonth"] = df["years"].map(str) + "D" + df["months"].map(str)
            unique_list = df["yearmonth"].tolist()
        elif partition_type == "year":
            unique_list = df["years"].tolist()
        else:
            raise Exception("Invalid parrition_type: must be year or month")

        return(unique_list)
    else:
        raise Exception("Wrong value error: Need to specify connector as either teradata or pyodbc")


    return



def generate_sql_main(export_path, file_name, env_short, usr, passw, db, table, meta_df, columns=[], nrows=-1,
                      partition_key="", current_partition="", partition_type=""):
    #print("in generage_sql_main")
    #print(partition_key)

    #Step 1
    setup_string = f".LOGTABLE {db}.fexplog; \n\n" + f".LOGON {env_short}/{usr}, {passw}; \n\n"
    #Step 2
    export_string = ".BEGIN EXPORT; \n\n .EXPORT OUTFILE " + export_path + "/data/" + file_name + " \n MODE RECORD FORMAT TEXT;\n\n"

    #Step 3
    select_string = "SELECT CAST(\n"
    if nrows > 0:
        select_string = f"SELECT TOP {nrows} CAST(\n"
    #Loop through and fill out the coalesce statements
    tot_chars = 0

    meta_df["ColumnName"] = meta_df["ColumnName"].apply(lambda x: x.lower().strip())
    col_list = []
    if len(columns) == 0:
        for i in range(0,len(meta_df)):
            end = False
            if i == len(meta_df) -1:
                end = True
            select_string += coalesce_statement(meta_df.loc[i,"ColumnName"], meta_df.loc[i,"FormattedColumnType"], end)
            if meta_df.loc[i,"ColumnType"] == "DA":
                tot_chars += 11
            else:
                tot_chars += int(meta_df.loc[i,"ColumnLength"])
            col_list.append(meta_df.loc[i,"ColumnName"])
    else:
        for i in range(0,len(columns)):
            end = False
            if i == len(columns) - 1:
                end = True
            sub_set = meta_df[meta_df["ColumnName"] == columns[i]]
            #print(sub_set)
            select_string += coalesce_statement(columns[i], sub_set["FormattedColumnType"].values[0], end)
            if sub_set["ColumnType"].values[0] == "DA":
                tot_chars += 11
            else:
                tot_chars += int(sub_set["ColumnLength"].values[0])
            col_list.append(columns[i])

    if partition_key != "" and partition_key not in col_list:
        raise Exception("Partition key must be in column list")

    select_string += f" AS CHAR({tot_chars}))\n"

    from_string = "FROM " + db + "." + table + "\n"

    where_string = ";\n"
    if partition_key != "" and current_partition != "":
        if partition_type == "year":
            where_string = "WHERE EXTRACT(YEAR  FROM " + partition_key + ") = " + str(current_partition) + "; \n"
        elif partition_type == "month":
            where_string = "WHERE EXTRACT(YEAR  FROM " + partition_key + ") = " + str(current_partition.split("D")[0]) + \
            " AND " + "EXTRACT(MONTH FROM " + partition_key + ") = " + str(current_partition.split("D")[1]) + "; \n"

    end_string = ".END EXPORT;\n\n .LOGOFF;"

    final = setup_string + export_string + select_string + from_string + where_string + end_string


    return(final, col_list)





def coalesce_statement(var, dtype, end=False):
    end_s = "\n"
    if not end:
        end_s = "||'|'||\n"

    if dtype not in ["DATE FORMAT 'YYYY-MM-DD') AS CHAR(10)","FORMAT 'Z(20)Z.ZZ') AS CHAR(15)"]:
        coal_s = "COALESCE(CAST(" + var + " AS " + dtype + "),'?')" + end_s
    else:
        coal_s = "COALESCE(CAST(CAST(" + var + " AS " + dtype + "),'?')" + end_s

    return(coal_s)


def parse_sql_single_table(export_path, env, db, table, columns=[], auth_dict=auth_dict,
                           custom_auth=False, nrows=-1,connector="teradata",
                           partition_key="", partition_type="year", execute=True):
    """
        Summary:
            Parses the information for a valid database table and writes script to output file

        Args:
            export_path (str): Path where you want your script to end up
            file_name (str): Name of the output data file you want to create in a sub /data directory.
                                The scripts will be generated with the word "script" appended in front of the name.
            env (str): Environment for connecting to. Needs to be a valid value either "ACT" or "PROD"
            db (str): Database name to connect to
            tbl (str): Table name to connect to
            columns (list(str)): Subset of columns to use, default is all of the columns found in the metadata
            auth_dict (dict): either use default for using a panda admin account, or else if you want to do custom auth,
                                you must flag custom_auth = True and pass in ("usrname","passw") as such like a tuple into auth_dict
            custom_auth (bool): default False, if you want to pass your own creds in, you must flag this as True and pass in a tuple into the auth dict
            nrows (int): Number of rows you want your script to generate, default is all (*)

        Returns:
            Pandas DataFrame containing columns DatabaseName, TableName, ColumnName, ColumnFormat, ColumnLength, CharType
    """


    env_short = env_dict[env][2]

    if not custom_auth:
        usr = auth_dict[env][0]
        passw = auth_dict[env][1]
    else:
        usr = auth_dict[0]
        passw = auth_dict[1]

    #Get metadata
    #print("metadata")
    meta_df = get_table_metadata(env,db,table, columns = columns, auth_dict=auth_dict, custom_auth=custom_auth, connector=connector, partition_key=partition_key)

    #If we have a partition key, we need to find the unique years for the date key
    #print("unique_partitions")
    unique_partitions = []
    if partition_key != "":
        print("Getting Unique Partitions")
        unique_partitions = get_unique_partitions(env,db,table,auth_dict=auth_dict,custom_auth=custom_auth,connector=connector,partition_key=partition_key, partition_type=partition_type)

    #print("after")
    final = ""
    col_list = []
    fast_export_scripts = []
    file_name = table
    if partition_key == "":
        _fname = file_name + "_export.txt"
        final, col_list = generate_sql_main(export_path, _fname, env_short, usr, passw, db, table, meta_df, columns=columns, nrows=nrows)
        #Save fast export file
        script_path = save_file(export_path, _fname, final)
        fast_export_scripts.append(script_path)
    else:
        for part in unique_partitions:
            _fname = file_name + "_" + str(part) + "_export.txt"
            final, col_list = generate_sql_main(export_path, _fname, env_short, usr, passw, db, table, meta_df, columns=columns, nrows=nrows, partition_key=partition_key, current_partition=part, partition_type=partition_type)
            #Save fast export file
            script_path = save_file(export_path, _fname, final)
            fast_export_scripts.append(script_path)


    #Check for testing missed columns from metadata
    #Testing purposes, can eventually get rid of
    if len(columns) > 0:
        meta_cols = [x.lower().strip() for x in meta_df["ColumnName"].tolist()]

        cols_not_found = [x for x in columns if x not in meta_cols]
        if len(cols_not_found) > 0:
            print("Missing columns needed adding: ")
            print(cols_not_found)


    return col_list, fast_export_scripts
