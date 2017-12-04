import pandas as pd
import numpy as np
import pyodbc as odbc
import teradata

import json
import os

from .io_processors import *
from ..metadata_processors.metadata_processors import *
from ..auth.auth import read_credential_file, load_db_info

auth, auth_dict, env_dict = read_credential_file()

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


def get_unique_partitions(env,db,table,auth_dict=auth_dict,custom_auth=False,connector="teradata",partition_key="", partition_type=""):

    env_n, env_dsn, env_short, usr, passw = load_db_info(env, custom_auth=custom_auth)

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

    conn = connect_teradata(env, connector)
    df = pd.read_sql(sql, conn)
    unique_list = []
    if partition_type == "month":
        df["yearmonth"] = df["years"].map(str) + "D" + df["months"].map(str)
        unique_list = df["yearmonth"].tolist()
    elif partition_type == "year":
        unique_list = df["years"].tolist()
    else:
        raise Exception("Invalid partition_type: must be year or month")

    return(unique_list)




def generate_sql_main(export_path, file_name, env_short, usr, passw, db, table, meta_df, columns=[], nrows=-1,
                      partition_key="", current_partition="", partition_type="", orderby=[], meta_table="", where_clause=""):
    #print("in generage_sql_main")
    #print(partition_key)
    log_table = db
    if len(meta_table) > 0:
        log_table, _ = meta_table.split(".")

    #Step 1
    setup_string = f".LOGTABLE {log_table}.fexplog; \n\n" + f".LOGON {env_short}/{usr}, {passw}; \n\n"
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
            elif meta_df.loc[i,"ColumnType"] != "DA" and int(meta_df.loc[i,"CharType"]) == 0:
                chs = int(meta_df.loc[i,"FormattedColumnType"].split("(")[-1].split(")")[0]) + 3
                tot_chars += chs
            else:
                tot_chars += int(meta_df.loc[i,"ColumnLength"] + 1)
            col_list.append(meta_df.loc[i,"ColumnName"])
        print("COLUMN LIST")
        print(col_list)
        print(select_string)
    else:
        for i in range(0,len(columns)):
            end = False
            if i == len(columns) - 1:
                end = True
            sub_set = meta_df[meta_df["ColumnName"] == columns[i].lower()]
            #print(sub_set)
            select_string += coalesce_statement(columns[i], sub_set["FormattedColumnType"].values[0], end)
            if sub_set["ColumnType"].values[0] == "DA":
                tot_chars += 11
            elif sub_set["ColumnType"].values[0] != "DA" and int(sub_set["CharType"].values[0]) == 0:
                chs = int(sub_set["FormattedColumnType"].values[0].split("(")[-1].split(")")[0]) + 3
                tot_chars += chs
            else:
                tot_chars += int(sub_set["ColumnLength"].values[0] + 1)
            col_list.append(columns[i])

    if partition_key != "" and partition_key not in col_list:
        raise Exception("Partition key must be in column list")

    select_string += f" AS CHAR({tot_chars}))\n"

    from_string = "FROM " + db + "." + table + "\n"

    where_string = ""
    if partition_key != "" and current_partition != "":
        if partition_type == "year":
            where_string = "WHERE EXTRACT(YEAR  FROM " + partition_key + ") = " + str(current_partition)
        elif partition_type == "month":
            where_string = "WHERE EXTRACT(YEAR  FROM " + partition_key + ") = " + str(current_partition.split("D")[0]) + \
            " AND " + "EXTRACT(MONTH FROM " + partition_key + ") = " + str(current_partition.split("D")[1])
        if len(where_clause) > 0:
            where_string += f" AND {where_clause}"
    else:
        if len(where_clause) > 0:
            where_string = f"WHERE {where_clause}"

    orderby_string = ""
    if len(orderby) > 0:
        #If we have order by columns add those to script (useful for horizontal partition pulls)
        orderby_string = " ORDER BY "
        for i, c in enumerate(orderby):
            orderby_string += f"{c} "
            if (i + 1) != len(orderby):
                orderby_string += ", "

    end_string = ";\n.END EXPORT;\n\n .LOGOFF;"

    final = setup_string + export_string + select_string + from_string + where_string + orderby_string + end_string


    return(final, col_list)





def coalesce_statement(var, dtype, end=False):
    end_s = "\n"
    if not end:
        end_s = "||'|'||\n"

    if (dtype != "DATE FORMAT 'YYYY-MM-DD') AS CHAR(10)") and ("DECIMAL" not in dtype):
        coal_s = "COALESCE(CAST(" + var + " AS " + dtype + "),'?')" + end_s
    else:
        coal_s = "COALESCE(CAST(CAST(" + var + " AS " + dtype + "),'?')" + end_s

    return(coal_s)


def parse_sql_single_table(export_path, env, db, table, columns=[], auth_dict=auth_dict,
                           custom_auth=False, nrows=-1,connector="teradata",
                           partition_key="", partition_type="year", execute=True, primary_keys=[], meta_table="", where_clause=""):
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


    env_n, env_dsn, env_short, usr, passw = load_db_info(env, custom_auth=custom_auth)
    #Get metadata
    #print("metadata")
    meta_df = get_table_metadata(env,db,table, columns = columns, auth_dict=auth_dict, custom_auth=custom_auth, connector=connector, partition_key=partition_key, meta_table=meta_table)

    #If we have a partition key, we need to find the unique years for the date key
    #print("unique_partitions")

    #Making changes here to accomodate horizontal scaling. To start off, we will just check that if we need to do horizontal scaling, you will not be able to use a partition Key
    did_partition = False  #Partition flag to pass through for appropriate processing
    MAX_COLS = 90
    tot_columns = len(meta_df["ColumnName"].apply(lambda x: x.lower().strip()).unique())

    unique_partitions = []
    if partition_key != "" and tot_columns <= MAX_COLS:
        print("Getting Unique Partitions")
        unique_partitions = get_unique_partitions(env,db,table,auth_dict=auth_dict,custom_auth=custom_auth,connector=connector,partition_key=partition_key, partition_type=partition_type)
        did_partition = True
    elif partition_key != "" and tot_columns > MAX_COLS:
        print("Cannot create vertical partition because horizontal partitioning is required. Creating horizontal partitions instead.")


    final = ""
    col_list = []
    fast_export_scripts = []
    file_name = table
    if did_partition == False and partition_key == "" and tot_columns <= MAX_COLS:
        _fname = file_name + "_export.txt"
        final, col_list = generate_sql_main(export_path, _fname, env_short, usr, passw, db, table, meta_df, columns=columns, nrows=nrows, meta_table=meta_table, where_clause=where_clause)
        #Save fast export file
        script_path = save_file(export_path, _fname, final)
        fast_export_scripts.append(script_path)
    elif did_partition == True and partition_key != "" and tot_columns <= MAX_COLS:
        #process the normal vertical partitioning
        for part in unique_partitions:
            _fname = file_name + "_" + str(part) + "_export.txt"
            final, col_list = generate_sql_main(export_path, _fname, env_short, usr, passw, db, table, meta_df, columns=columns, nrows=nrows, partition_key=partition_key, current_partition=part, partition_type=partition_type, meta_table=meta_table, where_clause=where_clause)
            #Save fast export file
            script_path = save_file(export_path, _fname, final)
            fast_export_scripts.append(script_path)
    elif tot_columns > MAX_COLS:
        if not isinstance(primary_keys, list):
            raise Exception("'primary_keys' must be a list, even if a single key")
        if len(primary_keys) == 0:
            raise Exception("Horizontal Partitioning is being attempted without a 'primary_keys' argument. Specify the list of primary_keys to execute this pull.")
        print("MAX_COLS exceeded: creating horizontal paritions to accomodate")
        def col_lookup(meta_df, i, tot_columns, MAX_COLS):
            low_index = i*MAX_COLS
            high_index = min((i+1)*MAX_COLS, tot_columns)

            cols = meta_df["ColumnName"].apply(lambda x: x.lower().strip()).unique().tolist()[low_index:high_index]
            #cols = df2.columns.tolist()[low_index:high_index]
            return(cols)

        for i in range(0,(tot_columns // MAX_COLS)+1):
            cols = col_lookup(meta_df, i, tot_columns, MAX_COLS) + primary_keys  #use the columns in our iteration and add on the specified primary keys so that we can recombine later
            cols = list(set(cols)) #reduce the columns to unique if one of our primary keys is repeated
            _fname = file_name + "_" + str(i) + "_export.txt"
            final, this_col_list = generate_sql_main(export_path, _fname, env_short, usr, passw, db, table, meta_df, columns=cols, nrows=nrows, orderby=primary_keys, meta_table=meta_table, where_clause=where_clause)
            col_list.append(this_col_list) #Since this case will have multiple col_lists, we create a list of lists to pass through
            #Save fast export file
            script_path = save_file(export_path, _fname, final)
            fast_export_scripts.append(script_path)
        did_partition = True

    #Check for testing missed columns from metadata
    #Testing purposes, can eventually get rid of
    """
    if len(columns) > 0:
        meta_cols = [x.lower().strip() for x in meta_df["ColumnName"].tolist()]

        cols_not_found = [x.lower() for x in columns if x.lower() not in meta_cols]
        if len(cols_not_found) > 0:
            print("Missing columns needed adding: ")
            print(cols_not_found)
    """
    return col_list, fast_export_scripts, did_partition

def force_string(df, series):
    try:
        df[series] = df[series].map(lambda x: '{:.0f}'.format(x))
    except:
        pass
    return
