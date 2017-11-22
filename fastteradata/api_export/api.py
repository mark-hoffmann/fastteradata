import sys
import time
import pandas as pd
import numpy as np
import os

from joblib import Parallel, delayed

from ..file_processors.file_processors import *
from ..file_processors.io_processors import *
from ..metadata_processors.metadata_processors import *


def extract_table(abs_path, table_name, env, db, nrows=-1, connector = "teradata", columns = [], clean_and_pickle=True, partition_key="", partition_type="year"):
    """
        Summary:
            Extracts table information from Teradata and saves / executes the appropriate files

        Args:
            abs_path (str): Absolute path where you want your scripts to reside and data and pickled subdirectories made
            table_name (str): Teradata table name you wish to query
            env (str): Environment that you want to connect to. (People usually have a testing and production environment)
            db (str): Database name to connect to
            nrows (int): *default = -1* The default of -1 means ALL rows. Otherwise, you can specificy a subset of rows such as 20
            connector (str): *default = 'teradata'* The default uses the teradata python module to connect to the cluster. Valid options include 'teradata' and 'pyodbc'
            columns (list(str)): *default = []* Subset of columns to use, default is all of the columns found in the metadata, however subsets can be selected by passing in ['col1','col2','col3']
            clean_and_pickle (bool): *default = True* Refers to if you want to read the resulting data file into memory to clean and then serialize in your pickled subdirectory
            partition_key (str): *default = ''* There is no partitioning by default. When you define a partition key, it MUST BE A DATE COLUMN AS DEFINED IN TERADATA. This breaks up the exporting
                                    into paritions by the *partition_type* argument. This generates multiple fexp scripts and executes them in parrelel using the available cores. This helps to break
                                    up extremely large datasets or increase speed. When a parition key is defined, after all of the partition files are finished loading from Teradata, the resulting data
                                    is COMBINED into a SINGLE DATA FILE and finishes processing through the following cleaning, data type specification, and serializing.
            partition_type (str): *default = 'year'* Default is to partition the partition_key by distict YEAR. Valid options include "year" or "month"

        Returns:
            Column list recieved from the metadata if clean_and_pickle is set to False, else nothing. Column names are returned in this case so you can save them and use them to read the raw data file
                later with appropriate columns.
    """
    import time
    import os
    try:
        if not os.path.isdir(f"{abs_path}/data"):
            os.makedirs(f"{abs_path}/data")
        if not os.path.isdir(f"{abs_path}/pickled"):
            os.makedirs(f"{abs_path}/pickled")
    except:
        raise Exception("Oops something went wrong when trying to make your storage directories. \
                            Make sub directories in your absolute path folder of 'data' and 'pickled'")
    try:
        t1 = time.time()
        print(f"Starting process for: {db}.{table_name}")
        script_name = table_name
        print("Grabbing meta data and generating fast export file...")
        col_list, fexp_scripts = parse_sql_single_table(abs_path, env,db,table_name, nrows=nrows, connector=connector, columns = columns, partition_key=partition_key, partition_type=partition_type)

        #FOR MULTIPROCESSING WHEN PUT INTO A PACKAGE
        from .multiprocess import call_sub

        print("finished")
        #Can only execute in parrelel from command line in windows, won't execute from jupyter notebooks on a windows machine
        #So we only parrelelize if we see we are on linux
        if os.name == "nt":
            import subprocess
            for f in fexp_scripts:
                print(f"Calling Fast Export on file...  {f}")
                subprocess.call(f"fexp < {f}", shell=True)
        else:
            r = Parallel(n_jobs=-1, verbose=5)(delayed(call_sub)(f) for f in fexp_scripts)

        data_file = ""
        if partition_key != "":
            data_file, concat_str, data_files, remove_cmd = combine_partitioned_file(fexp_scripts)
            #Concat and delete partition files
            concat_files(concat_str)
            for f in data_files:
                remove_file(remove_cmd, f)
        else:
            data_file = fexp_scripts[0]

        #raw_tbl_name = data_file.split("/")[-1].split(".")[0]

        if clean_and_pickle:
            print("Reading Table into memory...")
            #Need low_memory flag or else with large datasets we will end up with mixed datatypes
            _df = pd.read_csv(f"{abs_path}/data/{table_name}_export.txt", names=col_list, sep="|", low_memory=False)
            print("Cleaning data...")
            for col in _df.columns.tolist():
                if _df[col].dtype == "object":
                    _df[col] = _df[col].str.strip()
                    _df[col] = _df[col].apply(lambda x: np.nan if pd.isnull(x) else np.nan if ('missing' in x.lower()) else x)
            _df.replace("~",np.nan,inplace=True)
            _df.replace("!",np.nan,inplace=True)
            _df.replace("?",np.nan,inplace=True)
            _df.replace("",np.nan,inplace=True)

            #Try to find date looking columns and cast them appropriately (We know the format of the date because we are explicit about it in the fastexport script)
            #Try to find id columns and convert to strings proactively
            for col in _df.columns.tolist():
                if "_dt" in col:
                    try:
                        _df[col] =  pd.to_datetime(_df[col], format='%Y-%m-%d')
                    except:
                        pass
                if (("_id" in col) or ("_key" in col) or ("_cd" in col)):
                    try:
                        print("before force string")
                        check_nulls(_df["EPI_ID"])
                        force_string(_df,col)
                        print("after force string")
                        check_nulls(_df["EPI_ID"])
                    except:
                        pass
            print("Pickling data....")

            _df.to_pickle(f"{abs_path}/pickled/{table_name}.pkl")
            print("Finished: Your pickle file is located at:")
            print(f"{abs_path}/pickled/{table_name}.pkl")
            t2 = time.time()
            m, s = divmod(int(t2-t1), 60)
            h, m = divmod(m, 60)
            print(f"Process took: {h} hours {m} minutes {s} seconds")
            #If you pickle the data, then you will have the column metadata already
            return
        else:
            print("Finished: Your end data file is located at:")
            print(f"{abs_path}/data/{table_name}_export.txt")
            print("You have chosen to not clean or pickle your data and fast export does not support export column names. \
                    Be sure to gather and keep in order these column names.")


        t2 = time.time()
        m, s = divmod(int(t2-t1), 60)
        h, m = divmod(m, 60)
        print(f"Process took: {h} hours {m} minutes {s} seconds")
    except Exception as e:
        print(f"Error: {e}")
        return
    return(col_list)
