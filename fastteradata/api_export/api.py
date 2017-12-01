import sys
import time
import pandas as pd
import numpy as np
import os

from joblib import Parallel, delayed

from ..file_processors.file_processors import *
from ..file_processors.io_processors import *
from ..metadata_processors.metadata_processors import *


def extract_table(abs_path, table_name, env, db, nrows=-1, connector = "teradata", columns = [], clean_and_serialize="feather", partition_key="", partition_type="year", primary_keys=[]):
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
        if not os.path.isdir(f"{abs_path}/serialized"):
            os.makedirs(f"{abs_path}/serialized")
    except:
        raise Exception("Oops something went wrong when trying to make your storage directories. \
                            Make sub directories in your absolute path folder of 'data' and 'pickled'")
    data_file, concat_str, data_files, remove_cmd, _df, combine_type = "","","","","", ""
    try:
        t1 = time.time()
        print(f"Starting process for: {db}.{table_name}")
        script_name = table_name
        print("Grabbing meta data and generating fast export file...")
        col_list, fexp_scripts, did_partition = parse_sql_single_table(abs_path, env,db,table_name, nrows=nrows, connector=connector, columns = columns, partition_key=partition_key, partition_type=partition_type, primary_keys=primary_keys)

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


        data_file = f"{abs_path}/data/{table_name}_export.txt"
        #print("before did partition check")
        #print(did_partition)
        #print(fexp_scripts)
        #print(col_list)
        if did_partition:
            #First checking the case of vertical parrelelization

            if not isinstance(col_list[0], list):
                combine_type = "vertical"
            else:
                combine_type = "horizontal"
            #print(combine_type)
            concat_str, data_files, remove_cmd = combine_partitioned_file(fexp_scripts,combine_type=combine_type)


            #Concat and delete partition files
            #If we are doing a vertical concat, we use this
            #print("Concat str: " + str(concat_str))
            if concat_str:
                #print("In here")
                concat_files(concat_str)
            else:
                #If we are doing a horizontal concat we will read into memory and combine
                #This is because windows does not have a cood command in command prompt to do this operation as opposed to linux paste command
                _df = concat_files_horizontal(data_file, data_files, col_list, primary_keys)
                col_list = _df.columns.tolist()



            for f in data_files:
                remove_file(remove_cmd, f)


        #raw_tbl_name = data_file.split("/")[-1].split(".")[0]
        if clean_and_serialize != False:
            if clean_and_serialize not in ["feather","pickle"]:
                raise Exception("Serialize must be either 'feather' or 'pickle'")
            print("Reading Table into memory...")
            #Need low_memory flag or else with large datasets we will end up with mixed datatypes
            if concat_str or did_partition == False:
                #If we have a concat_str, that means we need to read _df into memory for the first time
                #If it's false, that means that we already have it in memory from doing a horizontal combining
                _df = pd.DataFrame()
                try:
                    _df = pd.read_csv(data_file, names=col_list, sep="|", low_memory=False)
                except Exception as e:
                    pass
                if len(_df) == 0:
                    _df = pd.read_csv(data_file, names=col_list, sep="|", low_memory=False, encoding='latin1')

            print("Cleaning data...")
            for col in _df.columns.tolist():
                if _df[col].dtype == "object":
                    #_df[col] = _df[col].apply(lambda x: x.str.strip())
                    _df[col] = _df[col].apply(lambda x: np.nan if pd.isnull(x) else x.strip())
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
                        #print("before force string")
                        check_nulls(_df["EPI_ID"])
                        force_string(_df,col)
                        #print("after force string")
                        check_nulls(_df["EPI_ID"])
                    except:
                        pass
            print("Pickling data....")
            if clean_and_serialize == "feather":
                _df.to_feather(f"{abs_path}/serialized/{table_name}.feather")
                print("Finished: Your data file is located at:")
                print(f"{abs_path}/serialized/{table_name}.feather")
            elif clean_and_serialize == "pickle":
                _df.to_pickle(f"{abs_path}/serialized/{table_name}.pkl")
                print("Finished: Your data file is located at:")
                print(f"{abs_path}/serialized/{table_name}.pkl")

            t2 = time.time()
            m, s = divmod(int(t2-t1), 60)
            h, m = divmod(m, 60)
            print(f"Process took: {h} hours {m} minutes {s} seconds")
            #If you pickle the data, then you will have the column metadata already
            return
        else:
            print("Finished: Your end data file is located at:")
            print(data_file)
            print("You have chosen to not clean or serialize your data and fast export does not support export column names. \
                    Be sure to gather and keep in order these column names.")


        t2 = time.time()
        m, s = divmod(int(t2-t1), 60)
        h, m = divmod(m, 60)
        print(f"Process took: {h} hours {m} minutes {s} seconds")
    except Exception as e:
        print(f"Error: {e}")
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)
        return

    return(col_list)
