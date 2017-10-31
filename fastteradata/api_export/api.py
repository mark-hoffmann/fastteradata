import sys
import time
import pandas as pd
import numpy as np
import os

from joblib import Parallel, delayed

from ..file_processors.file_processors import *
from ..file_processors.io_processors import *
from ..metadata_processors.metadata_processors import *
"""
def call_sub(f):
    import sys
    import subprocess
    print(f"Calling Fast Export on file...  {f}")
    sys.stdout.flush()
    subprocess.call(f"fexp < {f}", shell=True)
    sys.stdout.flush()
    return("")
"""
def extract_table(abs_path, table_name, env, db, nrows=-1, connector = "teradata", columns = [], clean_and_pickle=True, partition_key="", partition_type="year"):
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
            data_file = combine_partitioned_file(fexp_scripts)
        else:
            data_file = fexp_scripts[0]

        #raw_tbl_name = data_file.split("/")[-1].split(".")[0]

        if clean_and_pickle:
            print("Reading Table into memory...")
            _df = pd.read_csv(f"{abs_path}/data/{table_name}_export.txt", names=col_list, sep="|")
            print("Cleaning data...")
            for col in _df.columns.tolist():
                if _df[col].dtype == "object":
                    _df[col] = _df[col].str.strip()
                    _df[col] = _df[col].apply(lambda x: np.nan if ((pd.notnull(x)) and ('missing' in x.lower())) else x)
            _df.replace("~",np.nan,inplace=True)
            _df.replace("!",np.nan,inplace=True)
            _df.replace("?",np.nan,inplace=True)
            _df.replace("",np.nan,inplace=True)

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
            print(f"{abs_path}/pickled/{raw_tbl_name}.pkl")
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
