import sys
import time
import pandas as pd
import numpy as np
import os

from joblib import Parallel, delayed

from ..load_processors.script_generators import *
from ..load_processors.table_generators import *


def load_table(abs_path, df, table_name, env, db, connector = "teradata", clear_table=True):


    if not os.path.isdir(f"{abs_path}/data_loading"):
        os.makedirs(f"{abs_path}/data_loading")
    if not os.path.isdir(f"{abs_path}/data_loading_scripts"):
        os.makedirs(f"{abs_path}/data_loading_scripts")

    file_delim = ""
    if os.name == "nt":
        file_delim = "\\"
    else:
        file_delim = "/"

    f, _ = generate_fastload_script(abs_path, df, table_name, env, db)

    prep_load_table(df, table_name, env, db, connector, clear_table)

    df.to_csv(f"{abs_path}{file_delim}data_loading{file_delim}{table_name}.csv", sep="|", index=False)

    import subprocess
    print(f"Calling Fast Load on file...  {f}")
    subprocess.call(f"fastload < {f}", shell=True)



    return
