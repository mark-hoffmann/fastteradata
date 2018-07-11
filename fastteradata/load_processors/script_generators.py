import sys
import time
import pandas as pd
import numpy as np
import os

from joblib import Parallel, delayed
from ..auth.auth import read_credential_file, load_db_info

def save_load_file(script_path, contents):
    with open(script_path, "w") as text_file:
        text_file.write(contents)
    return


def generate_fastload_script(abs_path, df, table_name, env, db):

    env_n, env_dsn, env_short, usr, passw = load_db_info(env)

    beginning_string = f"logon {env_n}/{usr},{passw};\n\nSET RECORD VARTEXT '|';\nRECORD 2;\nDEFINE\n"

    define_string = ""
    col_type_zip = zip(df.columns.tolist(),df.dtypes.tolist())
    for col, col_type in col_type_zip:
        if col_type.str == "|O":
            try:
                size = df[col].map(len).max()
                define_string += f"{col} (VARCHAR({size})),\n"
            except:
                define_string += f"{col} (VARCHAR(90)),\n"
        else:
            define_string += f"{col} (VARCHAR(90)),\n"

    file_delim = ""
    if os.name == "nt":
        file_delim = "\\"
    else:
        file_delim = "/"
    define_string += f"FILE = {abs_path}{file_delim}data_loading{file_delim}{table_name}.csv;\n\n"

    table_string = f"BEGIN LOADING {db}.{table_name}\nERRORFILES {db}.err1, {db}.err2;\n\n"

    insert_string = f"INSERT INTO {db}.{table_name}\nVALUES (\n"
    for col in df.columns.tolist():
        insert_string += f": {col}"
        if col != df.columns.tolist()[-1]:
            insert_string += ",\n"
    insert_string += ");\nEND LOADING;\nLOGOFF;"


    final_string = beginning_string + define_string + table_string + insert_string


    script_path = abs_path + f"{file_delim}data_loading_scripts{file_delim}{table_name}.txt"

    save_load_file(script_path, final_string)

    return(script_path, final_string)
