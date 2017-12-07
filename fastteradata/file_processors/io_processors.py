import pandas as pd
import numpy as np

from ..auth.auth import read_credential_file, load_db_info
import os

import json

"""
auth = {}
auth_dict = {}
env_dict = {}
if os.path.exists(os.path.expanduser('~/.fastteradata')):
    auth = json.load(open(os.path.expanduser('~/.fastteradata')))
    auth_dict = auth["auth_dict"]
    env_dict = auth["env_dict"]

"""
auth, auth_dict, env_dict = read_credential_file()

def combine_files_base(combine_type=None):
    import os
    concat_str = ""
    file_delim = ""
    remove_cmd = ""
    #Making special exceptions for windows computers
    if os.name == "nt":
        file_delim = "\\"
        remove_cmd = "del "
    else:
        file_delim = "/"
        remove_cmd = "rm "
    if combine_type == "vertical":
        if os.name == "nt":
            concat_str += "type "
        else:
            concat_str += "cat "
    elif combine_type == "horizontal":
        #because windoes does not have a nice way to do this, we are going to read in and combine in python instead
        """
        if os.name == "nt":
            concat_str += " "
        else:
            concat_str += "paste -d '|' "
        """
        concat_str = False
    else:
        raise Exception("Internal Bug: Invalid combine_type")

    return(concat_str, file_delim, remove_cmd)

def combine_partitioned_file(script_files, combine_type=""):

    concat_str, file_delim, remove_cmd = combine_files_base(combine_type=combine_type)
    #First we need to add data into the file path to locate our correct files
    data_files = []
    for file in script_files:
        l = file.split("/")
        l.insert(-1,"data")
        l[-1] = l[-1][7:]
        data_files.append(file_delim.join(l))



    #Now Build up concat string
    #Remove the partition value from the filepath
    if concat_str:
        for f in data_files:
            concat_str += f"{f} "
        concat_str += "> "
    form = data_files[0].split("/")
    last_form  = form[-1].split("_")
    del last_form[-2]
    fixed = "_".join(last_form)
    form[-1] = fixed

    #join and execute command
    if concat_str:
        concat_str += file_delim.join(form)
        c = concat_str.split(" ")
        #print("concat stringg.....")
        concat_str = concat_str.replace("\\\\","\\")
        concat_str = concat_str.replace("//","/")
    #print(concat_str)


    #print(data_files)
    #clean data_files
    data_files = [x.replace("\\\\","\\") for x in data_files]
    data_files = [x.replace("//","/") for x in data_files]


    return(concat_str, data_files, remove_cmd)



def concat_files(concat_str):
    from subprocess import call
    call(concat_str, shell=True)
    return

def concat_files_horizontal(data_file, data_files, col_list, primary_keys, dtype_dict):
    _df = pd.DataFrame()
    #Combine data files in memory
    print("Concatenating Horizontally")
    for clist, d_file in zip(col_list,data_files):
        #print(d_file)
        #print(clist)
        #print(primary_keys)
        df = pd.DataFrame()
        try:
            df = pd.read_csv(d_file, names=clist, sep="|", dtype=dtype_dict, na_values=["?","","~","!"])
        except:
            pass
        if len(df) == 0:
            df = pd.read_csv(d_file, names=clist, sep="|", dtype=dtype_dict, na_values=["?","","~","!"], encoding='latin1')
        #print(df)
        if len(_df) == 0:
            _df = df
        else:
            _df = pd.merge(_df,df, how="inner", right_on=primary_keys,left_on=primary_keys)
        #print("Size of _df: " + str(len(_df)) + " " + str(len(_df.columns)) )

    #save dataframe from memory into a text file in the appropriate place
    print("Saving Horizontal concatenation file out")
    _df.to_csv(data_file,sep="|",index=False, header=None)

    return(_df)

def remove_file(remove_cmd, f):
    from subprocess import call
    call(f"{remove_cmd} {f}", shell=True)
    return

def save_file(export_path, file_name, file_contents):
    script_path = export_path + "/script_" + file_name

    with open(script_path, "w") as text_file:
        text_file.write(file_contents)

    return(script_path)
