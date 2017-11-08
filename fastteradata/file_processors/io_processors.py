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

def combine_partitioned_file(script_files):
    import os
    concat_str = ""
    file_delim = ""
    remove_cmd = ""
    #Making special exceptions for windows computers
    if os.name == "nt":
        concat_str += "type "
        file_delim = "\\"
        remove_cmd = "del "
    else:
        concat_str += "cat "
        file_delim = "/"
        remove_cmd = "rm "

    #First we need to add data into the file path to locate our correct files
    data_files = []
    for file in script_files:
        l = file.split("/")
        l.insert(-1,"data")
        l[-1] = l[-1][7:]
        data_files.append(file_delim.join(l))

    for f in data_files:
        concat_str += f"{f} "

    #Now Build up concat string
    #Remove the partition value from the filepath
    concat_str += "> "
    form = data_files[0].split("/")
    last_form  = form[-1].split("_")
    del last_form[-2]
    fixed = "_".join(last_form)
    form[-1] = fixed

    #join and execute command
    concat_str += file_delim.join(form)
    from subprocess import call
    c = concat_str.split(" ")
    #print("concat stringg.....")
    concat_str = concat_str.replace("\\\\","\\")
    concat_str = concat_str.replace("//","/")
    #print(concat_str)


    #print(data_files)
    #clean data_files
    data_files = [x.replace("\\\\","\\") for x in data_files]
    data_files = [x.replace("//","/") for x in data_files]


    return("/".join(form), concat_str, data_files, remove_cmd)

def concat_files(concat_str):
    from subprocess import call
    call(concat_str, shell=True)
    return

def remove_file(remove_cmd, f):
    from subprocess import call
    call(f"{remove_cmd} {f}", shell=True)
    return

def save_file(export_path, file_name, file_contents):
    script_path = export_path + "/script_" + file_name

    with open(script_path, "w") as text_file:
        text_file.write(file_contents)

    return(script_path)
