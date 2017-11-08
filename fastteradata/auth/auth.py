import json
import os


def read_credential_file():
    auth = {}
    auth_dict = {}
    env_dict = {}
    if os.path.exists(os.path.expanduser('~/.fastteradata')):
        auth = json.load(open(os.path.expanduser('~/.fastteradata')))
        auth_dict = auth["auth_dict"]
        env_dict = auth["env_dict"]

    return(auth, auth_dict, env_dict)

def load_db_info(custom_auth=False):

    auth, auth_dict, env_dict = read_credential_file()

    env_n = env_dict[env][0]
    env_dsn = env_dict[env][1]
    env_short = env_dict[env][2]
    if not custom_auth:
        usr = auth_dict[env][0]
        passw = auth_dict[env][1]
    else:
        usr = auth_dict[0]
        passw = auth_dict[1]

    return(env_n, env_dsn, env_short, usr, passw)
