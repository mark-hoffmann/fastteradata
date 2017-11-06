import pytest
import fastteradata as ftd

script_files = ['C://Users/u374781/Desktop/FastTeradata/dev_files/TEMP/script_CNSLDTD_DRUG_OKLAHOMA_2013_export.txt', \
                'C:/Users/u374781/Desktop/FastTeradata/dev_files/TEMP/script_CNSLDTD_DRUG_OKLAHOMA_2014_export.txt', \
                'C:/Users/u374781/Desktop/FastTeradata/dev_files/TEMP/script_CNSLDTD_DRUG_OKLAHOMA_2015_export.txt', \
                'C:/Users/u374781/Desktop/FastTeradata/dev_files/TEMP/script_CNSLDTD_DRUG_OKLAHOMA_2016_export.txt', \
                'C:/Users/u374781/Desktop/FastTeradata/dev_files/TEMP/script_CNSLDTD_DRUG_OKLAHOMA_2017_export.txt']
valid_concat_str_windows = "type C:\\Users\\u374781\\Desktop\\FastTeradata\\dev_files\\TEMP\\data\\CNSLDTD_DRUG_OKLAHOMA_2013_export.txt C:\\Users\\u374781\\Desktop\\FastTeradata\\dev_files\\TEMP\\data\\CNSLDTD_DRUG_OKLAHOMA_2014_export.txt C:\\Users\\u374781\\Desktop\\FastTeradata\\dev_files\\TEMP\\data\\CNSLDTD_DRUG_OKLAHOMA_2015_export.txt C:\\Users\\u374781\\Desktop\\FastTeradata\\dev_files\\TEMP\\data\\CNSLDTD_DRUG_OKLAHOMA_2016_export.txt C:\\Users\\u374781\\Desktop\\FastTeradata\\dev_files\\TEMP\\data\\CNSLDTD_DRUG_OKLAHOMA_2017_export.txt > C:\\Users\\u374781\\Desktop\\FastTeradata\\dev_files\\TEMP\\data\\CNSLDTD_DRUG_OKLAHOMA_export.txt"
valid_concat_str_linux = "cat C:/Users/u374781/Desktop/FastTeradata/dev_files/TEMP/data/CNSLDTD_DRUG_OKLAHOMA_2013_export.txt C:/Users/u374781/Desktop/FastTeradata/dev_files/TEMP/data/CNSLDTD_DRUG_OKLAHOMA_2014_export.txt C:/Users/u374781/Desktop/FastTeradata/dev_files/TEMP/data/CNSLDTD_DRUG_OKLAHOMA_2015_export.txt C:/Users/u374781/Desktop/FastTeradata/dev_files/TEMP/data/CNSLDTD_DRUG_OKLAHOMA_2016_export.txt C:/Users/u374781/Desktop/FastTeradata/dev_files/TEMP/data/CNSLDTD_DRUG_OKLAHOMA_2017_export.txt > C:/Users/u374781/Desktop/FastTeradata/dev_files/TEMP/data/CNSLDTD_DRUG_OKLAHOMA_export.txt"

valid_rm_cmd_windows = "del "
valid_rm_cmd_linux = "rm "

valid_data_file_windows = "C:\\\\Users\\u374781\\Desktop\\FastTeradata\\dev_files\\TEMP\\data\\CNSLDTD_DRUG_OKLAHOMA_export.txt"
valid_data_file_linux = "C:/Users/u374781/Desktop/FastTeradata/dev_files/TEMP/data/CNSLDTD_DRUG_OKLAHOMA_export.txt"

data_file, concat_str, data_files, remove_cmd = ftd.combine_partitioned_file(script_files)



def test_combine_partitioned_files_valid_concat():
    import os
    if os.name == "nt":
        assert valid_concat_str_windows == concat_str
    else:
        assert valid_concat_str_linux == concat_str

def test_combine_partitioned_files_valid_remove():
    import os
    if os.name == "nt":
        assert valid_rm_cmd_windows == remove_cmd
    else:
        assert valid_rm_cmd_linux == remove_cmd

def test_combine_partitioned_files_valid_data_file():
    import os
    if os.name == "nt":
        assert valid_data_file_windows == data_file
    else:
        assert valid_data_file_linux == data_file
