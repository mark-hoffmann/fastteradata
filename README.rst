fastteradata
============

.. image:: https://travis-ci.org/mark-hoffmann/fastteradata.png
   :target: https://travis-ci.org/mark-hoffmann/fastteradata
   :alt: Latest Travis CI build status

.. image:: https://codecov.io/gh/mark-hoffmann/fastteradata/branch/master/graph/badge.svg
   :target: https://codecov.io/gh/mark-hoffmann/fastteradata
   :alt: Coverage

Tools for faster and optimized interaction with Teradata and large datasets. This was initially made to make using Teradata's fast export utility easier and get around various spool space issues that may arise from large tables.


Installation
------------

fastteradata can easily be downloaded straight from github via the following:

.. code-block:: python

  pip install git+https://github.com/mark-hoffmann/fastteradata.git


Usage
-----
The main task this package accomplishes right now is export large tables faster and easier than trying to load them directly.
This is accomplished by connecting to a Teradata account and reading metadata about the table of interest to autogenerate and execute the appropriate fastexport script.
You then have the option to read the data into memory to clean it and then serialize it as a feather or pickle file with correct data types and columns.

**Major Updates**

*Update 11/10/2017*

Basic fastloading capabilities have been added for doing a fastload from a pandas dataframe

*Update 11/24/2017*

Major update to data type robustness on export

*Update 12/1/2017*

Auto horizontal scaling merged
Feather serialization merged


Additional components will continue to be worked on such as
 * Better optimization of auto parrellelization
 * The ability to join different tables for an export
 * Plus popular suggestions from others in the issues


**Setup**

Before you can use fasteradata, you must create a file in your working home directory called ".fastteradata", such that the path is "~/.fastteradata".
This file follows json formatting rules and an example can be found `here <https://github.com/mark-hoffmann/fastteradata/blob/master/.example_fastteradata>`_. All fields surround with underscores need to be updated. Don't necessary need two different environments.

|

Once you have your credential file set up, you are ready to go.

**Extracting a table**

An example of exporting is as follows:

.. code-block:: python

   import fastteradata as ftd

   ftd.extract_table("/absolute/path/to/output", "TABLE_NAME", "ENV_NAME", "DB_NAME", nrows=50, connector="pyodbc")

This particular call will
 * Create a *data* and *serialized* folder in the directory of your absolute path argument
 * Connect to Teradata via the connector "pyodbc"
 * Read metadata about the table
 * Generate a fast export script in your absolute path directory
 * Execute the script and populate a data file with the first 50 rows of the table into the data/ subdirectory
 * Read in the data file and attempt to clean it with np.nans where appropriate as well as appropriate typecasting
 * Save the resulting pandas dataframe into the serialized/ subdirectory

|
|

**Loading a table**

An example of loading is as follows:

.. code-block:: python

   import fastteradata as ftd

   ftd.load_table("/absolute/path/for/files", df,  "TABLE_NAME", "ENV_NAME", "DB_NAME", clear_table=True)

This call will
 * Take the datatypes from your dataframe and generate a fastload script file
 * Save your dataframe to the appropriate csv file that fastload expects
 * Since the clear_table flag is True (True by default as well) it will DROP THE CURRENT TABLE if one exists with that name in that databse, and recreate a new table to fill in
 * Execute the script to populate the database table

|
|

**Method Signatures**
---------------------

While this project is small, the method signatures can be found below. If this starts to become much larger, I will generate Sphinx docs.

|

**extract_table(abs_path, table_name, env, db, nrows=-1, connector = "teradata", columns = [], clean_and_serialize="feather", partition_key="", partition_type="year")**

*Summary*

Extracts table information from Teradata and saves / executes the appropriate files

*Args*

abs_path (str): Absolute path where you want your scripts to reside and data and serialized subdirectories made

table_name (str): Teradata table name you wish to query

env (str): Environment that you want to connect to as specified in your .fastteradata file. (People usually have a testing and production environment)

db (str): Database name to connect to

nrows (int): *default = -1* The default of -1 means ALL rows. Otherwise, you can specificy a subset of rows such as 20

connector (str): *default = 'teradata'* The default uses the teradata python module to connect to the cluster. Valid options include 'teradata' and 'pyodbc'

columns (list(str)): *default = []* Subset of columns to use, default is all of the columns found in the metadata, however subsets can be selected by passing in ['col1','col2','col3']

clean_and_serialize (str): *default = "feather"* Refers to if you want to read the resulting data file into memory to clean and then serialize in your serialized subdirectory. Available options incldue 'feather', 'pickle', and False  False is if you do not want to serialize the resulting dataframe, but just get the text.

partition_key (str): *default = ''* There is no partitioning by default. When you define a partition key, it MUST BE A DATE COLUMN AS DEFINED IN TERADATA. This breaks up the exporting into paritions by the *partition_type* argument. This generates multiple fexp scripts and executes them in parrelel using the available cores. This helps to break up extremely large datasets or increase speed. When a parition key is defined, after all of the partition files are finished loading from Teradata, the resulting data is COMBINED into a SINGLE DATA FILE and finishes processing through the following cleaning, data type specification, and serializing.

partition_type (str): *default = 'year'* Default is to partition the partition_key by distict YEAR. Valid options include "year" or "month"

primary_keys (list(str)): *default = []* This is required any time that horizontal partitioning is required. Horizontal partitioning is done automatically when there are more than 100 columns you are trying to extract from a table. The list of column names should be the columns that define a unique row in the dataset. If these do not define a unique row, you will recieve unexpected behavior of unwanted rows appearing in yoru dataset. This scaling feature is required because of the limitationso f Teradata's fastexport utility. It is to abstract back the headache of having to deal with more columns than the utility can handle.

*Returns*

Column list received from the metadata if clean_and_serialize is set to False, else nothing. Column names are returned in this case so you can save them and use them to read the raw data file later with appropriate columns.

|
|

**load_table(abs_path, df, table_name, env, db, connector = "teradata", clear_table=True)**

*Summary*

Loads a pandas dataframe from memory into teradata via the optimized fastload functionality.

*Args*

abs_path (str): Absolute path where you want your scripts to reside and appropriate subdirectories made (Most of the time should be same absolute path as the extract_table abs_path)

df (pandas DataFrame): The pandas DataFrame that you want to save up to teradata

table_name (str): The desired table name

env (str): Environment that you want to connect to as specified in your .fastteradata file. (People usually have a testing and production environment)

db (str):  Database name to connect to

connector (str): *default = 'teradata'* The default uses the teradata python module to connect to the cluster. Valid options include 'teradata' and 'pyodbc'

clear_table (bool): *default = 'True'* This specifies if you want the table you specify in your db to be dropped before loading in. Right now, this is the recomended way of using this function. Otherwise, you have to be sure to have the columns exactly right and error handling has not been robustly built out for that case yet.

*Returns*

Nothing

Requirements
^^^^^^^^^^^^
- `pandas <https://github.com/pandas-dev/pandas>`_
- `numpy <https://github.com/numpy/numpy>`_
- `joblib <https://github.com/joblib/joblib>`_
- `pyodbc <https://github.com/mkleehammer/pyodbc>`_
- `teradata <https://github.com/Teradata/PyTd>`_
- `feather-format <https://github.com/wesm/feather>`_



Compatibility
-------------

fastteradata currently supports Python 3.6

Licence
-------

`MIT <https://github.com/mark-hoffmann/fastteradata/blob/master/LICENSE.txt>`_

Authors
-------

`fastteradata` was written by `Mark Hoffmann <markkhoffmann@gmail.com>`_.
