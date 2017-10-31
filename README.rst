fastteradata
===

.. image:: https://img.shields.io/pypi/v/fastteradata.svg
    :target: https://pypi.python.org/pypi/fastteradata
    :alt: Latest PyPI version


Tools for faster and optimized interaction with Teradata and large datasets. This was initially made to make using Teradata's fast export utility easier and get around various spool space issues that may arise from large tables.


Installation
------------

fastteradata can easily be downloaded from Pypi package index via the following:

.. code-block:: python

  pip install fastteradata


Usage
-----
The main task this package accomplishes right now is export large tables faster and easier than trying to load them directly.
This is accomplished by connecting to a Teradata account and reading metadata about the table of interest to autogenerate and execute the appropriate fastexport script.
You then have the option to read the data into memory to clean it and then serialize it as a pickle file with correct data types and columns.
|
Additional components will continue to be worked on such as:
- Better optimization of auto parrellelization
- The ability to join different tables for an export
- Fastloading capabilities
- Plus popular suggestions from others in the issues

**Setup**
Before you can use fasteradata, you must create a file in your working home directory called ".fastteradata".
This file follows json formatting rules and an example can be found `here <https://github.com/mark-hoffmann/fastteradata/blob/master/.example_fastteradata>`_.
|
Once you have your credential file set up, you are ready to go.

**Extracting a table**

An example of using the module is as follows:

.. code-block:: python
   import fastteradata as ftd
   ftd.extract_table("/absolute/path/to/output", "TABLE_NAME", "ENV_NAME", "DB_NAME", nrows=50, connector="pyodbc")

This particular call will
- Create a *data* and *pickled* folder in the directory of your absolute path argument
- Connect to Teradata via the connector "pyodbc"
- Read metadata about the table
- Generate a fast export script in your absolute path directory
- Execute the script and populate a data file with the first 50 rows of the table into the data/ subdirectory
- Read in the data file and attempt to clean it with np.nans where appropriate as well as appropriate typecasting
- Save the resulting pandas dataframe into the pickled/ subdirectory

|
|

**Method Signatures**

While this project is small, the method signatures can be found below. If this starts to become much larger, I will generate Sphinx docs.
|
**extract_table(abs_path, table_name, env, db, nrows=-1, connector = "teradata", columns = [], clean_and_pickle=True, partition_key="", partition_type="year")**
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

|
|



Requirements
^^^^^^^^^^^^
- `pandas <https://github.com/pandas-dev/pandas>`_
- `numpy <https://github.com/numpy/numpy>`_
- `joblib <https://github.com/joblib/joblib>`_
- `pyodbc <https://github.com/mkleehammer/pyodbc>`_
- `teradata <https://github.com/Teradata/PyTd>`_



Compatibility
-------------

fastteradata currently supports Python 3.4, 3.5, and 3.6

Licence
-------

`MIT <https://github.com/mark-hoffmann/fastteradata/blob/master/LICENSE.txt>`_

Authors
-------

`fastteradata` was written by `Mark Hoffmann <markkhoffmann@gmail.com>`_.
