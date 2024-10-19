


# Setting Up the Database
## PostgreSQL Installation  

Follow the link https://www.postgresql.org/download/ to download and install the suitable distribution of the database for your platform. 

## Loading TPCH Data  

### Obtaining DBGEN
1. Open the TPC webpage following the link: https://www.tpc.org/tpc_documents_current_versions/current_specifications5.asp  
2. In the `Active Benchmarks` table (first table), follow the link of `Download TPC-H_Tools_v3.0.1.zip`, it'll redirect to `TPC-H Tools Download
` page   
3. Give your details and click `download`, it'll email you the download link. Use the link to download the zip file.  
4. Unzip the zip file, and it must have the `dbgen` folder among the extracted contents  

### Prepare TPCH data on PostgreSQL using DBGEN
1. Download the code `tpch-pgsql` from the link: https://github.com/Data-Science-Platform/tpch-pgsql/tree/master.  
2. Follow the `tpch-pgsql` project Readme to prepare and load the data.  
3. (In case the above command gives error as `malloc.h` not found, showing the filenames, go inside dbgen folder, open the file and replace `malloc.h` with `stdlib.h`)

## Sample TPCH Data  
TPCH 100MB (sf=0.1) data is provided at this repo. 
The load.sql file in the folder needs to be updated with the corresponding location of the data .csv files.

## Loading TPCH Data using DuckDB
https://duckdb.org/docs/extensions/tpch.html

### Requirements
* Python 3.8.0 or above
* `django==4.2.4`
* `sympy==1.4`
* `psycopg2==2.9.3`
* `numpy==1.22.4`

# Setting Up the Code

The code is organized into the following directories:  

## mysite

The `mysite` directory contains the main project code.

### hqe

Inside `hqe`, you'll find the following subdirectories:

#### src

The `src` directory contains code that has been refactored from the original codebase developed in various theses, as well as newly written logic, often designed to simplify existing code. This may include enhancements or entirely new functionality.

#### test

The `test` directory houses some test cases.

# Usage

## Configuration
inside `mysite` directory, there are two files as follows:    
* `pkfkrelations.csv`: contains key details for the TPCH schema. If any other schema is to be used, change this file accordingly.  
* `config.ini`: This contains database login credentials and flags for optional features. Change the fields accordingly.    

### Config File Details:
`database` section: set your database credentials.  

`support` section: give support file name. The support file should be present in the same directory of this config file.

`logging` section: set logging level. The developer mode is `DEBUG`. Other valid levels are `INFO`, `ERROR`.

`feature` section: set flags for advanced features, as the flag names indicate. Included features are, `UNION`, `OUTER JOIN`, `<>` or `!=` operator in arithmetic filter predicates and `IN` operator. 

`options` section: extractor options. E.g. the maximum value for `LIMIT` clause is 1000. If the user needs to set a higher value, use `limit=value`.


### Running XPOSE:
#### From Command Line:
1. Open Terminal.  
2. Change the current directory to `mysite`.  
3. Use the following command:  
`python3 -m hqe.src.main_cmd F4 Extract`  --> to extract for hidden Query F4  

#### Usage:  

* python3 -m hqe.src.main_cmd <QID> --> Will just execute the query in db and show the timing  

* python3 -m hqe.src.main_cmd <QID> Extract --> Will execute the query in db and then extract it using XPOSE, then show the timing profile  


#### Workload:

The following query keys are available for running the extraction experiments:  

* Pure Union Queries: U1, U2, U3, U4, U5, U6, U7, U8, U9  

* Algebraic Predicate Queries: A1, A2, A3, A4, A5  

* NEP query: N1  

* OJ Queries: O1, O2, O3, O4, O5, O6  

* Fusion Queries: F1, F2, F3, F4  

