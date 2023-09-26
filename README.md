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
1. Download the code `tpch-pgsql` from the link: [https://github.com/Data-Science-Platform/tpch-pgsql/tree/master](https://github.com/ahanapradhan/tpch-pgsql).  
2. Follow the `tpch-pgsql` project Readme to prepare and load the data.  
3. (In case the above command gives error as `malloc.h` not found, showing the filenames, go inside dbgen folder, open the file and replace `malloc.h` with `stdlib.h`)  

# Setting up IDE  
A developement environment for python project is required next. Here is the link to PyCharm Community Edition: https://www.jetbrains.com/pycharm/download/  (Any other IDE is also fine)

# Setting Up the UN1+UNION UNMASQUE Code
This code corresponds to UNMASQUE Project: https://dsl.cds.iisc.ac.in/projects/HIDDEN/index.html  
UNMASQUE code for https://dsl.cds.iisc.ac.in/publications/report/TR/TR-2021-02_updated.pdf    
This repository contains code developed by various students who worked on the project.   
The code is organized into the following directories:  

## mysite

The `mysite` directory contains the main project code.

### unmasque

Inside `unmasque`, you'll find the following subdirectories:

#### refactored

The `refactored` directory contains code that has been refactored from the original codebase developed in various theses. This code has likely undergone improvements and optimizations.

#### src

The `src` directory contains newly written logic, often designed to simplify existing code. This may include enhancements or entirely new functionality.

#### test

The `test` directory houses unit test cases for each extractor module. These tests are crucial for ensuring the reliability and correctness of the code.

Please explore the individual directories for more details on the code and its purpose.

## Usage

### Configuration
inside `mysite` directory, there are three files as follows:  
config.ini --> This contains database login credentials. Change the fields accordingly.  
create_indexes.sql --> currently empty.  (do not delete this file.)  
pkfkrelations.csv --> contains key details for the TPCH schema. If any other schema is to be used, change this file accordingly.  

### Running Unmasque
Open `mysite/unmasque/src/main_cmd.py` file.  
This script has one default input specified.  
Change this query to try Unmasque for various inputs.  
`test.util` package has `queries.py` file, containing few sample queries. Any of them can be used for testing.

#### From Command Line:
Change current directory to `mysite`.
Use the following command:  
`python -m unmasque.src.main_cmd` 

#### From IDE:
the `main` function in main_cmd.py can be run from the IDE.  

(Current code uses relative imports in main_cmd.py script. If that causes import related error while trying to run from IDE, please change the imports to absolute.)

