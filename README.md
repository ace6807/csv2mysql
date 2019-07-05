# CSV to MySQL uploader

## To use load local data from pymysql
The mysql server and client need to be configured to allow loading from a local file.

#### To configure the server: 
`SET GLOBAL local_infile = ON` needs to be run from the mysql cli tool

#### To configure the client
`local_infile=True` needs to be passed to the connection constructor 

## To install dependencies
With your virtualenv activated, run `pip install -r requirements.txt`

## Running the application
To see help run: `python csv2mysql.py --help`

To run the application: `python csv2mysql.py <path_to_csv> <table_name>`