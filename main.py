# Import the necessary Python libraries.
import sys            # Provides access to some variables used or maintained by the interpreter.
import petl           # A library for extracting, transforming, and loading tables of data.
import pymssql        # A library for connecting to an MS SQL database.
import configparser   # Used for handling configuration files.
import requests       # Allows you to send HTTP requests using Python.
import datetime       # Supplies classes for manipulating dates and times.
import json           # Provides JSON encoder and decoder.
import decimal        # Supports exact decimal floating point arithmetic.


# Create an instance of the ConfigParser class.
config = configparser.ConfigParser()


# Attempt to read the configuration file.
try:
    config.read('ETL-Python.ini')
except Exception as e:
    print('Could not read configuration file:' + str(e))
    sys.exit()


# Retrieve configurations from the 'CONFIG' section of the configuration file.
startDate = config['CONFIG']['startDate']
url = config['CONFIG']['url']
destServer = config['CONFIG']['server']
destDatabase = config['CONFIG']['database']


# Try to send a web request.
try:
    # The requests.get function is used to perform a GET request.
    # The URL for the GET request is created by concatenating 'url' and 'startDate'.
    # 'url' might be a base path, and 'startDate' might be a parameter or path extension.
    # The result of the GET request is stored in the variable BOCResponse.
    BOCResponse = requests.get(url+startDate)
except Exception as e:
    print('could not make request:' + str(e))
    # Terminate the program using sys.exit().
    # This ensures that the program does not continue executing if the request fails,
    # which might be crucial if subsequent code depends on the success of this request.
    sys.exit()


# Initialize an empty list to store dates from the BOCResponse data.
BOCDates = []
# Initialize an empty list to store rates from the BOCResponse data.
BOCRates = []


# Check if the web request was successful by verifying the status code.
if (BOCResponse.status_code == 200):
    # If the request was successful, parse the JSON response text into a Python dictionary.
    BOCRaw = json.loads(BOCResponse.text)

    # Extract observation data into the column arrays (lists).
    for row in BOCRaw['observations']:
        # Convert the date string to a datetime object and append it to BOCDates.
        BOCDates.append(datetime.datetime.strptime(row['d'], '%Y-%m-%d'))
        # Convert the rate value to a decimal object and append it to BOCRates.
        BOCRates.append(decimal.Decimal(row['FXUSDCAD']['v']))

    # Create a petl table from the BOCDates and BOCRates lists and rename the columns to 'date' and 'rate'.
    exchangeRates = petl.fromcolumns([BOCDates, BOCRates], header=['date', 'rate'])

    # Load the expense document from an Excel file.
    try:
        expenses = petl.io.xlsx.fromxlsx('resources/Expenses.xlsx', sheet='Github')
    except Exception as e:
        print('could not open Expenses.xlsx:' + str(e))
        sys.exit()

    # Join the exchangeRates table with the expenses table on the 'date' column.
    expenses = petl.outerjoin(exchangeRates, expenses, key='date')

    # Fill down missing 'rate' values in the expenses table.
    expenses = petl.filldown(expenses, 'rate')

    # Remove rows where there are no expenses (i.e., where the 'USD' field is None).
    expenses = petl.select(expenses, lambda rec: rec.USD != None)

    # Add a new column 'CAD' to the expenses table, converting 'USD' to 'CAD' using the exchange rate.
    expenses = petl.addfield(expenses, 'CAD', lambda rec: decimal.Decimal(rec.USD) * rec.rate)

    # Initialize a database connection to the specified server and database.
    try:
        dbConnection = pymssql.connect(server=destServer, database=destDatabase)
    except Exception as e:
        print('could not connect to database:' + str(e))
        sys.exit()

    # Populate the Expenses table in the database with the data from the expenses table.
    try:
        petl.io.todb(expenses, dbConnection, 'Expenses')
    except Exception as e:
        print('could not write to database:' + str(e))
