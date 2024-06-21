# ETL with Python

<p align="center">
  <img src="docs/_images/Batalla naval - Foto proyecto 1.png"/>
</p>![image](https://github.com/floresernesto95/Images/blob/main/project-python-etl.png)

## Project Overview

This project, "ETL with Python," showcases a robust Extract, Transform, and Load (ETL) pipeline developed using Python. It demonstrates my proficiency in data extraction from web APIs, data transformation and manipulation using the `petl` library, and loading data into an MS SQL database. The project efficiently handles configuration management, error handling, and data integration tasks, making it a comprehensive example of a practical ETL solution.

## Key Features

- **Data Extraction**: Utilizes the `requests` library to fetch data from a web API, ensuring seamless integration with external data sources.
- **Data Transformation**: Leverages the `petl` library to perform data transformation operations such as parsing JSON data, converting data types, joining tables, and handling missing values.
- **Data Loading**: Establishes a connection to an MS SQL database using `pymssql` and efficiently loads the transformed data into the database.
- **Configuration Management**: Uses `configparser` to manage configuration settings, ensuring the flexibility and reusability of the code.
- **Error Handling**: Implements robust error handling mechanisms to manage potential issues during the ETL process, ensuring reliability and stability.

## Technical Details

The ETL process is implemented in Python with the following key components:

1. **Configuration Management**:
    - Reads configuration settings from an `ETL-Python.ini` file, including start date, API URL, server, and database details.

2. **Data Extraction**:
    - Sends a GET request to the API using the `requests` library.
    - Handles potential request errors gracefully to maintain the integrity of the ETL process.

3. **Data Transformation**:
    - Parses the JSON response from the API.
    - Extracts relevant data (dates and rates) and converts them into appropriate data types.
    - Joins the extracted data with existing expense data from an Excel file.
    - Fills missing values, removes rows with no expenses, and adds new calculated fields.

4. **Data Loading**:
    - Connects to an MS SQL database using `pymssql`.
    - Loads the transformed data into the `Expenses` table in the database.
    - Handles potential database connection and writing errors to ensure data consistency.

## Conclusion

"ETL with Python" demonstrates my ability to build efficient and reliable ETL pipelines using Python. This project highlights my skills in data extraction, transformation, and loading, along with my attention to detail in handling configurations and errors. It is a testament to my capability to develop solutions that integrate and manage data from multiple sources, ensuring data consistency and reliability.
