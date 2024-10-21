from db_connection import create_connection, close_connection
import pandas as pd
import os

def truncate_table(connection, table_name):
    try:
        cursor = connection.cursor()
        cursor.execute(f"TRUNCATE TABLE {table_name}")
        connection.commit()
        print(f"Table {table_name} truncated successfully.")
    except Exception as e:
        print(f"Error truncating table {table_name}: {e}")

# Insert data into SEC Business Line table
def SECBLTable(connection, SECTable, file_name, procedure_code, date):
    try:
        cursor = connection.cursor()

        # Insert the new record
        query = f"INSERT INTO {SECTable} (file_name, procedure_code, date) VALUES (%s, %s, %s)"
        values = (file_name.strip(), procedure_code.strip(), date)
        
        cursor.execute(query, values)
        connection.commit()
        print(f"Inserted: {file_name}, {procedure_code}, {date}")

    except Exception as e:
        print(f"Error inserting data into MySQL: {e}")


def ARISBLInsert(connection, ARISTable, procedure_name, procedure_code, date):
    try:
        cursor = connection.cursor()

        # Strip and truncate procedure_name and procedure_code
        procedure_name = procedure_name.strip()[:255]
        procedure_code = procedure_code.strip()[:255]

        query = f"INSERT INTO {ARISTable} (procedure_name, procedure_code, Date) VALUES (%s, %s, %s)"
        values = (procedure_name, procedure_code, date)
        
        cursor.execute(query, values)
        connection.commit()
        print(f"Inserted: {procedure_name}, {procedure_code}, {date}")

    except Exception as e:
        print(f"Error inserting data into MySQL: {e}")


def readDataFromARISExcel(ArisData, ARISTable):
    count = 0
    connection = create_connection()
    
    # Truncate the ARIS_DTTBL table before insertion
    truncate_table(connection, ARISTable)

    # Insert each row of ARIS data into the table
    for index, row in ArisData.iterrows():
        procedure_name = row['Process Name'] if 'Process Name' in row else ""
        procedure_code = row['Procedure Code'] if 'Procedure Code' in row else ""
        date = pd.to_datetime('today').date()  # Example of adding a timestamp
        count = count + 1
        ARISBLInsert(connection,ARISTable, procedure_name, procedure_code, date)

    print(f"Total number of procedrue and control in ARIS {count}")

    close_connection(connection)