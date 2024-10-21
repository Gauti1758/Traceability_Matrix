import pandas as pd
from matrix_handler import process_data, update_unique_procedure_code, update_status_table, create_summary_report, updateRedList, procedureCodeOccurance
from db_operations import SECBLTable, truncate_table, readDataFromARISExcel
from db_connection import create_connection, close_connection
import os


def main():
    count = 0
    #Table name of database
    SECTable = "SEC_DTTBL"
    ARISTable = "ARIS_DTTBL"
    StatusTable = "Status_DTTBL"

    # Path of source Excel file
    SEC_file_path = 'SEC_Source_Docs/SEC_Library_DTTBL.xlsx'
    ARIS_file_path = 'ARIS_Source_Docs/ARIS_DTTBL.xlsx'

    # Read the Excel file
    SecData = pd.read_excel(SEC_file_path)
    ArisData = pd.read_excel(ARIS_file_path)

    # Process the data using the logic from matrix_handler.py
    processed_df, total_matched, total_unmatched = process_data(SecData)

    # Print names and procedure codes
    for index, row in processed_df.iterrows():
        count += 1
        print(f"Name: {row['Name']}, Procedure Code: {row['Procedure_Code']}")

    print()
    print(f"Total Number of Procedures: {count}")
    print(f"Total Matched Procedure Codes: {total_matched}")
    print(f"Total No Procedure Codes: {total_unmatched}")

    output_path = 'Output_Docs/SEC_FinanceBL_Extracted.xlsx'
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    processed_df.to_excel(output_path, index=False)
    print(f"Processed data saved to {output_path}")

    New_Data = 'SEC_Source_Docs/SEC_DTTBL_allProcedure.xlsx'
    SecNewData = pd.read_excel(New_Data)
    # Insert processed data into the database
    connection = create_connection()
    if connection:
        truncate_table(connection, SECTable)

        for index, row in SecNewData.iterrows():
            # Insert data into SEC Business Line table
            SECBLTable(connection, SECTable, row['Name'].strip(), row['Procedure_Code'].strip(), pd.to_datetime('today').date())
        close_connection(connection)

    # Call the function
    update_unique_procedure_code(SECTable)

    readDataFromARISExcel(ArisData, ARISTable)

    update_status_table(StatusTable, SECTable, ARISTable)
    

#     # Save the processed data to a new Excel file
#     Aris_output_path = 'Output_Docs/Aris_procedure.xlsx'
    # generate_status_excel('Output_Docs/FinanceBL_Status.xlsx', StatusTable)
    
#     # Ensure output directory exists

#     os.makedirs(os.path.dirname(Aris_output_path), exist_ok=True)
#     ArisData[['Process Name', 'Procedure Code']].to_excel(Aris_output_path, index=False)
    
#     ArisNewData = pd.read_excel(Aris_output_path)

    updateRedList(SECTable, ARISTable)
    create_summary_report(SECTable, ARISTable, StatusTable)
    # procedureCodeOccurance(SECTable, ARISTable)

    # Check procedure status
    # procedureStatus(processed_df, ArisNewData)

if __name__ == "__main__":
    main()