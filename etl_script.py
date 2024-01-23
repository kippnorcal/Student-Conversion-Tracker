import pandas as pd

from sqlsorcery import MSSQL
from pandas import DataFrame, to_numeric,merge
from pygsheets import Address, authorize, DataRange, GridRange, Worksheet
from typing import Union, List

#####################################################################
#                                                                   #
#          Extract: Query data from Data Warehouse and Gsheets      #
#                                                                   #
#####################################################################



def _data_warehouse_query() -> DataFrame:
    return MSSQL().query_from_file("./sql/enrollment.sql")



def _connect_to_gsheet(spreadsheet_ID: str, sheet_name: str) -> Union[Worksheet, None]:
    gsheet = None
    try:
        client = authorize(service_file='./service_account_credentials.json')
        sheet = client.open_by_key(spreadsheet_ID)
        gsheet = sheet.worksheet_by_title(sheet_name)
    except Exception as e:
        print('Error connecting to Google Sheet')
    return gsheet
    




#####################################################################
#                                                                   #
#        Transformations: Process data into formatted dataframes    #
#                                                                   #
#####################################################################


def _format_dw_query(df: DataFrame) -> DataFrame:
    _dw_df_type_conversion(df)
    _rename_dw_df_columns(df)
    df.fillna("", inplace=True)
    return df
    
def _dw_df_type_conversion(dw_df: DataFrame) -> None:
    """Converting StudentID data type to enable merging with sheets IDs"""
    dw_df["Application_ID"] = dw_df["Application_ID"].astype("Int64", copy=False)

def _rename_dw_df_columns(dw_df: DataFrame) -> None:
    """Renaming columns from DW to match column names on spreadsheet"""
    dw_df.rename(
        columns={
            "Application_ID": "App ID"
            , "Student_FullName": "Student Full Name"
            , "SIS_Student_ID": "PowerSchool ID"
            , "Student_Birth_Date": "DOB"
            , "StudentAddress": "Home Address"
            , "Current_School": "Previous School"
            , "Current_Grade": "Grade"
            , "Application_Status": "Current Status"
            , "Waitlist_Number": "Current Waitlist #"
        },
        inplace=True
    )


def _intervention_monitor_ids_to_df(wks: Worksheet) -> DataFrame:
    """Converts Intervention Monitor worksheet to DataFrame"""
    return wks.get_as_df(start="A2", end=(wks.rows, 1), include_tailing_empty_rows=False)

def _get_first_empty_cell(df: DataFrame) -> str:
    # Index starts at 1 + 2 header rows = 3
    row_number = len(df.index) + 3
    return f'A{row_number}'

##(3) Joining GS and DW data
def _get_data_refresh_from_warehouse(wk_df: DataFrame, dw_df: DataFrame) -> DataFrame:
    """wk_df is a DF of just student ids from a school's intervention monitor
    worksheet. dw_df is a DF from DW data filtered on school id/name"""
    result = wk_df.merge(
        dw_df,
        how="inner",
        on="App ID")
    result.drop(["ID"], axis=1, inplace=True)
    return result

#####################################################################
#                                                                   #
#                 Load: Refresh and update spreadhseet              #
#                                                                   #
#####################################################################  

##Refreshing fields
def _truncate_and_reload_monitor(df: DataFrame, wks: Worksheet) -> None:
    try:
        wks.set_dataframe(df, "A3", copy_head=False)
    except Exception as e:
        print("Error Here2")

def _get_students_to_append(dw_df: DataFrame, wk_df: DataFrame) -> DataFrame:
    result = merge(
        dw_df,
        wk_df,
        indicator=True,
        how="outer",
        on=["App ID"]).query('_merge=="left_only"')
    result.drop(["ID", "_merge"], axis=1, inplace=True)
    return result

def _append_new_students_to_intervention_monitor(df: DataFrame, wks: Worksheet, start: str) -> None:
    wks.set_dataframe(df, start, copy_head=False, extend=True)

def _get_first_empty_cell(df: DataFrame) -> str:
    # Index starts at 1 + 2 header rows = 3
    row_number = len(df.index) + 3
    return f'A{row_number}'

def _check_for_new_students_and_append() -> None:
    new_students = _get_students_to_append(DataWarehouse_Dataframe, GoogleSheet_Dataframe)
    EmptyCell1 = _get_first_empty_cell(GoogleSheet_Dataframe)
    if not new_students.empty:
        _append_new_students_to_intervention_monitor(new_students,
                                                     Worksheet, EmptyCell1)



#####################################################################
#                                                                   #
#                             Run                                   #
#                                                                   #
#####################################################################
    


#dw_df1 = pd.read_csv('Prize-AppData2025.csv')
#DataWarehouse_Dataframe = _format_dw_query(dw_df1)
DataWarehouse_Dataframe = _format_dw_query(_data_warehouse_query())


prototype_sheet_key = '1eZ4sr06syE1uyhnLD9IoJ-kjbNL7fvX6NGMy-6AZgWc'
Worksheet = _connect_to_gsheet(prototype_sheet_key,"Waitlist and Registration Management")
GoogleSheet_Dataframe = _intervention_monitor_ids_to_df(Worksheet)

#Join dataframes
df_join = _get_data_refresh_from_warehouse(GoogleSheet_Dataframe,DataWarehouse_Dataframe)

#Refresh sheet
_truncate_and_reload_monitor(df_join,Worksheet)       
#Append new apps to shet
_check_for_new_students_and_append()

