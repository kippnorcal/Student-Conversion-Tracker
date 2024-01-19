import pandas as pd

from sqlsorcery import MSSQL
from pandas import DataFrame, to_numeric,merge
from pygsheets import Address, authorize, DataRange, GridRange, Worksheet
from typing import Union, List

#######

# Extract

######




#Extracting DW data
def _get_attendance_response_view_df() -> DataFrame:
    """Extracting view from DW as DF"""
    return MSSQL().query_from_file("./sql/enrollment.sql")


#Extracting GS data 
def _create_sheet_connection(sheet_key: str, worksheet_name: str) -> Union[Worksheet, None]:

    worksheet = None
    try:
        client = authorize(service_file='./service_account_credentials.json')
        sheet = client.open_by_key(sheet_key)
        worksheet = sheet.worksheet_by_title(worksheet_name)
    except Exception as e:
        print('Error Here')
    return worksheet






####

# Process Data

####

#(1) Process DW data

def _format_attendance_response_df(df: DataFrame) -> DataFrame:
    _dw_df_type_conversion(df)
    ##df.fillna("", inplace=True)
    _rename_dw_df_columns(df)
    return df
    

def _dw_df_type_conversion(dw_df: DataFrame) -> None:
    """Converting StudentID data type to enable merging with sheets IDs"""
    dw_df["Application_ID"] = dw_df["Application_ID"].astype("Int64", copy=False)

def _rename_dw_df_columns(dw_df: DataFrame) -> None:
    """Renaming columns from DW to match column names from intervention monitor"""
    dw_df.rename(
        columns={
            "Application_ID": "App ID"
            , 'Student_Last_Name': "Last Name"
            , "Student_First_Name": "First Name"
            , "Full_Name": "[Student Full Name]"
            , "Student_Birth_Date": "DOB"
            , "Current_School": "Previous School"
            , "Current_Grade": "Grade"
            , "Application_Status": "Current Status"
            , "Waitlist_Number": "Current Waitlist #"
        },
        inplace=True
    )


    

#(2) Process GS Data
prototype_sheet_key = '1eZ4sr06syE1uyhnLD9IoJ-kjbNL7fvX6NGMy-6AZgWc'


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
    return result

####
# Loading
###   
def _truncate_and_reload_monitor(df: DataFrame, wks: Worksheet) -> None:
    try:
        wks.set_dataframe(df, "A3", copy_head=False)
    except Exception as e:
        print("Error Here")



####
# Testing
###
    


dw_df1 = pd.read_csv('mock-df.csv')
dw_df2 = _format_attendance_response_df(dw_df1)
#print("DW Dataframe:")




Worksheet = _create_sheet_connection(prototype_sheet_key,"Waitlist Mangement")
gs_dw = _intervention_monitor_ids_to_df(Worksheet)
#print("GS Dataframe")
#print(gs_dw)

df_join = _get_data_refresh_from_warehouse(gs_dw,dw_df2)

def _truncate_and_reload_monitor(df: DataFrame, wks: Worksheet) -> None:
    try:
        wks.set_dataframe(df, "A3", copy_head=False)
    except Exception as e:
        print("Error Here")

_truncate_and_reload_monitor(df_join,Worksheet)       