from typing import Union, List

from pandas import DataFrame, merge
from pygsheets import  authorize, Worksheet
from sqlsorcery import MSSQL

from utils.school_dataclass import SchoolDataClass


# Extract: Query data from Data Warehouse and Gsheets

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
    
# Transformations: Process data into formatted dataframes


def _format_dw_query(df: DataFrame) -> DataFrame:
    _dw_df_type_conversion(df)
    _rename_dw_df_columns(df)
    df.fillna('', inplace=True)
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
            , "Waitlist_Number": "Current WL #"
            , "Last_Status_Change": "Last Updated"
            , "Days_in_Current_Status": "Days in Current Status"
            , "Age_of_Reg_Pipeline": "Age of Reg Pipeline"
            , 'other_app': 'Other KIPP Application(s)?'
        },
        inplace=True
    )

def _intervention_monitor_ids_to_df(wks: Worksheet) -> DataFrame:
    """Converts Intervention Monitor worksheet to DataFrame"""
    return wks.get_as_df(start="A4", end=(wks.rows, 1), include_tailing_empty_rows=False)

def _get_first_empty_cell(df: DataFrame) -> str:
    # Index starts at 1 + 2 header rows = 3
    row_number = len(df.index) + 5
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


# Load: Refresh and update spreadhseet             

##Refreshing fields
def _truncate_and_reload_monitor(df: DataFrame, wks: Worksheet) -> None:
    try:
        wks.set_dataframe(df, "A5", copy_head=False)
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


def _check_for_new_students_and_append(school: SchoolDataClass) -> None:
    new_students = _get_students_to_append(school.data_warehouse_df, school.google_sheets_df)
    if not new_students.empty:
        _append_new_students_to_intervention_monitor(new_students,
                                                     school.google_sheets_obj, school.first_empty_cell)
        print(f'Appended {len(new_students)} onto {school.school_name} Student Conversion Tracker')
        
def _refresh_intervention_monitor_data(school: SchoolDataClass) -> None:
    """Refreshes student records in worksheet

    If data_refresh_df is empty, then there are no records in worksheet and is assumed to be new.
    New sheets will be checked for range protection"""
    data_refresh_df = _get_data_refresh_from_warehouse(school.google_sheets_df, school.data_warehouse_df)
    if not data_refresh_df.empty:
        _truncate_and_reload_monitor(data_refresh_df, school.google_sheets_obj)
        print(f'Refreshing {len(data_refresh_df)} student records on {school.school_name} Student Conversion Tracker')
    else:
        #_protect_ids(school.google_sheets_obj)
        pass


    



def _sort_intervention_monitor(school: SchoolDataClass) -> None:
    """Sort intervention monitor last name then by  enrollment status with un-enrolled students at bottom"""
    sheet_dim = (school.google_sheets_obj.rows, school.google_sheets_obj.cols)
    school.google_sheets_obj.sort_range('A5', sheet_dim, basecolumnindex=1, sortorder='ASCENDING')
    school.google_sheets_obj.sort_range('A5', sheet_dim, basecolumnindex=8, sortorder='DESCENDING')
    school.google_sheets_obj.sort_range('A5', sheet_dim, basecolumnindex=7, sortorder='ASCENDING')


def refresh_intervention_monitors(schools: List[SchoolDataClass]) -> None:
    dw_df = _data_warehouse_query()
    dw_df = _format_dw_query(dw_df)

    for school in schools:
        print(f'\n--- PROCESSING {school.school_name} ---')

        school.google_sheets_obj = _connect_to_gsheet(school.sheets_key, "Waitlist + Registration Tracker")
        print(f"Connected to {school.school_name}'s google sheet")
        if school.google_sheets_obj is not None:
            school.google_sheets_df = _intervention_monitor_ids_to_df(school.google_sheets_obj)
            school.first_empty_cell = _get_first_empty_cell(school.google_sheets_df)
            school.data_warehouse_df = dw_df.loc[dw_df["ID"].isin([school.school_id])]

            _refresh_intervention_monitor_data(school)
            _check_for_new_students_and_append(school)
            _sort_intervention_monitor(school)