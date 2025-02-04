import logging
import os
from typing import Union, List

import pandas as pd
from pandas import DataFrame, merge
from pygsheets import  authorize, Worksheet

from models.school_dataclass import SchoolDataClass

# These constants are data points that the script is dependent on and are tied to how the Google Sheets are set up
ID_FIELD = "App ID"
DATA_SCHOOL_ID_FIELD = "ID"
SHEET_NAME = "Waitlist + Registration Tracker"
START_CELL = "A5"
START_CELL_WITH_HEADERS = "A4"
SHEET_ROW_OFFSET = 5 # Number of rows until where the data begins (including headers)

def _connect_to_gsheet(spreadsheet_id: str, sheet_name: str) -> Union[Worksheet, None]:
    gsheet = None
    try:
        client = authorize(service_file=os.getenv("SERVICE_ACCOUNT_CREDENTIAL_FILE"))
        sheet = client.open_by_key(spreadsheet_id)
        gsheet = sheet.worksheet_by_title(sheet_name)
    except Exception as e:
        logging.info(f"Error raised: {e}")
        logging.info(f"Exception type {type(e)}")
    return gsheet


def _tracker_student_ids_to_df(wks: Worksheet) -> DataFrame:
    """Gets student IDs from Google Sheet as a DataFrame. Used for updating existing records in the sheet"""
    return wks.get_as_df(start=START_CELL_WITH_HEADERS, end=(wks.rows, 1), include_tailing_empty_rows=False)


def _get_first_empty_cell(df: DataFrame) -> str:
    row_number = len(df.index) + SHEET_ROW_OFFSET
    logging.info(f'Marking cell A{row_number} as first blank cell.')
    return f'A{row_number}'


def _get_data_refresh_from_warehouse(wk_df: DataFrame, dw_df: DataFrame) -> DataFrame:
    """
    wk_df is a DF of just student ids from a school's tracker worksheet.
    dw_df is a DF from DW data filtered on school id
    """
    result = wk_df.merge(
        dw_df,
        how="inner",
        on=ID_FIELD)
    result.drop([DATA_SCHOOL_ID_FIELD], axis=1, inplace=True)
    return result


# Refreshing fields
def _truncate_and_reload_tracker(df: DataFrame, wks: Worksheet) -> None:
    try:
        wks.set_dataframe(df, START_CELL, copy_head=False)
    except Exception as e:
        logging.info(f"Error raised: {e}")
        logging.info(f"Exception type {type(e)}")


def _get_students_to_append(dw_df: DataFrame, wk_df: DataFrame) -> DataFrame:
    result = merge(
        dw_df,
        wk_df,
        indicator=True,
        how="outer",
        on=[ID_FIELD]).query('_merge=="left_only"')
    result.drop([DATA_SCHOOL_ID_FIELD, "_merge"], axis=1, inplace=True)
    return result


def _append_new_students_to_conversion_tracker(df: DataFrame, wks: Worksheet, start: str) -> None:
    wks.set_dataframe(df, start, copy_head=False, extend=True)


def _check_for_new_students_and_append(school: SchoolDataClass) -> None:
    new_students = _get_students_to_append(school.data_warehouse_df, school.google_sheets_df)
    if not new_students.empty:
        _append_new_students_to_conversion_tracker(new_students, school.google_sheets_obj, school.first_empty_cell)
        logging.info(f"Inserted {len(new_students)} new students onto the tracker")


def _refresh_conversion_tracker_data(school: SchoolDataClass) -> None:
    data_refresh_df = _get_data_refresh_from_warehouse(school.google_sheets_df, school.data_warehouse_df)
    if not data_refresh_df.empty:
        _truncate_and_reload_tracker(data_refresh_df, school.google_sheets_obj)
        logging.info(f"Refreshing {len(data_refresh_df)} student records on {school.school_name} Conversion Tracker")
    else:
        logging.info(f"No student records to refresh on {school.school_name} Conversion Tracker")



def _sort_conversion_tracker(school: SchoolDataClass) -> None:
    """Sort conversion tracker by App ID, Age of Reg Pipeline, then Days in Current Status"""
    sheet_dim = (school.google_sheets_obj.rows, school.google_sheets_obj.cols)
    school.google_sheets_obj.sort_range(START_CELL, sheet_dim, basecolumnindex=1, sortorder="ASCENDING")
    school.google_sheets_obj.sort_range(START_CELL, sheet_dim, basecolumnindex=8, sortorder="DESCENDING")
    school.google_sheets_obj.sort_range(START_CELL, sheet_dim, basecolumnindex=7, sortorder="ASCENDING")


def refresh_conversion_trackers(schools: List[SchoolDataClass], tracker_data: pd.DataFrame) -> None:

    for school in schools:
        logging.info(f'\n--- PROCESSING {school.school_name} ---')

        school.google_sheets_obj = _connect_to_gsheet(school.sheets_key, SHEET_NAME)

        logging.info(f"\nConnected to {school.school_name}'s google sheet")
        if school.google_sheets_obj is not None:
            school.google_sheets_df = _tracker_student_ids_to_df(school.google_sheets_obj)
            school.first_empty_cell = _get_first_empty_cell(school.google_sheets_df)
            school.data_warehouse_df = tracker_data.loc[tracker_data[DATA_SCHOOL_ID_FIELD].isin([school.school_id])]

            _refresh_conversion_tracker_data(school)
            _check_for_new_students_and_append(school)
            _sort_conversion_tracker(school)
