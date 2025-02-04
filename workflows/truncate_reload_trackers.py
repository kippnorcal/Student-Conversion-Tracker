import logging
from typing import List

from pandas import DataFrame
from pygsheets import Worksheet

from models.school_dataclass import SchoolDataClass

# Cell to start truncate and load from
START_CELL = "A1"
DATA_SCHOOL_ID_FIELD = ""

def _truncate_and_reload_tracker(df: DataFrame, wks: Worksheet) -> None:
    try:
        wks.set_dataframe(df, START_CELL, copy_head=False)
    except Exception as e:
        logging.info(f"Error raised: {e}")
        logging.info(f"Exception type {type(e)}")


def run_truncate_and_load(schools: List[SchoolDataClass], tracker_data: DataFrame) -> None:
    for school in schools:
        logging.info(f'\n--- PROCESSING {school.school_name} ---')

        if school.google_sheets_obj is not None:
            school.data_warehouse_df = tracker_data.loc[tracker_data[DATA_SCHOOL_ID_FIELD].isin([school.school_id])]

