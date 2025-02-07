import logging
from typing import List

from pandas import DataFrame
from pygsheets import Worksheet

from models.school_dataclass import SchoolDataClass

# Cell to start truncate and load from
START_CELL = (3, 1)
END_COL = 13
DATA_SCHOOL_ID_FIELD = "school_id"

def _truncate_and_reload_tracker(df: DataFrame, wks: Worksheet) -> None:
    try:
        wks.clear(START_CELL, end=(wks.rows, END_COL))
        wks.set_dataframe(df, START_CELL, copy_head=False)
        logging.info(f"Truncated and loaded {len(df)} records into conversion tracker")
    except Exception as e:
        logging.info(f"Error raised: {e}")
        logging.info(f"Exception type {type(e)}")


def run_truncate_and_reload(schools: List[SchoolDataClass], tracker_data: DataFrame) -> None:
    for school in schools:
        if school.google_sheets_obj is not None:
            logging.info(f'--- PROCESSING {school.school_name} ---')
            school.data_warehouse_df = tracker_data[tracker_data[DATA_SCHOOL_ID_FIELD] == school.school_id]
            school.data_warehouse_df = school.data_warehouse_df.drop([DATA_SCHOOL_ID_FIELD], axis="columns")
            if not school.data_warehouse_df.empty:
                _truncate_and_reload_tracker(school.data_warehouse_df, school.google_sheets_obj)
            else:
                logging.info(f"No data to load for {school.school_name}")
