import argparse
import logging
import traceback
import os
import sys
from typing import Union

from gbq_connector import BigQueryClient
from job_notifications import create_notifications
import pandas as pd
from pygsheets import authorize, Worksheet

from models.school_dataclass import create_dataclass
from models.school_dataclass import SchoolDataClass
try:
    from school_meta_data import SCHOOL_META_DATA
except ImportError:
    raise Exception("school_meta_data.py no found. See README for more info.")
from workflows.merge_update_trackers import run_merge_and_update
from workflows.truncate_reload_trackers import run_truncate_and_reload


parser = argparse.ArgumentParser()
parser.add_argument(
    "--school-year",
    help="Runs the merge and update tracker workflow; If not provided, the truncate and reload tracker workflow is run",
    dest="merge_update",
    action="store_true"
)
parser.add_argument(
    "--dbt-refresh",
    help="Runs dbt refresh job",
    dest="dbt_refresh",
    action="store_true"
)

DEBUG = int(os.getenv("DEBUG", default=0))

logging.basicConfig(
    handlers=[
        logging.FileHandler(filename="app.log", mode="w+"),
        logging.StreamHandler(sys.stdout),
    ],
    level=logging.DEBUG if DEBUG else logging.INFO,
    format="%(asctime)s | %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %I:%M:%S%p %Z",
)

logger = logging.getLogger(__name__)
args = parser.parse_args()
SHEET_NAME = "Waitlist + Registration Tracker"

# Create notifications object
notifications = create_notifications("Student Conversion Tracker", "mailgun")

def get_data_snapshot():
    gbq = BigQueryClient()
    df = gbq.get_table_as_df("model_name", dataset="norcal_analytics")
    return df


def _prep_dataset(df: pd.DataFrame) -> pd.DataFrame:
    _rename_dw_df_columns(df)
    df.fillna('', inplace=True)
    return df


def _rename_dw_df_columns(dw_df: pd.DataFrame) -> None:
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


def _connect_to_gsheet(school: SchoolDataClass, sheet_name: str) -> Union[Worksheet, None]:
    gsheet = None
    try:
        client = authorize(service_file=os.getenv("SERVICE_ACCOUNT_CREDENTIAL_FILE"))
        sheet = client.open_by_key(school.sheets_key)
        gsheet = sheet.worksheet_by_title(sheet_name)
        logging.info(f"Connected to {school.school_name}")
    except Exception as e:
        logger.info(f"Error raised connecting to {school.school_name}: {e}")
        logger.info(f"Exception type {type(e)}")
    return gsheet


def main():
    schools = [create_dataclass(x) for x in SCHOOL_META_DATA if x['status'] == 'ACTIVE']
    if args.dbt_refresh:
        logger.info("refresh-dbt no implemented")
    tracker_dataset = get_data_snapshot()
    tracker_dataset = _prep_dataset(tracker_dataset)

    if not schools:
        logger.info("No schools marked ACTIVE. Check school_meta_data.py")
    else:
        for school in schools:
            school.google_sheets_obj = _connect_to_gsheet(school, SHEET_NAME)
        if args.merge_update:
            logger.info("Running merge and update trackers workflow")
            run_merge_and_update(schools, tracker_dataset)
        else:
            logger.info("Running truncate and reload trackers workflow")
            run_truncate_and_reload(schools, tracker_dataset)


if __name__ == "__main__":
    try:
        main()
        notifications.notify()
    except Exception as e:
        logging.exception(e)
        error_message = traceback.format_exc()
        notifications.notify(error_message=error_message)