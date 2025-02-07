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
    "--merge-update",
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
SHEET_NAME = "[DSIM DQ Check] New Students to Add"

# Create notifications object
notifications = create_notifications("Student Conversion Tracker", "mailgun", logs="app.log")

def get_data_snapshot():
    gbq = BigQueryClient()
    df = gbq.get_table_as_df("rpt_enrollment__enrollment_management_tool", dataset="norcal_analytics")
    return df


def _prep_dataset(df: pd.DataFrame) -> pd.DataFrame:
    _rename_dw_df_columns(df)
    df["Last Updated"] = df["Last Updated"].dt.strftime("%m/%d/%Y")
    obj_cols = df.select_dtypes(object).columns
    df[obj_cols] = df[obj_cols].fillna("")
    df = df.astype(str)
    df = df.fillna('')
    df["school_id"] = df["school_id"].astype(int)
    return df


def _rename_dw_df_columns(dw_df: pd.DataFrame) -> None:
    """Renaming columns from DW to match column names on spreadsheet"""
    dw_df.rename(
        columns = {
            "app_id": "App ID",
            "student_full_name": "Student Full Name",
            "powerschool_id": "PowerSchool ID",
            "dob": "DOB",
            "home_address": "Home Address",
            "previous_school": "Previous School",
            "sibling": "Sibling",
            "other_kipp_applications": "Other KIPP Application(s)?",
            "grade": "Grade",
            "current_status": "Current Status",
            "current_waitlist_number": "Current WL #",
            "last_updated": "Last Updated",
            "days_in_current_status": "Days in Current Status",
            "school_id": "school_id", # This id is for filtering df; will get dropped before adding to sheet
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