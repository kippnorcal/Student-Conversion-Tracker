from dataclasses import dataclass

from pandas import DataFrame
from pygsheets import Worksheet

"""
Attributes school_id, school_name, short_name, sheets_key, status will map to SCHOOL_META_DATA

- google_sheets_obj - pygsheets.Worksheet object
- first_empty_cell - Used to track first empty cell in colum A for appending
- google_sheets_df - DataFrame of google_sheets_obj
- data_warehouse_df - DataFrame of school data from DW
- count_of_no_show_students - Each year, a few students enroll but never attend. These students get dropped from the DW. 
    This counts how many students to remove from Google Sheets. 
"""


@dataclass
class SchoolDataClass:
    school_id: int
    school_name: str
    short_name: str
    sheets_key: str
    google_sheets_obj: Worksheet = None
    first_empty_cell = None
    google_sheets_df: DataFrame = None
    data_warehouse_df: DataFrame = None
    status: str = None
    count_of_no_show_students: int = None

    def __str__(self):
        return f'{self.school_name} - {self.sheets_key}'


def create_dataclass(data) -> SchoolDataClass:
    return SchoolDataClass(**data)
