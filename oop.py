from pandas import DataFrame
from pygsheets import Worksheet

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




SCHOOL_META_DATA = [
    {
        "school_name": "KIPP Prize",
        "school_id": 14,
        "short_name": "Prize",
        "sheets_key": "1eZ4sr06syE1uyhnLD9IoJ-kjbNL7fvX6NGMy-6AZgWcs",
        "status": "ACTIVE"
    },
    {
         "school_name": "KIPP Bayview Elementary",
         "school_id": 3,
         "short_name": "BayviewES",
         "sheets_key": "1sO6WIzndtq8ivaNobm4sI4IXXd8xzmcP-HNkABimPVw",
         "status": "ACTIVE"
     }
]

#Prize = SchoolData(SCHOOL_META_DATA[0]["school_id"],SCHOOL_META_DATA[0]["sheets_key"])
#print(Prize.school_id)


school_dataclasses = [create_dataclass(x) for x in SCHOOL_META_DATA if x['status'] == 'ACTIVE']