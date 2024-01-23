from utils.school_dataclass import SchoolDataClass
from utils.school_dataclass import create_dataclass
from utils.school_meta_data import SCHOOL_META_DATA
from typing import List

def get_schools() -> List[SchoolDataClass]:
    """Fetches and filters list of SchoolDataClass objects

    Func will fetch based on whether --schools or --exclude arguments are present
    """
    school_dataclasses = [create_dataclass(x) for x in SCHOOL_META_DATA if x['status'] == 'ACTIVE']
    if args.schools:
        return [school for school in school_dataclasses if school.short_name.upper() in args.schools]
    elif args.exclude:
        return [school for school in school_dataclasses if school.short_name.upper() not in args.exclude]
    else:
        return school_dataclasses