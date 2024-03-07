
from job_notifications import create_notifications

from etl_script import refresh_intervention_monitors
from utils.school_meta_data import SCHOOL_META_DATA
from utils.school_dataclass import create_dataclass

# Create notifications object
notifications = create_notifications("Student Conversion Tracker", "mailgun")

def main():
    schools = [create_dataclass(x) for x in SCHOOL_META_DATA if x['status'] == 'ACTIVE']
    
    if not schools:
        print("No schools selected for job")
    else:
        refresh_intervention_monitors(schools)

if __name__ == "__main__":
    try:
        main()
        notifications.notify()
    except Exception as e:
        print("error running main")