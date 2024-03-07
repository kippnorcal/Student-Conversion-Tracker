import logging
import traceback

from job_notifications import create_notifications

from etl_script import refresh_conversion_trackers
from utils.school_meta_data import SCHOOL_META_DATA
from utils.school_dataclass import create_dataclass

# Create Logger
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

# Create notifications object
notifications = create_notifications("Student Conversion Tracker", "mailgun")

def main():
    schools = [create_dataclass(x) for x in SCHOOL_META_DATA if x['status'] == 'ACTIVE']
    
    if not schools:
        print("No schools selected for job")
    else:
        refresh_conversion_trackers(schools)

if __name__ == "__main__":
    try:
        main()
        notifications.notify()
    except Exception as e:
        logging.exception(e)
        error_message = traceback.format_exc()
        notifications.notify(error_message=error_message)