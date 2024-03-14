# student_conversion_tracker
ETL pipeline that connects data warehouse and GoogleSheets for Student Conversion tracking. The pipeline has one job:
* Conversion Tracker Refresh - Pulls latest enrollment data snapshot and refreshes the "Waitlist + Registration Tracker" tab in the Student Conversion Tracker tool (for all of our schools)

## Getting Started

### Dependencies:
* Python3.8
* [Pipenv](https://pipenv.readthedocs.io/en/latest/)
* [Docker](https://www.docker.com/)


### Clone the repo
https://github.com/kippnorcal/Student-Conversion-Tracker.git


### Create .env file with project secrets

```
# Database connection
DB_SERVER=
DB=
DB_USER=
DB_PWD=
DB_SCHEMA=

# Google Developer Service Account
ACCOUNT_EMAIL=

# Mailgun variables
MG_API_KEY=
MG_API_URL=
MG_DOMAIN=

# Email Notification Variables
SENDER_EMAIL=
RECIPIENT_EMAIL=
```

### Create school_meta_data.py
A list of dictionaries that represent each school. List needs to be named SCHOOL_META_DATA. Each dictionary needs to contain the following key, value pairs:
```
school_name = String: School name from data warehouse for use in much of the Python code
school_id = Int: School ID of record
short_name = String: Used for runtime arguments. Do not use spaces in names.
sheets_key = String: Google Sheets key for school's attendance tracking worksheet
status = String: For beginning of school year as schools come online. Unless the value is 'ACTIVE', school will be 
    filtered out.
```

The sheets_key is the same keys used for the as attendance_response_letters job.


### Create a service account for Google API access
Use the same credentials as the attendance_response_letters job.


## Build docker image
```
docker build -t student_conversion_tracker .
```
If running locally on a Mac with M1 chip, add `--platform linux/amd46`.

```
docker build -t student_conversion_tracker . --platform linux/amd64
```

## Run the code
For all jobs and all schools:
```
docker run --rm -t student_conversion_tracker
```