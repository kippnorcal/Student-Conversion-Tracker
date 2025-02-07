# Student Matriculation Conversion Tracker
This repo is a data source pipeline for a tracker in Google Sheets that displays enrollment data for stakeholders. The tracker is built to load data into a Google sheet two different ways:
1. Truncate and Reload: This is the default behavior. This repo will truncate and reload data in a specific sheet in a Google Workbook
2. Merge and Update: This method has a runtime argument `--merge-update`. This method is good for when columns to the right of the data are being used by staff for capturing notes. It updates data in a Google sheet in two steps:
   * Using a unique ID field, the repo identifies records in the incoming dataset that already exist in the Google sheet and updates those records
   * Using the same ID field, it identifies records in the incoming dataset that do not exist in the Google sheet and merges them into the sheet

## Getting Started

### Dependencies:
* Python3
* [Pipenv](https://pipenv.readthedocs.io/en/latest/)
* [Docker](https://www.docker.com/)


### Clone the repo
https://github.com/kippnorcal/Student-Conversion-Tracker.git


### Create .env file with project secrets

```
# Google Service Account Credentials
SERVICE_ACCOUNT_CREDENTIAL_FILE=

# Google Big Query Info
GOOGLE_APPLICATION_CREDENTIALS=
GBQ_PROJECT=
GBQ_DATASET=

# Mailgun variables
MG_API_KEY=
MG_API_URL=
MG_DOMAIN=
FROM_ADDRESS=
TO_ADDRESS=
```

### The `school_meta_data.py` module
Create a file at the repo's root called `school_meta_data.py`. In this file, create a list named `SCHOOL_META_DATA`. This fill this list with dictionaries that represent data of each school. Each dictionary needs to contain the following keys:
```python
SCHOOL_META_DATA = [
    {
        "school_name": "Some School", # Name of school for logging purposes
        "school_id": 1, # School ID of record
        "sheets_key": "ABCD123", # Google Sheets key for school's attendance tracking worksheet
        "active": True # For beginning of school year as schools come online. False will be filtered out.
    }
]
```

### Module Constants
Some of the modules have constants at the top. A lot of these constants are cell references for the Google sheets that are being loaded to. Adjust these constants as needed for your use case.

### Create a service account for Google API access
#### Enable APIs in Developer Console
* Navigate to the API library in the developer console.
* Create a new project to use with this project and give the project a name.
* Search for Admin SDK API, and Enable it.

#### Create a service account.
* In the Google Developer Console (console.developers.google.com), go to Credentials.
* Click on "Create Credentials -> Service Account"
* Create a name for your service account.
* Select the "Owner" role for the service account.
* Create a key, saving the result file as service_account_credentials.json. Move this file to the project directory.
* In Service Accounts, click on the service account you just created.
* Scroll to the bottom of the page, click "Show Domain Wide Delegation", and check the box for "Enable Google Workspace Domain-Wide Delegation" and click "Save".


## Build docker image
```
docker build -t student-conversion-tracker .
```

## Run the code

To run the code as a truncate and load:
```
docker run --rm -t student-conversion-tracker
```

Here is an example with the `--merge-update` argument
```
docker run --rm -t student-conversion-tracker
```