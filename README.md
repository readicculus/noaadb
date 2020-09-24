# noaadb SDK
This package allows you to query the data.  
It was built with python3.6 and uses the lovely sqlalchemy api to allow for easy query serialization.


#### structure
    .
    ├── api                      # public api stuff
    │   └── server.py            # server
    ├── schema                   # generate datasets/chips for training
    │   └── models/              # generate datasets/chips for training
    │       ├──  survey_data.py  # schema definition for survey data
    │       ├──  label_data.py   # schema definition for label data
    │       └──  ml_data.py      # schema definition for ml experiment data
    ├── setup.py                 # project path/dependency setup stuff
    └── README.md


#### Download data models
Can install models to be used with api and all associate code as follows:

Install requirements for psycopg2(python postgresql driver)
```
sudo apt install libpq-dev python3-dev
```
Install package with pip
```
pip install git+ssh://git@github.com/readicculus/noaadb.git@v0.0.1
```
Or install package from source
```
git clone git@github.com:readicculus/noaadb.git
cd noaadb/
pip install . 
```

#### How to use in a project
```
from noaadb import Session
s = Session()
s.query(...)
s.close()
```

The above will initially raise an error.
In order for the Session to connect to the database you need to set the correct environment variables.
This package uses the dotenv package so one option is to include a `.env` file in the project you are importing noaadb from.
```
# .env file contents
DB_USR=database username
DB_PWD=database password
DB_HOST=database url or localhost
DB_NAME=name of database
```
Alternatively these could be defined as system environment variables if preferred.


### Data models
`survey_data.py`: models for survey data containing image metadata and associated instrument info
`label_data.py`: label information, bounding boxes, species, and metadata about origins of the label data
`ml_data.py`: ml related information, image tags, train/test info
