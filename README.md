# noaadb api
This package allows you to query the data.  
It was built with python3.6 and uses the lovely sqlalchemy api to allow for easy query serialization.

View the JSON API specification at https://www.yuvalboss.com/api/

#### structure
    .
    ├── api                    # public api stuff
    │   └── server.py          # server
    ├── schema                 # generate datasets/chips for training
    │   ├── models.py          # schema definition
    │   ├── queries.py         # utility queries
    │   ├── config.py          # database connection configurations for creation/populations (requires env variables)
    │   └── restore_db.py      # recreates all tables in the schema
    ├── setup.py               # project path/dependency setup stuff
    └── README.md

The public api includes hardcoded readonly credentials.
#### Use Python to query hotspots api endpoint
```
query_config = {
          "species_list": [
            "Polar Bear", "Bearded Seal"
          ],
          "workers": [],
          "jobs": [],
          "surveys": [],
          "flights": [],
          "camera_positions": [],
          "image_type": "eo",
          "show_shadows": False,
          "show_removed_labels": False
        }
API_ENDPOINT = "https://yuvalboss.com/api/hotspots"
headers = {'Content-type': 'application/json'}
r = requests.post(url = API_ENDPOINT, data = json.dumps(query_config), headers=headers)
if r.status_code != 200:
    raise Exception(r.text)
json_response = json.loads(r.text)
```

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