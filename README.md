# noaadb api
This package allows you to query the data.  
It was built with python3.6 and uses the lovely sqlalchemy api to allow for easy query serialization.

To install:
```
git clone git@github.com:readicculus/noaadb.git
cd noaadb/
pip install . 
```
#### structure
    .
    ├── api                    # public readonly api
    │   └── api.py             # api stuff
    ├── schema                 # generate datasets/chips for training
    │   ├── models.py          # schema definition
    │   ├── queries.py         # utility queries
    │   ├── config.py          # database connection configurations for creation/populations (requires env variables)
    │   └── restore_db.py      # recreates all tables in the schema
    ├── setup.py               # project path/dependency setup stuff
    └── README.md

The public api includes hardcoded readonly credentials.

#### Example usage:
Open a new session:
```python
from noaadb.api import LabelDBApi
api = LabelDBApi()
api.begin_session()
api.close_session() # when done close the session
```

Here is an example of how you would get all hotspots:
```python
from noaadb.api import LabelDBApi
api = LabelDBApi()
api.begin_session()
all_hotspots = api.get_hotspots()
api.close_session()
```

Suppose you only wanted only hotspots your call would look like this:
`api.get_hotspots(species_filter=('Ringed Seal', 'Bearded Seal', 'UNK Seal'))`

I will add more capabilities to this on an as needed base but you can always do the following to create your own sqlalchemy queries:
```python
from noaadb.api import LabelDBApi
api = LabelDBApi()
Session = api.get_session_config()
session = Session() # begin the session

... custom queries/filters etc..

session.close()     # end the session
```
