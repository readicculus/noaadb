# noaadb api
This package allows you to query the data.  
It was built with python3.6 and uses the lovely sqlalchemy api to allow for easy query serialization.

### Example usage:
Open a new session:
```
api = LabelDBApi()
api.begin_session()
```

When done close the session:
```
api.close_session()
```

Here is an example of how you would get all hotspots:
```
api = LabelDBApi()
api.begin_session()
all_hotspots = api.get_hotspots()
api.close_session()
```

Suppose you only wanted only hotspots your call would look like this:
`api.get_hotspots(species_filter=('Ringed Seal', 'Bearded Seal', 'UNK Seal'))`

I will add more capabilities to this on an as needed base but you can always do the following to create your own sqlalchemy queries:
```
api = LabelDBApi()
Session = api.get_session_config()
session = Session() # begin the session

... custom queries/filters etc..

session.close()     # end the session
```