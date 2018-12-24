# Create python virtual enviornment

You must first set up your virtual environment. Any virtual enviornment will work, but my preference is virtualenv. 

### Install virtualenv
```bash
$ sudo apt-get install python-virtualenv
```

### Create virtual environment
```bash
$ virtualenv -p python3 env
$ source ./env/bin/activate
$ pip install -r requirements.txt
```

# Update configurations

The `kb-geonames.py` script has two dict configuration objects within the `main` method; `es_cfg` and `gn_cfg`. The `es_cfg` represents all of the elasticsearch configutions and the `gn_cfg` represents all of the geonames configurations. Open the python script and ensure these configurations are correct before running the script below. 

Most of the time these configurations will remain unchanged. The important configurations to inspect are described below

### Elasticsearch configurations

`input_index` : The elasticsearch index to read from
`dest_index` : The elasticsearch index that will be created and populated with new geoLocation data
`host`: The elasticsearch host
`port`: The elasticsearch port
`timeout`: The elasticsearch timeout
`scroll`: The scroll size to batch process the query results
`size`: The batch procesing size
`doc_type`: The mapping for both input and destination indexes
`query`: The query that will be executed against the input index
`loc_types`:  Array of types that contain location information
`body`: Settings and mapping for destination elasticsearch index

### Geonames configurations

`user`: username to use for geonames web service
`url`: the geonames web service url
`endpoint` : the geonames endpoint that will be used

# Run Script
```bash
$ python kb-geonames.py
``` 
