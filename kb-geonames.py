from elasticsearch2 import Elasticsearch
import json
import requests
import pygeohash as gh

def process_results(hits, es_cfg, gn_cfg):
    """
    Add geolocation and query geonames for current scroll
    results

    :param hits: results returned from elasticsearch query
    :param es_cfg: elasticsearch configurations
    :param gn_cfg: geonames configurations

    :return json_str, count: bulk json reocrds, the number of geolocations extracted
    """
    json_str = ""
    count = 0;

    if( len(hits) > 0 ):
        print("Processing batch of " + str(len(hits)) + " results")

    #iterate over all results from query
    for item in hits:

        #create bulk record header
        json_str += '{"index":{"_index":"' + es_cfg['dest_index'] + '", "_type":"' + es_cfg['doc_type'] + '"}}\n'

        #check if types list intersects with location array
        if bool(set(es_cfg['loc_types']) & set(item['_source']['types'])):

            name = item['_source']['name']

            #verify there is a value to query geonames with
            if name is not None:

                # get source data and add geo info
                lat, lon = geonames_lookup(name, gn_cfg)
    
                # add new geolocation to the dict
                geolocation = build_geolocation(lat, lon)

                if geolocation:
                    print(' name[' + name + '] returned (' + lat + ',' + lon + ')')
                    item['_source']['geolocation'] = geolocation
                    count += 1
                else:
                    print(' name[' + name + '] returned no results')

                #build bulk json record string
                json_str += ''.join(json.dumps(item['_source']))
                json_str += '\n'
        else:
            #add empty geolocation and build bulk json record string
            item['_source']['geolocation'] = {}
            json_str += ''.join(json.dumps(item['_source']))
            json_str += '\n'

    return json_str, count
 
def geonames_lookup(value, gn_cfg):
    """
    Creates geonames request with a max return of one single result

    :param value: the value to query qeonames with
    :param gn_cfg: geonames configurations

    :return lat,lon: returns the extract lat and lon values
    """
    #build the request
    resp = requests.get(gn_cfg['url'] + gn_cfg['endpoint'] + '?q='+ value + '&maxRows=1&username=' + gn_cfg['user'])

    if resp.status_code == 200:
        data = resp.json()
        if len(data['geonames']) > 0:
            return data['geonames'][0]['lat'], data['geonames'][0]['lng']
    
    return None, None

def build_geolocation(lat, lon):
    """
    Converts lat/lon to geohash and builds geolocation dict

    :param lat: latitude
    :param lon: longitude

    :return dict: the dict json representation of a geolocation
    """
    geolocation = {}

    if lat and lon:
        geohash = gh.encode(float(lat), float(lon), 8)
        geolocation = {'geohash':geohash,'lat':lat,'lon':lon}

    return geolocation

def process_input_index(es, es_cfg, gn_cfg):
    """
    Process input elasticsearch index for geolocation data

    :param es: the elasticsearch connection
    :param es_cfg: elasticsearch configurations
    :param gn_cfg: geonames configurations
    """
    total = 0

    if not es:
        print('ERROR: empty elasticsearch connection, exiting')
        exit()

    # verify if index exists
    if not es.indices.exists(index=es_cfg['input_index']):
        print('ERROR: index ' + es_cfg['input_index'] + ' does not exist at http://' + str(es_cfg['host']) + ':' + str(es_cfg['port']))
        exit()

    # execute query
    data = es.search(
        index=es_cfg['input_index'],
        scroll=es_cfg['scroll'],
        size=es_cfg['size'],
        body=es_cfg['query']
    )

    # get scroll id and size
    sid = data['_scroll_id']
    scroll_size = len(data['hits']['hits'])

    # buld intial batch of bulk json data
    records, count = process_results(data['hits']['hits'], es_cfg, gn_cfg)
    total += count

    if records:
        insert_bulk_es_records(es, es_cfg['dest_index'], es_cfg['doc_type'], records) 

    while scroll_size > 0:

        data = es.scroll(scroll_id=sid, scroll=es_cfg['scroll'])

        # buld next batch of bulk json data
        records, count = process_results(data['hits']['hits'], es_cfg, gn_cfg)
        total += count

        if records:
            insert_bulk_es_records(es, es_cfg['dest_index'], es_cfg['doc_type'], records)

        # update scroll id and size
        sid = data['_scroll_id']
        scroll_size = len(data['hits']['hits'])

    print('Successfully found and extracted ' + str(total) + ' geolocations')

def create_dest_index(es, index, body):
    """
    Creates new elasticsearch index

    :param es: elasticsearch connection
    :param idnex: index to create
    :param body: settings and mapping data for index creation
    """
    #verify index does not already exist
    if es.indices.exists(index=index):
        print("ERROR: Attempted to create index " + index + " that already exists")
        exit()

    es.indices.create(index=index, body=body)

def insert_bulk_es_records(es, index, doc_type, records):
    """
    Insert bulk json record into elasticsearch

    :param es: elasticsearch connection
    :param index: index to insert bulk json into
    :param records: bulk json records
    """
    es.bulk(index=index, doc_type=doc_type, body=records)

def main():
    """
    Main function
    """
    #elasticsearch config
    es_cfg = {
        'input_index': 'kb-clean',
        'dest_index': 'kb-clean-geo',
        'host': 'localhost',
        'port': 9200,
        'timeout': 1000,
        'size': 1000,
        'scroll': '2m',
        'doc_type': 'kb-clean',
        'query_locations': {"query":{"terms":{"types":["Location","Facility","GeopoliticalEntity","Physical.OrganizationLocationOrigin"]}}},
        'query': {"query":{"match_all":{}}},
        'loc_types': ["Location", "Facility","GeopoliticalEntity","Physical.OrganizationLocationOrigin"],
        'body':'''{"settings":{"index":{"number_of_shards":3,"number_of_replicas":0}},"mappings":{"kb_clean":{"properties":{"categories":{"type":"string","index":"not_analyzed"},"docIds":{"type":"string","index":"not_analyzed"},"edgeLabel":{"type":"string","index":"not_analyzed"},"edgeTarget":{"type":"string","index":"not_analyzed"},"hypotheses":{"type":"string","index":"not_analyzed"},"kbid":{"type":"string","index":"not_analyzed"},"name":{"type":"string","index":"not_analyzed"},"types":{"type":"string","index":"not_analyzed"},"x":{"type":"long"},"y":{"type":"long"},"geoLocation":{"properties":{"geohash":{"type":"string"},"lon":{"type":"double"},"lat":{"type":"double"}}}}}}}'''
    }

    #geonames config
    gn_cfg = {
        'user': 'psharkey',
        'url': 'http://api.geonames.org/',
        'endpoint': 'searchJSON'
    }

    #estalbish elasticsearch connection
    es = Elasticsearch(
         [
            {
               'host':es_cfg['host'], 
               'port':es_cfg['port']
            }
         ],
         timeout=es_cfg['timeout']
    )

    #create new destination index
    create_dest_index(es, es_cfg['dest_index'], es_cfg['body'])

    #execute process
    process_input_index(es, es_cfg, gn_cfg)

if __name__ == "__main__":
    main()

