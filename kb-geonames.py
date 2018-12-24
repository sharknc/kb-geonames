from elasticsearch2 import Elasticsearch
import json
import requests
import pygeohash as gh

def process_results(hits, es_cfg, gn_cfg):
    """
    Add geolocation and query geonames for current scroll
    results

    :param es: TODO
    :param hits: TODO
    """
    json_str = ""
    for item in hits:
        name = item['_source']['name']
        if name is not None:
            # get source data and add geo info
            lat, lon = geonames_lookup(name, gn_cfg)
            geolocation = build_geolocation(lat, lon)

            #build bulk json record string
            json_str += '{"index":{"_index":"' + es_cfg['dest_index'] + '", "_type":"' + es_cfg['doc_type'] + '"}}\n'
            json_str += ''.join(json.dumps(item['_source']))
            json_str += '\n'

    return json_str
 
def geonames_lookup(value, gn_cfg):
    """
    Send Geonames request for value

    :param: value: TODO
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
    """
    if lat and lon:
        geohash = gh.encode(float(lat), float(lon), 8)
        geolocation = {'geolocation': { 'geohash':geohash,'lat':lat,'lon':lon}}
        print(geolocation)

def process_input_index(es, es_cfg, gn_cfg):
    """
    Process input elasticsearch index
    for geolocation data

    :param es: TODO
    :param index: TODO
    :param size: TODO
    """
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

    print("Found: " + str(scroll_size) + " results")

    d = process_results(data['hits']['hits'], es_cfg, gn_cfg)
    if d:
        insert_bulk_es_records(es, es_cfg['dest_index'], d) 

    while scroll_size > 0:
        data = es.scroll(scroll_id=sid, scroll=es_cfg['scroll'])

        # process next batch of results
        d = process_results(data['hits']['hits'], es_cfg, gn_cfg)
        if d:
            insert_bulk_es_records(es, es_cfg['dest_index'], d)

        # update scroll id and size
        sid = data['_scroll_id']
        scroll_size = len(data['hits']['hits'])

def create_dest_index(es, es_cfg):
    """
    Creates new elasticsearch index

    :param es: TODO
    :param idnex: TODO
    :param body: TODO
    """
    #verify index does not already exist
    if es.indices.exists(index=es_cfg['dest_index']):
        print("ERROR: Attempted to create index " + es_cfg['dest_index'] + " that already exists")
        exit()

    es.indices.create(index=es_cfg['dest_index'], body=es_cfg['body'])

def insert_es_record(es, index, record):
    """
    Insert record into elasticsearch

    :param es: TODO
    :param index: TODO
    :param record: TODO
    """
    es.create(index=index, doc_type='kb-clean',  body=record)

def insert_bulk_es_records(es, index, records):
    """
    Insert bulk json record into elasticsearch

    :param es: TODO
    :param index: TODO
    :param records: TODO
    """
    es.bulk(index=index, doc_type='kb-clean', body=records)

def main():
    """
    Main function
    """
    #elasticsearch config
    es_cfg = {
            'input_index': 'kb-clean',
            'dest_index': 'kb-clean-g',
            'host': 'localhost',
            'port': 9200,
            'timeout': 1000,
            'size': 1000,
            'scroll': '2m',
            'doc_type': 'kb-clean',
            'query': {"query":{"terms":{"types":["Location","Facility","GeopoliticalEntity","Physical.OrganizationLocationOrigin"]}}},
            'body':'''{"settings":{"index":{"number_of_shards":3,"number_of_replicas":0}},"mappings":{"kb_clean":{"properties":{"categories":{"type":"string","index":"not_analyzed"},"docIds":{"type":"string","index":"not_analyzed"},"edgeLabel":{"type":"string","index":"not_analyzed"},"edgeTarget":{"type":"string","index":"not_analyzed"},"hypotheses":{"type":"string","index":"not_analyzed"},"kbid":{"type":"string","index":"not_analyzed"},"name":{"type":"string","index":"not_analyzed"},"types":{"type":"string","index":"not_analyzed"},"x":{"type":"long"},"y":{"type":"long"},"geoLocation":{"properties":{"geohash":{"type":"string"},"lon":{"type":"double"},"lat":{"type":"double"}}}}}}}'''
         }

    #geonames variables
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

    create_dest_index(es, es_cfg)
    process_input_index(es, es_cfg, gn_cfg)

if __name__ == "__main__":
    main()

