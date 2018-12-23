from elasticsearch2 import Elasticsearch
import json
import geocoder

def process_results(es, hits):
    """
    Add geolocation and query geonames for current scroll
    results
    """
    json_str = ""
    for item in hits:
        name = item['_source']['name']
        if name is not None:
            # get source data and add geo info
            print('Geonames lookup: ' + name)
            json_str += '{"index":{"_index":"kb-clean-g", "_type":"kb-clean"}}\n'
            json_str += ''.join(json.dumps(item['_source']))
            json_str += '\n'

    return json_str

        
def geonames_lookup(value):
    """
    Send Geonames request for value
    """
    g = geocoder.geonames(value, method='details', key=geonames_user)
    if g is not None:
        print('Value: ' + value + ' [' + str(g.lat) + ',' + str(g.lng) + ']')

def process_input_index(es, index, size, query, scroll, output_index):
    """
    Process input elasticsearch index
    for geolocation data
    """
    if not es:
        print('ERROR: empty elasticsearch connection, exiting')
        exit()

    # verify if index exists
    if not es.indices.exists(index=index):
        print('ERROR: index ' + index + ' does not exist at http://' + str(es.hosts))
        exit()

    # execute query
    data = es.search(
        index=index,
        scroll=scroll,
        size=size,
        body=query
    )

    # get scroll id and size
    sid = data['_scroll_id']
    scroll_size = len(data['hits']['hits'])

    print("Found: " + str(scroll_size) + " results")

    d = process_results(es, data['hits']['hits'])
    if d:
        insert_bulk_es_records(es, output_index, d) 

    while scroll_size > 0:
        data = es.scroll(scroll_id=sid, scroll=scroll)

        # process next batch of results
        d = process_results(es, data['hits']['hits'])
        if d:
            insert_bulk_es_records(es, output_index, d)

        # update scroll id and size
        sid = data['_scroll_id']
        scroll_size = len(data['hits']['hits'])

def create_es_index(es, index, body):
    """
    Creates new elasticsearch index
    """
    #verify index does not already exist
    if es.indices.exists(index=index):
        print("ERROR: Attempted to create index " + index + " that already exists")
        exit()

    es.indices.create(index=index, body=body)

def insert_es_record(es, index, record):
    """
    Insert record into elasticsearch
    """
    es.create(index=index, doc_type='kb-clean',  body=record)

def insert_bulk_es_records(es, index, records):
    """
    Insert bulk json record into elasticsearch
    """
    es.bulk(index=index, doc_type='kb-clean', body=records)

def get_geohash(lat, lng):
    """
    Gets the geohash for a lat/lng
    """
    print('Generating geohash')

def main():
    """
    Main function
    """

    #elasticsearch variables
    es_input_index='kb-clean'
    es_output_index='kb-clean-g'
    es_host='localhost'
    es_port=9200
    es_timeout=1000
    es_size=1000
    es_scroll='2m'
    es_doc_type='kb-clean'
    es_query={"query":{"terms":{"types":["Location","Facility","GeopoliticalEntity","Physical.OrganizationLocationOrigin"]}}}
    es_output_body='''{"settings":{"index":{"number_of_shards":3,"number_of_replicas":0}},"mappings":{"kb_clean":{"properties":{"categories":{"type":"string","index":"not_analyzed"},"docIds":{"type":"string","index":"not_analyzed"},"edgeLabel":{"type":"string","index":"not_analyzed"},"edgeTarget":{"type":"string","index":"not_analyzed"},"hypotheses":{"type":"string","index":"not_analyzed"},"kbid":{"type":"string","index":"not_analyzed"},"name":{"type":"string","index":"not_analyzed"},"types":{"type":"string","index":"not_analyzed"},"x":{"type":"long"},"y":{"type":"long"},"geoLocation":{"properties":{"geohash":{"type":"string"},"lon":{"type":"double"},"lat":{"type":"double"}}}}}}}'''

    #geonames variables
    geonames_user='psharkey'

    #estalbish elasticsearch connection
    es = Elasticsearch(
         [
            {
               'host':es_host, 
               'port':es_port
            }
         ],
         timeout=es_timeout
    )

    create_es_index(es, es_output_index, es_output_body)

    #g = geocoder.geonames('New York', key='psharkey')
    #print(g.description)
    #if lat is not None:
    #    print("Error occured when querying geonames for with: " + 'New York')
    #else:
    #    print("lat: " + str(lat))

    process_input_index(es, es_input_index, es_size, es_query, es_scroll, es_output_index)

if __name__ == "__main__":
    main()

