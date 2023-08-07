from elasticsearch import Elasticsearch

def create_es_client(username, password, cloudid):
    es = Elasticsearch(
        cloud_id=cloudid,
        basic_auth=(username, password)
    )
    return es
