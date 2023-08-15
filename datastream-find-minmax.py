from elasticsearch import Elasticsearch

# Connect to Elasticsearch
# Create the client instance
es = Elasticsearch(
    cloud_id="ess-cloud-id",
    basic_auth=("username", "password")
)

# Get the list of all backing indices for 'my-datastream'
response = es.cat.indices(index="my-datastream*", h="index", format="json")
indices = [index_info['index'] for index_info in response]

# For each backing index, find the min and max event_timestamp
results = {}

for index in indices:
    response = es.search(index=index, body={
        "size": 0,
        "aggs": {
            "min_event_timestamp": {
                "min": {
                    "field": "event_timestamp"
                }
            },
            "max_event_timestamp": {
                "max": {
                    "field": "event_timestamp"
                }
            }
        }
    })

    results[index] = {
        "min_event_timestamp": response['aggregations']['min_event_timestamp']['value_as_string'],
        "max_event_timestamp": response['aggregations']['max_event_timestamp']['value_as_string']
    }

# Print results
for index, values in results.items():
    print(f"Index: {index}")
    print(f"\tMin event_timestamp: {values['min_event_timestamp']}")
    print(f"\tMax event_timestamp: {values['max_event_timestamp']}")

