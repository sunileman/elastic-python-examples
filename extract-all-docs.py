from elasticsearch import Elasticsearch
import json

# Initialize Elasticsearch connection
es = Elasticsearch(
    cloud_id=<CLOUD_I<D>,
    basic_auth=(<ELASTIC_USERNAME>, <ELASTIC_PASSWORD>)
)

index_name = <INDEX_NAME>
output_file = "output.json"

## Scroll API initialization using individual parameters instead of 'body'
page = es.search(
    index=index_name,
    scroll='2m',
    size=1000
)

sid = page['_scroll_id']
scroll_size = len(page['hits']['hits'])

# Open the file to write
with open(output_file, 'w') as outfile:
    while scroll_size > 0:
        # Get the documents from the current batch
        for hit in page['hits']['hits']:
            # Write the source document to the file as JSON
            outfile.write(json.dumps(hit['_source']))
            outfile.write("\n")

        # Use the scroll API to get the next batch of documents
        page = es.scroll(scroll_id=sid, scroll='2m')

        # Update the scroll ID and size for the next iteration
        sid = page['_scroll_id']
        scroll_size = len(page['hits']['hits'])

print("Extraction complete!")
