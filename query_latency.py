from es_helper import create_es_client
from time import sleep

username = ''
password = ''
cloudid = ''
index_name = ''

# get es object
es = create_es_client(username, password, cloudid)

# validate es object
response = es.info()
print(response)


def fetch_current_query_total(es, index_name):
    # Get the statistics for the specific index
    stats = es.indices.stats(index=index_name)

    # Extract the total number of queries for the index
    query_total = stats['indices'][index_name]['total']['search']['query_total']

    return query_total


def fetch_current_query_time_in_millis(es, index_name):
    # Get the statistics for the specific index
    stats = es.indices.stats(index=index_name)

    # Extract the total query time in milliseconds for the index
    query_time_in_millis = stats['indices'][index_name]['total']['search']['query_time_in_millis']

    return query_time_in_millis


es = create_es_client(username, password, cloudid)

previous_query_total = 0
previous_query_time_in_millis = 0
time_interval = 5
monitoring = True

while monitoring:
    try:
        current_query_total = fetch_current_query_total(es, index_name)
        current_query_time_in_millis = fetch_current_query_time_in_millis(es, index_name)

        queries_since_last_check = current_query_total - previous_query_total
        time_since_last_check = current_query_time_in_millis - previous_query_time_in_millis

        if queries_since_last_check > 0:
            average_query_latency_in_millis = time_since_last_check / queries_since_last_check
            print("Average query latency:", average_query_latency_in_millis, "ms")

        previous_query_total = current_query_total
        previous_query_time_in_millis = current_query_time_in_millis

        sleep(time_interval)
    except Exception as e:
        print(f"An error occurred: {e}")
