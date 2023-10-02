import os
import json
import time
import requests

# Overriding the built-in print function to include timestamps
original_print = print


def new_print(*args, **kwargs):
    # Add a timestamp to each print call
    original_print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}]", *args, **kwargs)


print = new_print

# Create a 'wallets' directory if it does not exist
os.makedirs("wallets", exist_ok=True)

# Define the header for HTTP requests
headers = {"Content-Type": "application/json"}


def get_request_content(url, query):
    # Send a POST request to the provided URL with the given query
    # Return the JSON content of the response
    return requests.post(url, headers=headers, data=json.dumps(query)).json()


def get_request_content_scroll(url, query):
    # Initialize by sending a request to the provided URL
    data = get_request_content(url, query)
    # Extract the total number of hits
    total_data = data["hits"]["total"]["value"]

    if 100000 < total_data:
        print(f"Too much transactions ? ({total_data})")
        return
    else:
        print(f"Total: {total_data}")
        scroll_id = data["_scroll_id"]
        all_data = data["hits"]["hits"]

        # Keep scrolling and collecting the data until we have collected all hits
        while len(all_data) < total_data:
            query = {"scroll": "1m", "scroll_id": scroll_id}
            data = get_request_content(
                "https://index.multiversx.com/_search/scroll", query)
            scroll_id = data["_scroll_id"]
            all_data += data["hits"]["hits"]
            print(f"Current: {len(all_data)}")
        return all_data


# Start by querying the accounts
url = "https://index.multiversx.com/accounts/_search?scroll=1m&size=10"
query = {"query": {"match_all": {}}, "sort": [
    {"balanceNum": {"order": "desc"}}], "track_total_hits": True}
accounts = [e["_id"] for e in get_request_content(url, query)["hits"]["hits"]]

# Loop over each account and fetch the related transactions, smart contract results, and logs
for i, wallet in enumerate(accounts):
    print(i+1, wallet)

    url = "https://index.multiversx.com/transactions/_search?scroll=1m&size=10000"
    query = {"query": {"bool": {"should": [{"match": {e: wallet}} for e in [
        "sender", "receiver", "receivers"]]}}, "sort": [{"timestamp": {"order": "desc"}}], "track_total_hits": True}
    data = get_request_content_scroll(url, query)

    if not data:
        continue

    # Filter out the ids of transactions with results, operations, or logs
    ids = [e["_id"] for e in data if e["_source"].get(
        "hasScResults") or e["_source"].get("hasOperations") or e["_source"].get("hasLogs")]

    # Get all smart contract results related to the filtered transactions
    url = "https://index.multiversx.com/scresults/_search?scroll=1m&size=10000"
    query = {"query": {"bool": {"should": [
        {"terms": {"originalTxHash": ids}}]}}, "track_total_hits": True}
    scresults = get_request_content_scroll(url, query)

    scresults_dict = {}
    for scresult in scresults:
        tx = scresult["_source"]["originalTxHash"]
        if tx not in scresults_dict:
            scresults_dict[tx] = [scresult]
        else:
            scresults_dict[tx] += [scresult]

    # Get all logs related to the filtered transactions
    url = "https://index.multiversx.com/logs/_search?scroll=1m&size=10000"
    query = {"query": {"bool": {"should": [
        {"terms": {"originalTxHash": ids}}]}}, "track_total_hits": True}
    logs = get_request_content_scroll(url, query)

    logs_dict = {}
    for log in logs:
        tx = log["_source"]["originalTxHash"]
        if tx not in logs_dict:
            logs_dict[tx] = [log]
        else:
            logs_dict[tx] += [log]

    print(len(data), len(scresults), len(logs))

    # Attach the corresponding smart contract results and logs to each transaction
    for i, transaction in enumerate(data):
        scresult = scresults_dict.get(transaction["_id"])
        if scresult:
            data[i]["_source"].setdefault("events", []).extend(scresult)
        log = logs_dict.get(transaction["_id"])
        if log:
            data[i]["_source"].setdefault("events", []).extend(log)

    # Save the fetched and combined data for each wallet to a separate JSON file
    with open(f"wallets/{wallet}.json", "w") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)
