import os
import json
import requests
import argparse
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor
import concurrent.futures

WALLETS_FOLDER = "wallets"
MAX_WORKERS = min(32, os.cpu_count() + 4)


def get_request_content(url, query):
    headers = {"Content-Type": "application/json"}

    # Send a POST request to the provided URL with the given query
    # Return the JSON content of the response
    return requests.post(url, headers=headers, data=json.dumps(query)).json()


def get_request_content_scroll(url, query):
    # Initialize by sending a request to the provided URL
    data = get_request_content(url, query)
    # Extract the total number of hits
    total_data = data["hits"]["total"]["value"]

    if 100000 < total_data:
        tqdm.write(f"Too much transactions ? ({total_data})")
        return
    else:
        scroll_id = data["_scroll_id"]
        all_data = data["hits"]["hits"]

    # Keep scrolling and collecting the data until we have collected all hits
    while len(all_data) < total_data:
        query = {"scroll": "1m", "scroll_id": scroll_id}
        data = get_request_content(
            "https://index.multiversx.com/_search/scroll", query)
        scroll_id = data["_scroll_id"]
        all_data += data["hits"]["hits"]
    return all_data


def getAllWallets(file):
    tqdm.write(f"Reading {file}")

    def getFilesWithoutExtension(parent):
        files = os.listdir(parent)

        return [os.path.splitext(
            filename)[0] for filename in files]

    with open(file, "r") as accountsFile:
        ignoredAccounts = getFilesWithoutExtension(WALLETS_FOLDER)

        accounts = accountsFile.read().splitlines()

        print("Found {0}/{1} ({2}%) accounts already processed.".format(
            len(ignoredAccounts), len(accounts), round(len(ignoredAccounts) / len(accounts) * 100)))

        accounts = filter(lambda a: not a.startswith(
            "erd1qqq") and not a in ignoredAccounts, accounts)

        return accounts


def processWallet(wallet):

    url = "https://index.multiversx.com/transactions/_search?scroll=1m&size=10000"
    query = {"query": {"bool": {"should": [{"match": {e: wallet}} for e in [
        "sender", "receiver", "receivers"]]}}, "sort": [{"timestamp": {"order": "desc"}}], "track_total_hits": True}
    data = get_request_content_scroll(url, query)

    if not data:
        with open(os.path.join(WALLETS_FOLDER, wallet+".txt"), "w") as f:
            f.write("ignored")
        return

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

    # Attach the corresponding smart contract results and logs to each transaction
    for i, transaction in enumerate(data):
        scresult = scresults_dict.get(transaction["_id"])
        if scresult:
            data[i]["_source"].setdefault("events", []).extend(scresult)
        log = logs_dict.get(transaction["_id"])
        if log:
            data[i]["_source"].setdefault("events", []).extend(log)

    # Save the fetched and combined data for each wallet to a separate JSON file
    with open(os.path.join(WALLETS_FOLDER, wallet+".json"), "w") as f:
        json.dump(data, f, ensure_ascii=False)


def main():
    # Create a 'wallets' directory if it does not exist
    os.makedirs(WALLETS_FOLDER, exist_ok=True)

    parser = argparse.ArgumentParser()
    parser.add_argument("-l", "--list", help="The wallets list",
                        type=str, default=r"lists\all_wallets.txt")
    args = parser.parse_args()
    pbar = tqdm(unit=" account")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:

        futures = []

        for wallet in getAllWallets(args.list):
            futures.append(executor.submit(processWallet, wallet))

        for future in tqdm(concurrent.futures.as_completed(futures)):
            pbar.update()


if __name__ == "__main__":
    main()
