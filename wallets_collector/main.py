import asyncio
from datetime import datetime
import os
import json
import random
import requests
import argparse
from tqdm import tqdm
import concurrent.futures
from ratelimit import limits, sleep_and_retry

WALLETS_FOLDER = "wallets"
MAX_WORKERS = min(32, os.cpu_count() + 4)

# 10 calls per seconds
CALLS = 5
PERIOD = .5


@sleep_and_retry
@limits(calls=CALLS, period=PERIOD)
def get_request_content(url, query):
    headers = {"Content-Type": "application/json"}

    # Send a POST request to the provided URL with the given query
    # Return the JSON content of the response
    response = requests.post(url, headers=headers, data=json.dumps(query))

    response.raise_for_status()

    return response.json()


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


def getAllWallets(file, shuffle):
    tqdm.write(f"Reading {file}")

    def getIgnoredWallets():
        ignoredWallets = []

        for _, _, files in os.walk(WALLETS_FOLDER):
            for file in files:
                filename = os.path.splitext(file)[0]
                ignoredWallets.append(filename)

        return ignoredWallets

    with open(file, "r") as accountsFile:
        ignoredWallets = getIgnoredWallets()

        wallets = accountsFile.read().splitlines()

        if shuffle is True:
            random.shuffle(wallets)
            print("Shuffling wallets")

        print("Found {0}/{1} ({2}%) wallets already processed.".format(
            len(ignoredWallets), len(wallets), round(len(ignoredWallets) / len(wallets) * 100)))

        wallets = filter(lambda a: not a.startswith(
            "erd1qqq") and not a in ignoredWallets, wallets)

        return wallets


def processWallet(parentFolder, wallet, pbar):

    url = "https://index.multiversx.com/transactions/_search?scroll=1m&size=10000"
    query = {"query": {"bool": {"should": [{"match": {e: wallet}} for e in [
        "sender", "receiver", "receivers"]]}}, "sort": [{"timestamp": {"order": "desc"}}], "track_total_hits": True}
    data = get_request_content_scroll(url, query)

    if not data:
        with open(os.path.join(WALLETS_FOLDER, "ignored", wallet+".txt"), "w") as f:
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
    with open(os.path.join(parentFolder, wallet+".json"), "w") as f:
        json.dump(data, f, ensure_ascii=False)

    pbar.update(1)


async def main(walletsFile, shuffle):

    timestamp = str(int(
        datetime.timestamp(datetime.now())))
    parentFolder = os.path.join(WALLETS_FOLDER, timestamp)

    os.makedirs(parentFolder, exist_ok=True)
    wallets = getAllWallets(walletsFile, shuffle)

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:

        loop = asyncio.get_event_loop()
        pbar = tqdm(unit=" account")

        tasks = [
            loop.run_in_executor(
                executor,
                processWallet,
                *(parentFolder, wallet, pbar)
            )
            for wallet in wallets
        ]
        for _ in await asyncio.gather(*tasks):
            pass

        pbar.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-l", "--list", help="The wallets list",
                        type=str, default=r"lists\all_wallets.txt")
    parser.add_argument("-s", "--shuffle", type=bool)
    args = parser.parse_args()

    loop = asyncio.get_event_loop()
    main_task = asyncio.ensure_future(main(args.list, args.shuffle))

    loop.run_until_complete(main_task)
