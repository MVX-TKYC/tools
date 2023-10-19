# Wallets Data Scrapper Readme

## Overview

This Python script is designed for scraping data related to cryptocurrency wallets from the MultiversX blockchain. It fetches transaction data, smart contract results, and logs for a list of wallets, saving the collected data in separate JSON files. The script also allows you to specify various parameters for customization.


## Prerequisites

Before using this script, make sure you have the following dependencies installed:

- Python 3.7+
- Required Python packages (can be installed using `pip`):
  - `asyncio`
  - `datetime`
  - `os`
  - `json`
  - `random`
  - `requests`
  - `argparse`
  - `tqdm`
  - `concurrent.futures`
  - `ratelimit`

You can install the required packages using the following command:

```
pip install asyncio datetime os json random requests argparse tqdm concurrent.futures ratelimit
```

## Usage

To use this script, follow these steps:

1. Clone or download the script to your local machine.

2. Open a terminal or command prompt and navigate to the directory where the script is located.

3. Run the script with the following command:

   ```
   python main.py
   ```

### Command Line Arguments

The script accepts the following command line arguments:

- `-l` or `--list`: Path to the wallets list file (default: "lists\all_wallets.txt"). This file should contain a list of wallet addresses to scrape.

- `-s` or `--shuffle`: Shuffle the order of wallets in the list (default: False). Shuffling helps distribute the workload across multiple workers.

- `-w` or `--workers`: Number of worker threads to use for concurrent scraping (default: 16). Adjust this value based on your machine's capabilities.

- `-o` or `--output`: Output folder name (default: current timestamp). The scraped data will be saved in a folder with this name inside the "wallets" directory.

### Output

The script will create an output folder inside the "wallets" directory (e.g., "wallets/current_timestamp") based on the provided or default output folder name. Inside this folder, you will find JSON files named after each wallet address. These files contain the scraped data for each wallet.

## Important Notes

- This script is specifically designed for scraping data from the MultiversX blockchain. Make sure you have the necessary permissions and comply with all relevant terms of service when using this script.

- Be aware of rate limits and adjust the number of workers accordingly to avoid overloading the API.

- The script may take some time to complete, depending on the number of wallets in your list and the size of the blockchain data.

## Development Choices

- Getting data as fast as possible: The faster we get the data, the faster we can train the IA. So we choose to ignore wallets with less than 50 transactions
- Scraping method: Instead of API, we used Elastic Search which has no rate limit.


## Contributing

If you encounter issues or have suggestions for improvements, feel free to open an issue or submit a pull request on the script's GitHub repository.

## License

This script is released under the MIT License. You can find the full license details in the LICENSE file included with the script.

---

**Disclaimer:** This script is provided for educational and informational purposes only. Use it responsibly and in compliance with all applicable laws and regulations. The script author are not responsible for any misuse or illegal activities involving this script.
