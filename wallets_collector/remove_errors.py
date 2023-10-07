# Because main.py raised error, maybe some JSON are not valid
# So, this program is here to ensure json are valids

import json
import os
from tqdm import tqdm

folder_path = 'wallets'  # Change this to the path of your folder


def is_valid_json(data):
    try:
        json.loads(data)
        return True
    except ValueError:
        return False


def main():
    for filename in tqdm(os.listdir(folder_path)):
        if filename.endswith('.json'):
            file_path = os.path.join(folder_path, filename)

            file = open(file_path, 'r')
            json_data = file.read()
            file.close()

            if not is_valid_json(json_data):
                os.remove(file_path)
                tqdm.write(f"Invalid JSON removed: {file_path}")


if __name__ == "__main__":
    main()
