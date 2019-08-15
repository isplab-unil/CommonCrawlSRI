import os
import re
import argparse
import json
import urllib.request


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Extract emails from input directory')
    parser.add_argument('input', help='input file')
    parser.add_argument('output', help='output file')
    args = parser.parse_args()

    with open(args.input) as file:
        data = json.load(file)
        with open(args.output, 'w') as out:
            for row in data['rows']:
                id = row['id']
                print('Package: ' + id)
                url = "https://cdn.jsdelivr.net/npm/" + id + "/package.json"
                print('URL: ' + url)
                try:
                    lines = urllib.request.urlopen("https://cdn.jsdelivr.net/npm/" + id + "/package.json")
                    for line in lines:
                        email = re.search(r'[\w\.-]+@[\w\.-]+', line.decode('utf-8'))
                        if email:
                            out.write(email.group(0) + '\n')
                    print("Done!")
                except:
                    print("Error!")

