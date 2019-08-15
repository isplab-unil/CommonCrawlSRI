import os
import re
import argparse
from pathlib import Path


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Extract emails from input directory')
    parser.add_argument('input', help='input directory')
    parser.add_argument('output', help='output file')
    args = parser.parse_args()

    with open(args.output, 'w') as out:
        for file in Path(args.input).glob('**/*'):
            if os.path.isfile(str(file)):
                print(str(file))
                try:
                    with open(str(file), 'r') as lines:
                        for line in lines:
                            email = re.search(r'[\w\.-]+@[\w\.-]+', line)
                            if email:
                                out.write(email.group(0) + '\n')
                    print('Done!')
                except UnicodeDecodeError:
                    print('Unable to parse!')
