import argparse
import json
from collections import defaultdict
from functools import reduce
from operator import concat


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--data_jsonl',
        default='./output.jsonl'
    )
    parser.add_argument(
        '--output_jsonl',
        default='./output_balance.jsonl'
    )
    args = parser.parse_args()

    i_to_line = []
    len_to_is = defaultdict(list)

    with open(args.data_jsonl, encoding='utf-8') as f:
        for i, line in enumerate(f):
            i_to_line.append(line)

            line_dict = json.loads(line)
            id_dicts = line_dict['dialogue']

            len_ = len(id_dicts)
            len_to_is[len_].append(i)

    for len_ in len_to_is.keys():
        is_ = len_to_is[len_]

        tmp = len(is_) // (2 ** max(5 - len_, 0))
        len_to_is[len_] = is_[:tmp]
    
    is_ = sorted(reduce(concat, len_to_is.values()))

    with open(args.output_jsonl, 'w', encoding='utf-8') as f:
        for i in is_:
            f.write(i_to_line[i])


if __name__ == '__main__':
    main()
