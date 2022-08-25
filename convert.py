import argparse
import glob
import json
from typing import Any, Dict

from tqdm import tqdm


def convert_dialogue_into_line(
    dialogue: Dict[str, Any]
) -> Dict[str, Any]:
    id_dicts = []

    status_dict = {}
    user_dict = {}

    for status in dialogue:
        id_ = status['id']

        user = status['user']
        user_id = user['id']

        id_dict = {
            'id': id_,
            'user_id': user_id
        }
        id_dicts.append(id_dict)

        status_dict[id_] = {
            'text': status['text'],
        }

        user_dict[user_id] = {
            'name': user['name'],
            'description': user['description']
        }
    
    line = json.dumps(
        {
            'dialogue': id_dicts,
            'status': status_dict,
            'user': user_dict
        },
        ensure_ascii=False,
        separators=(',', ':')
    )
    return line
 

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--data_dir',
        default='./'
    )
    parser.add_argument(
        '--output_jsonl',
        default='./output.jsonl'
    )
    args = parser.parse_args()

    dialogues = []
    for json_file in tqdm(glob.glob(args.data_dir + '/*.json')):
        with open(json_file, encoding='utf-8') as f:
            try:
                dialogues.extend(json.load(f))
            except json.JSONDecodeError as err:
                print(err)

    lines = set(map(convert_dialogue_into_line, dialogues))
    with open(args.output_jsonl, 'w', encoding='utf-8') as f:
        for line in lines:
            f.write(line + '\n')


if __name__ == '__main__':
    main()
