import argparse
import glob
import json


def convert_dialogue(dialogue):
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

    return {
        'dialogue': id_dicts,
        'status': status_dict,
        'user': user_dict
    }
 

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

    for json_file in glob.glob(args.data_dir + '/*.json'):
        with open(json_file, encoding='utf-8') as f:
            dialogues.extend(json.load(f))

    line_dicts = map(convert_dialogue, dialogues)

    with open(args.output_jsonl, 'w', encoding='utf-8') as f:
        for line_dict in line_dicts:
            line = json.dumps(
                line_dict,
                ensure_ascii=False,
                separators=(',', ':')
            )

            f.write(line + '\n')


if __name__ == '__main__':
    main()
