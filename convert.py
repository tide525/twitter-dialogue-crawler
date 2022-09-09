import argparse
import glob
import html
import json
import os
import re
from typing import Any, Dict, Set

from tqdm import tqdm


MIN_STATUS_TEXT_LEN = 8
REPEAT_PATTERN = re.compile(r'(.+?)\1{4}')
NOT_ASCII_OR_JA_PATTERN = re.compile(r'[^\u0000-\u007f\u3000-\u30ff\u4e00-\u9fff]')
JA_PATTERN = re.compile(r'[\u3000-\u30ff\u4e00-\u9fff]')

MIN_DIALOGUE_LEN = 6


def clean_text_or_name(text_or_name: str) -> str:
    text_or_name = html.unescape(text_or_name)
    text_or_name = ' '.join(
        ton for ton in text_or_name.split()
        if not (ton.startswith('#') or ton.startswith('@'))
    )
    return text_or_name


def filter_status_text(status_text: str) -> bool:
    if len(status_text) < MIN_STATUS_TEXT_LEN:
        return False
    if REPEAT_PATTERN.search(status_text):
        return False
    if NOT_ASCII_OR_JA_PATTERN.search(status_text):
        return False
    if not JA_PATTERN.search(status_text):
        return False
    return True


def filter_line_dict(
    line_dict: Dict[str, Any]
) -> bool:
    if len(line_dict['dialogue']) < MIN_DIALOGUE_LEN:
        return False
    if len(line_dict['user']) < 2:
        return False
    if any(
        line_dict['dialogue'][i]['user_id']
        == line_dict['dialogue'][i + 1]['user_id']
        for i in range(len(line_dict['dialogue']) - 1)
    ):
        return False
    return True


def convert_dialogues_into_lines(
    dialogues: Dict[str, Any]
) -> Set[str]:
    lines = set()

    for dialogue in dialogues:
        id_dicts = []

        status_dict = {}
        user_dict = {}

        for status in dialogue:
            status_id = status['id']
            status_text = status['text']

            status_text = clean_text_or_name(status_text)
            if not filter_status_text(status_text):
                break

            user_id = status['user']['id']
            user_name = status['user']['name']

            user_name = clean_text_or_name(user_name)

            id_dict = {
                'id': status_id,
                'user_id': user_id
            }
            id_dicts.append(id_dict)

            status_dict[status_id] = status_text
            user_dict[user_id] = user_name

        else:
            line_dict = {
                'dialogue': id_dicts,
                'status': status_dict,
                'user': user_dict,
            }
            if not filter_line_dict(line_dict):
                continue

            line = json.dumps(
                line_dict,
                ensure_ascii=False,
                separators=(',', ':')
            )
            lines.add(line)

    return lines


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

    lines = set()

    json_files = glob.glob(os.path.join(args.data_dir, '**', '*.json'), recursive=True)
    for json_file in tqdm(json_files):
        with open(json_file, encoding='utf-8') as f:
            try:
                dialogues = json.load(f)
                lines |= convert_dialogues_into_lines(dialogues)
            except json.JSONDecodeError as err:
                print(err)

    with open(args.output_jsonl, 'w', encoding='utf-8') as f:
        for line in lines:
            f.write(line + '\n')


if __name__ == '__main__':
    main()
