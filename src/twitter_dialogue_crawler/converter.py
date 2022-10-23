import argparse
import glob
import html
import json
import os
import re
from typing import Any, Dict, Iterator, List, Optional, Union

import pandas as pd
import zenhan
from pyknp import Juman
from tqdm import tqdm

MIN_STATUS_TEXT_LEN = 7
MIN_DIALOGUE_LEN = 3

REPEAT_PATTERN = re.compile(r'(.+?)\1{4}')
NOT_ASCII_OR_JA_PATTERN = re.compile(r'[^\u0000-\u007f\u3000-\u30ff\u4e00-\u9fff]')
JA_PATTERN = re.compile(r'[\u3000-\u30ff\u4e00-\u9fff]')

jumanpp = Juman()


def clean_text_or_name(text_or_name: str) -> str:
    text_or_name = html.unescape(text_or_name)
    text_or_name = ' '.join(
        ton for ton in text_or_name.split()
        if not (ton.startswith('#') or ton.startswith('@'))
    )
    return text_or_name


def filter_status_text(status_text: str) -> bool:
    # 文字数
    if len(status_text) < MIN_STATUS_TEXT_LEN:
        return False

    # 繰り返し
    if REPEAT_PATTERN.search(status_text):
        return False

    # 日本語
    if NOT_ASCII_OR_JA_PATTERN.search(status_text):
        return False
    if not JA_PATTERN.search(status_text):
        return False

    # 未知語
    result = jumanpp.analysis(zenhan.h2z(status_text))
    if any('未知語' in mrph.imis for mrph in result.mrph_list()):
        return False

    return True


def filter_line_dict(line_dict: Dict[str, Any]) -> bool:
    # 発話数
    if len(line_dict['dialogue']) < MIN_DIALOGUE_LEN:
        return False

    # 話者数
    if len(line_dict['user']) < 2:
        return False

    # 交互
    if any(
        line_dict['dialogue'][i]['user_id']
        == line_dict['dialogue'][i + 1]['user_id']
        for i in range(len(line_dict['dialogue']) - 1)
    ):
        return False

    return True


def _convert_dialogues_to_line_dicts(
    dialogues: Iterator[List[Dict[str, Any]]],
) -> Iterator[Dict[str, Any]]:
    for dialogue in tqdm(dialogues, desc='変換'):
        id_dicts = []

        status_dict = {}
        user_dict = {}
        for status in dialogue:
            status_id_str = status['id_str']
            status_text = status['text']

            status_text = clean_text_or_name(status_text)
            if not filter_status_text(status_text):
                break

            user_id_str = status['user']['id_str']
            user_name = status['user']['name']

            user_name = clean_text_or_name(user_name)

            id_dict = {
                'id': status_id_str,
                'user_id': user_id_str
            }
            id_dicts.append(id_dict)

            status_dict[status_id_str] = status_text
            user_dict[user_id_str] = user_name

        else:
            line_dict = {
                'dialogue': id_dicts,
                'status': status_dict,
                'user': user_dict,
            }
            if not filter_line_dict(line_dict):
                continue

            yield line_dict


def _load_data(
    data_dir: Union[str, os.PathLike],
) -> Iterator[List[Dict[str, Any]]]:
    for data_json in glob.glob(data_dir + '/*.json'):
        with open(data_json, encoding='utf-8') as f:
            yield from json.load(f)


def _dump_outputs_in_jsonl(
    line_dicts: List[Dict[str, Any]],
    output_jsonl: Union[str, os.PathLike],
) -> None:
    lines = set()
    for line_dict in tqdm(line_dicts, desc='保存'):
        line = json.dumps(
            line_dict,
            ensure_ascii=False,
            separators=(',',':'),
        ) + '\n'
        lines.add(line)

    with open(output_jsonl, 'w', encoding='utf-8') as f:
        for line in lines:
            f.write(line)


def _dump_outputs_in_tsv(
    line_dicts: List[Dict[str, Any]],
    output_tsv: Union[str, os.PathLike],
) -> None:
    rows = set()
    for line_dict in tqdm(line_dicts, desc='保存'):
        id_dicts = line_dict['dialogue']
        status_dict = line_dict['status']

        status_texts = ()
        for id_dict in id_dicts:
            status_id = id_dict['id']

            status_text = status_dict[status_id]
            status_texts += (status_text,)

        rows.add(status_texts)

    df = pd.DataFrame(rows)
    df.to_csv(output_tsv, sep='\t', header=False, index=False)


def convert(
    data_dir: Union[str, os.PathLike],
    output_file: Union[str, os.PathLike],
    in_tsv: Optional[bool] = False,
) -> None:
    dialogues = _load_data(data_dir)
    line_dicts = _convert_dialogues_to_line_dicts(dialogues)

    if in_tsv:
        _dump_outputs_in_tsv(line_dicts, output_file)
    else:
        _dump_outputs_in_jsonl(line_dicts, output_file)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--data_dir',
        help='収集した対話のディレクトリ',
    )
    parser.add_argument(
        '--output_file',
        help='変換した対話のファイル',
    )
    parser.add_argument(
        '--in_tsv',
        action='store_true',
        help='変換した対話をTSV形式で保存するかどうか',
    )
    args = parser.parse_args()

    convert(
        args.data_dir,
        args.output_file,
        in_tsv=args.in_tsv,
    )


if __name__ == '__main__':
    main()
