import argparse
import json
import os
from datetime import datetime
from typing import Any, Dict, List, Set

from dotenv import load_dotenv
import tweepy
from tqdm import tqdm


SOURCES = [
    'TweetDeck',
    'Twitter for Android',
    'Twitter for iPad',
    'Twitter for iPhone',
    'Twitter Web App'
]


def filter_status(
    status: tweepy.models.Status
) -> bool:
    if status.source not in SOURCES:
        return False
    
    if status.entities['urls']:
        return False

    if 'media' in status.entities:
        return False
    
    return True


def collect_user_ids(
    api: tweepy.API,
    q: str
):
    statuses = api.search_tweets(
        q=q,
        lang='ja',
        result_type='recent',
        count=100
    )

    user_ids = set()
    for status in tqdm(statuses):
        if status.source in SOURCES:
            user_ids.add(status.author.id)

    return user_ids


def crawl_user_timeline(
    api: tweepy.API,
    user_ids: Set[int],
    id_to_status: Dict[int, tweepy.models.Status],
    id_to_in_reply_to_status_id: Dict[int, int]
) -> Set[int]:
    in_reply_to_user_ids = set()

    for user_id in tqdm(user_ids):
        try:
            statuses = api.user_timeline(
                user_id=user_id,
                count=200,
                exclude_replies=False,
                include_rts=False
            )

        except:
            continue

        for status in statuses:
            if not filter_status(status):
                continue

            id_to_status[status.id] = status

            if status.in_reply_to_status_id is not None:
                id_to_in_reply_to_status_id[status.id] = (
                    status.in_reply_to_status_id
                )
                in_reply_to_user_ids.add(status.in_reply_to_user_id)

    return in_reply_to_user_ids


def build_dialogues_from_dict(
    id_to_in_reply_to_status_id: Dict[int, int],
    id_to_status: Dict[int, tweepy.models.Status]
) -> List[List[tweepy.models.Status]]:
    leaf_ids = (
        set(id_to_in_reply_to_status_id.keys())
        - set(id_to_in_reply_to_status_id.values())
    )

    dialogues = []
    for leaf_id in tqdm(leaf_ids):
        dialogue = []

        id_ = leaf_id
        while True:
            # no root
            if id_ not in id_to_status.keys():
                break

            dialogue.append(id_to_status[id_])

            # root
            if id_ not in id_to_in_reply_to_status_id.keys():
                dialogues.append(reversed(dialogue))
                break

            id_ = id_to_in_reply_to_status_id[id_]

    return dialogues


def arrange_dialogues_for_jsonl(
    dialogues: List[List[tweepy.models.Status]]
) -> List[Dict[str, Any]]:
    line_dicts = []
    for dialogue in dialogues:
        users = set()

        dialogue_dicts = []
        text_dict = {}
        for status in dialogue:
            dialogue_dict = {
                'id': status.id,
                'user_id': status.author.id
            }
            dialogue_dicts.append(dialogue_dict)

            text_dict[status.id] = {
                'text': status.text,
                'source': status.source
            }
            users.add(status.author)

        if len(users) != 2:
            continue

        user_dict = {}
        for user in users:
            user_dict[user.id] = {
                'name': user.name,
                'description': user.description
            }

        line_dict = {
            'dialogue': dialogue_dicts,
            'text': text_dict,
            'user': user_dict,
        }
        line_dicts.append(line_dict)

    return line_dicts


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--output_dir',
        default='./',
    )
    args = parser.parse_args()

    load_dotenv()

    consumer_key = os.environ['CONSUMER_KEY']
    consumer_secret = os.environ['CONSUMER_SECRET']
    access_token = os.environ['ACCESS_TOKEN']
    access_token_secret = os.environ['ACCESS_TOKEN_SECRET']

    auth = tweepy.OAuth1UserHandler(
        consumer_key,
        consumer_secret,
        access_token,
        access_token_secret,
    )

    api = tweepy.API(
        auth,
        wait_on_rate_limit=True,
    )

    user_ids = collect_user_ids(
        api=api,
        q='い',
    )

    id_to_status = {}
    id_to_in_reply_to_status_id = {}

    in_reply_to_user_ids = crawl_user_timeline(
        api=api,
        user_ids=user_ids,
        id_to_status=id_to_status,
        id_to_in_reply_to_status_id=id_to_in_reply_to_status_id,
    )

    crawl_user_timeline(
        api=api,
        user_ids=in_reply_to_user_ids-user_ids,
        id_to_status=id_to_status,
        id_to_in_reply_to_status_id=id_to_in_reply_to_status_id,
    )

    dialogues = build_dialogues_from_dict(
        id_to_in_reply_to_status_id=id_to_in_reply_to_status_id,
        id_to_status=id_to_status,
    )

    line_dicts = arrange_dialogues_for_jsonl(dialogues)

    output_jsonl = os.path.join(
        args.output_dir,
        '{:%Y%m%d%H%M%S%f}'.format(datetime.now()) + '.jsonl',
    )
    with open(output_jsonl, 'w', encoding='utf-8') as f:
        for line_dict in line_dicts:
            line = json.dumps(
                line_dict,
                ensure_ascii=False,
                separators=(',', ':'),
                sort_keys=True,
            )

            f.write(line + '\n')


if __name__ == '__main__':
    main()
