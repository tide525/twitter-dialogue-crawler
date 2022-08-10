import argparse
import json
import os
from datetime import datetime
from typing import Any, Dict, List, Set

from dotenv import load_dotenv
import tweepy
from tqdm import tqdm


SEARCH_TWEETS_COUNT = 100
SEARCH_TWEETS_Q = 'い'

USER_TIMELINE_COUNT = 200

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
):
    statuses = api.search_tweets(
        q=SEARCH_TWEETS_Q,
        lang='ja',
        result_type='recent',
        count=SEARCH_TWEETS_COUNT
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
                count=USER_TIMELINE_COUNT,
                exclude_replies=False,
                include_rts=False
            )

        except:
            continue

        for status in statuses:
            if not filter_status(status):
                continue

            id_to_status[status.id] = status._json

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
                dialogues.append(list(reversed(dialogue)))
                break

            id_ = id_to_in_reply_to_status_id[id_]

    return dialogues


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

    output_jsonl = os.path.join(
        args.output_dir,
        '{:%Y%m%d%H%M%S%f}'.format(datetime.now()) + '.json',
    )
    with open(output_jsonl, 'w', encoding='utf-8') as f:
        json.dump(
            dialogues,
            f,
            ensure_ascii=False,
            separators=(',', ':')
        )


if __name__ == '__main__':
    main()
