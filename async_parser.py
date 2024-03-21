import sys
import asyncio
import datetime
import json
from collections import Counter

from aiohttp import ClientSession
from queue import Queue, Empty

POST_INDEX = 0
COMMENT_INDEX = 1
IS_END = "IS_END"
DAY_PER_SECONDS = 86400

BASE_URL = "http://www.reddit.com/r"
SUBREDDIT = "Python"
read_depth_days = 3
date_from = datetime.datetime.utcnow().timestamp() - read_depth_days * DAY_PER_SECONDS
TOP_N = 10

pages_json_queue = Queue()
posts_json_queue = Queue()

post_authors = []
comment_authors = []


def main(*args):
    global SUBREDDIT
    if len(args) > 1:
        SUBREDDIT = args[1]
    url = f'{BASE_URL}/{SUBREDDIT}.json?sort=new'
    asyncio.run(run(url))


async def run(url):
    asyncio.create_task(get_page(url, pages_json_queue))
    tasks = [asyncio.create_task(coroutine()) for coroutine in (extract_posts_url_and_authors,
                                                                extract_comments)]
    await asyncio.gather(*tasks)

    print(Counter(post_authors).most_common()[:TOP_N])
    print(Counter(comment_authors).most_common()[:TOP_N])


async def get_page(page_url: str, destanation: Queue) -> json:
    page = await resolve_url(page_url)
    try:
        json_page = json.loads(page)
    except json.decoder.JSONDecodeError:
        print(page)
        return
    destanation.put(json_page)


async def resolve_url(page_url: str) -> str:
    async with ClientSession() as client:
        resp = await client.get(page_url)
        page = await resp.text()
    return page


def handle_queue(handling_queue: Queue):
    def handle_queue_decorator(func):
        async def wrapper():
            is_end = False
            tasks = []
            pending_tasks = []

            while not is_end or not handling_queue.empty() or not (len(pending_tasks) == 0):
                if tasks:
                    _, pending_tasks = await asyncio.wait(tasks, timeout=0)

                try:
                    queue_element = handling_queue.get(block=False)
                except Empty:
                    await asyncio.sleep(0)
                    continue

                if queue_element == IS_END:
                    if tasks:
                        await asyncio.wait(tasks)
                    is_end = True

                tasks.extend(await func(queue_element))

                handling_queue.task_done()
                await asyncio.sleep(0)

        return wrapper

    return handle_queue_decorator


@handle_queue(pages_json_queue)
async def extract_posts_url_and_authors(page_json):
    tasks = []
    next_page = True

    if page_json != IS_END:
        for post in page_json['data']['children']:
            created = post['data']['created']
            if created < date_from:
                pages_json_queue.put(IS_END)
                next_page = False
                continue
            else:
                post_authors.append(post['data']['author'])
                tasks.append(asyncio.create_task(get_page(create_post_url(post['data']['id']), posts_json_queue)))
        if next_page:
            tasks.append(
                asyncio.create_task(get_page(create_next_page_url(page_json['data']['after']), pages_json_queue)))
    else:
        posts_json_queue.put(IS_END)

    return tasks


@handle_queue(posts_json_queue)
async def extract_comments(post_json):
    tasks = []

    if post_json != IS_END:
        post_id = post_json[POST_INDEX]['data']['children'][POST_INDEX]['data']['id']

        comments = parse_comments(post_json[COMMENT_INDEX]['data']['children'])

        for comment in comments:
            if comment['kind'] == 'more':
                for comment_id in comment['data']['children']:
                    tasks.append(asyncio.create_task(get_page(create_post_url(post_id, comment_id), posts_json_queue)))

            if comment['kind'] == 't1':
                comment_authors.append(comment['data']['author'])

    return tasks


def parse_comments(comments: dict) -> list[dict]:
    list_of_comments = []
    for comment in comments:
        list_of_comments.append(comment)
        if comment['kind'] == 't1':
            try:
                if comment['data']['replies']:
                    list_of_comments.extend(parse_comments(comment['data']['replies']['data']['children']))
            except KeyError:
                pass

    return list_of_comments


def create_next_page_url(after: str):
    return f'{BASE_URL}/{SUBREDDIT}.json?after={after}&sort=new'


def create_post_url(post_id: str, comment_id: str = None) -> str:
    if comment_id:
        return f"https://www.reddit.com/r/{SUBREDDIT}/comments/{post_id}.json?comment={comment_id}"

    return f"https://www.reddit.com/r/{SUBREDDIT}/comments/{post_id}.json"


if __name__ == '__main__':
    main(*sys.argv)
