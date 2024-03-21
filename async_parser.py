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
read_depth_days = 1
date_from = datetime.datetime.utcnow().timestamp() - read_depth_days * DAY_PER_SECONDS
TOP_N = 10

pages_url_queue = Queue()
pages_json_queue = Queue()
posts_url_queue = Queue()
posts_json_queue = Queue()

post_authors = []
comment_authors = []


def main(*args):
    global SUBREDDIT
    if len(args) > 1:
        SUBREDDIT = args[1]
    url = f'{BASE_URL}/{SUBREDDIT}.json?sort=new'
    pages_url_queue.put(url)
    asyncio.run(run())


async def run():
    tasks = [asyncio.create_task(coroutine()) for coroutine in (handle_page_url,
                                                                extract_posts_url_and_authors,
                                                                get_post_pages,
                                                                get_comments)]
    await asyncio.gather(*tasks)

    print(Counter(post_authors).most_common()[:TOP_N])
    print(Counter(comment_authors).most_common()[:TOP_N])
    print(len(comment_authors))


async def get_page(page_url: str, resourse: Queue) -> json:
    page = await resolve_url(page_url)
    try:
        json_page = json.loads(page)
    except json.decoder.JSONDecodeError:
        print(page)
        return
    resourse.put(json_page)


async def resolve_url(page_url: str) -> str:
    async with ClientSession() as client:
        resp = await client.get(page_url)
        page = await resp.text()
    return page


def handle_queue(handling_queue: Queue):
    def handle_queue_decorator(func):
        async def wrapper():
            is_end = False
            print(f'{func.__name__} START')
            while not is_end:

                try:
                    queue_element = handling_queue.get(block=False)
                except Empty:
                    await asyncio.sleep(0)
                    continue

                await func(queue_element)

                if queue_element == IS_END:
                    is_end = True

                handling_queue.task_done()
                await asyncio.sleep(0)

            print(f'{func.__name__} END')
        return wrapper
    return handle_queue_decorator


@handle_queue(pages_url_queue)
async def handle_page_url(page_url: str):
    print(page_url)
    if page_url != IS_END:
        asyncio.create_task(get_page(page_url, pages_json_queue))
    else:
        pages_json_queue.put(IS_END)


@handle_queue(pages_json_queue)
async def extract_posts_url_and_authors(page_json):
    if page_json != IS_END:
        for post in page_json['data']['children']:
            created = post['data']['created']
            if created < date_from:
                pages_url_queue.put(IS_END)
                continue
            else:
                post_authors.append(post['data']['author'])
                posts_url_queue.put(create_post_url(post['data']['id']))
        else:
            pages_url_queue.put(create_next_page_url(page_json['data']['after']))
    else:
        posts_url_queue.put(IS_END)


async def get_post_pages() -> None:
    is_end = False
    tasks = []
    while not is_end:
        try:
            post_url = posts_url_queue.get(block=False)
        except Empty:
            await asyncio.sleep(0)
            continue

        if post_url != IS_END:
            tasks.append(asyncio.create_task(get_page(post_url, posts_json_queue)))
        else:
            if tasks:
                done, pending = await asyncio.wait(tasks)
            is_end = True
            posts_json_queue.put(IS_END)

        posts_url_queue.task_done()
        await asyncio.sleep(0)


async def get_comments():
    is_end = False
    tasks = []
    pending = []
    while not is_end or not posts_json_queue.empty() or (len(pending) > 0):
        if tasks:
            done, pending = await asyncio.wait(tasks, timeout=0)
        try:
            post = posts_json_queue.get(block=False)
        except Empty:
            await asyncio.sleep(0)
            continue

        if post != IS_END:
            post_id = post[POST_INDEX]['data']['children'][POST_INDEX]['data']['id']

            comments = parse_comment(post[COMMENT_INDEX]['data']['children'])

            for comment in comments:
                if comment['kind'] == 'more':
                    for comment_id in comment['data']['children']:
                        tasks.append(asyncio.create_task(get_page(create_post_url(post_id, comment_id), posts_json_queue)))

                if comment['kind'] == 't1':
                    comment_authors.append(comment['data']['author'])

        else:
            is_end = True

        posts_json_queue.task_done()
        await asyncio.sleep(0)


def parse_comment(comments: dict) -> list[dict]:
    list_of_comments = []
    for comment in comments:
        list_of_comments.append(comment)
        if comment['data']['replies']:
            list_of_comments.extend(parse_comment(comment['data']['replies']['data']['children']))

    return list_of_comments


def create_next_page_url(after: str):
    return f'{BASE_URL}/{SUBREDDIT}.json?after={after}&sort=new'


def create_post_url(post_id: str, comment_id: str = None) -> str:
    if comment_id:
        return f"https://www.reddit.com/r/{SUBREDDIT}/comments/{post_id}.json?comment={comment_id}"

    return f"https://www.reddit.com/r/{SUBREDDIT}/comments/{post_id}.json"


if __name__ == '__main__':
    main(*sys.argv)
