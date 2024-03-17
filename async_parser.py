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
    tasks = [asyncio.create_task(coroutine()) for coroutine in (extract_pages,
                                                                extract_posts_url_and_authors,
                                                                get_post_pages,
                                                                get_comments)]
    await asyncio.gather(*tasks)

    print(Counter(post_authors).most_common()[:TOP_N])
    print(Counter(comment_authors).most_common()[:TOP_N])


async def get_page(page_url: str, purpose: Queue) -> None:
    page = await resolve_url(page_url)
    try:
        json_page = json.loads(page)
    except json.decoder.JSONDecodeError:
        print(page)
        return
    purpose.put(json_page)


async def resolve_url(page_url: str) -> str:
    async with ClientSession() as client:
        resp = await client.get(page_url)
        page = await resp.text()
    return page


async def extract_pages():
    is_end = False
    while not is_end:
        try:
            page_url = pages_url_queue.get(block=False)
        except Empty:
            await asyncio.sleep(0)
            continue

        if page_url != IS_END:
            asyncio.create_task(get_page(page_url, pages_json_queue))
        else:
            is_end = True

        pages_url_queue.task_done()
        await asyncio.sleep(0)


async def extract_posts_url_and_authors():
    is_end = False
    while not is_end:
        try:
            page = pages_json_queue.get(block=False)
        except Empty:
            await asyncio.sleep(0)
            continue

        for post in page['data']['children']:
            created = post['data']['created']
            if created < date_from:
                is_end = True
                continue
            post_authors.append(post['data']['author'])
            posts_url_queue.put(create_post_url(post['data']['id']))

        pages_url_queue.put(create_next_page_url(page['data']['after']))

        pages_json_queue.task_done()
        await asyncio.sleep(0)

    pages_url_queue.put(IS_END)
    posts_url_queue.put(f"extract_posts_url_and_authors {IS_END}")


async def get_post_pages() -> None:
    is_end = {
        f'extract_posts_url_and_authors {IS_END}': False,
        f'get_comments {IS_END}': False
    }

    tasks = []
    while not all(is_end.values()):
        try:
            post_url = posts_url_queue.get(block=False)
        except Empty:
            if posts_json_queue.empty() and tasks:
                done, pending = await asyncio.wait(tasks, timeout=0)
                if len(pending) == 0:
                    is_end[f'get_comments {IS_END}'] = True
            await asyncio.sleep(0)
            continue

        if post_url != f"extract_posts_url_and_authors {IS_END}" and post_url != f"get_comments {IS_END}":
            tasks.append(asyncio.create_task(get_page(post_url, posts_json_queue)))
        else:
            is_end[post_url] = True

        posts_url_queue.task_done()
        await asyncio.sleep(0)

    posts_json_queue.put(IS_END)


async def get_comments():
    is_end = False
    while not is_end:
        try:
            post = posts_json_queue.get(block=False)
        except Empty:
            await asyncio.sleep(0)
            continue

        if post != IS_END:
            post_id = post[0]['data']['children'][0]['data']['id']

            for comment in post[COMMENT_INDEX]['data']['children']:
                if comment['kind'] == 'more':
                    for comment_id in comment['data']['children']:
                        posts_url_queue.put(create_post_url(post_id, comment_id))

                if comment['kind'] == 't1':
                    comment_authors.append(comment['data']['author'])

        else:
            is_end = True
            posts_url_queue.put(f"get_comments {IS_END}")
            posts_json_queue.put(IS_END)

        posts_json_queue.task_done()
        await asyncio.sleep(0)


def create_next_page_url(after: str):
    return f'{BASE_URL}/{SUBREDDIT}.json?after={after}&sort=new'


def create_post_url(post_id: str, comment_id: str = None) -> str:
    if comment_id:
        return f"https://www.reddit.com/r/{SUBREDDIT}/comments/{post_id}.json?comment={comment_id}"

    return f"https://www.reddit.com/r/{SUBREDDIT}/comments/{post_id}.json"


if __name__ == '__main__':
    main(*sys.argv)
