import pytest

from parser import create_page_url, create_post_url
from parser import SUBREDDIT, BASE_URL


def test__create_page_url__return_correct_url():
    assert create_page_url() == f'{BASE_URL}/{SUBREDDIT}.json?sort=new'


def test__create_page_url__return_correct_url_with_after():
    assert create_page_url(after='1234567') == f'{BASE_URL}/{SUBREDDIT}.json?after=1234567&sort=new'


def test__create_post_url__return_correct_url():
    assert create_post_url(post_id=1) == f"https://www.reddit.com/r/{SUBREDDIT}/comments/1.json"


def test__create_post_url__return_correct_url_with_comment_id():
    assert create_post_url(post_id=1, comment_id=1) == f"https://www.reddit.com/r/{SUBREDDIT}/comments/1.json?comment=1"
