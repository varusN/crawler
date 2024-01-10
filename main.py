import argparse
import asyncio
import os

import aiofiles
import aiofiles.os
from aiohttp import ClientSession, ClientTimeout, client_exceptions
from bs4 import BeautifulSoup

URL = 'https://news.ycombinator.com/'
NEWS_URL = 'https://news.ycombinator.com/item?id='


async def download_one(news_id: str, url: str, args: argparse.Namespace, limit: asyncio.Semaphore) -> str:
    news_dir = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        args.directory,
        news_id
    )
    try:
        await aiofiles.os.mkdir(news_dir)
    except FileExistsError:
        pass

    async with limit:
        try:
            print(f'Downloading url({news_id}): {url}')
            status, content = await download_content(url)
            if status == 200:
                async with aiofiles.open(f'{news_dir}/{gen_url_filename(url)}', mode = 'wb') as f:
                    await f.write(content)
        except client_exceptions.ClientConnectorError:
            print(f'Not able to connect to {url}')
        return news_id


async def get_comments_urls(news_id: str, news: dict, limit: asyncio.Semaphore) -> dict:
    comment_url = NEWS_URL + news_id
    comments = list()
    comments.append(news[0])
    urls = dict()
    async with limit:
        try:
            status, html = await download_content(comment_url)
            if status == 200:
                soup = BeautifulSoup(html.decode('utf-8'), "html.parser")
                comments_data = soup.find_all('span', class_='commtext c00')
                for data in comments_data:
                    if url := data.find('a', href=True):
                        comments.append(url['href'])
        except client_exceptions.ClientConnectorError:
            print(f'Not able to connect to {comment_url}, ignoring')
    urls[news_id] = comments
    return urls


def gen_url_filename(url: str) -> str:
    url = (url
           .replace("://", "_")
           .replace(".", "_")
           .replace("/", "_")
           .replace("?", "_")
           .replace("&", "_")
           )
    return url


async def get_news_list(html: str) -> dict:
    news = dict()
    soup = BeautifulSoup(html, "html.parser")
    news_list = soup.findAll(class_='athing')
    for data in news_list:
        url = data.find('span', class_='titleline').find('a', href=True)['href']
        news_id = data['id']
        if url.startswith('item?id='):
            url = URL + url
        news[news_id] = []
        news[news_id].append(url)
    return news


async def download_all(news: dict, args: argparse.Namespace):
    DIR = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        args.directory,
    )
    if await aiofiles.os.path.isdir(str(DIR)):
        downloaded_news = next(os.walk(str(DIR)))[1]
    else:
        print(f'Directory {DIR} does not exists')
        return
    tasks = list()
    comments = list()
    limit = asyncio.Semaphore(args.workers)
    for news_id in news.keys():
        if news_id is not None and news_id not in downloaded_news:
            comments.append(asyncio.ensure_future(get_comments_urls(news_id, news[news_id], limit=limit)))
    urls = await asyncio.gather(*comments, return_exceptions=False)
    for item in range(len(urls)):
        for news_id in urls[item].keys():
            for url in urls[item][news_id]:
                tasks.append(asyncio.ensure_future(download_one(news_id, url, args, limit=limit)))
    result = await asyncio.gather(*tasks, return_exceptions=False)
    if len(result) == 0:
        print('There is nothing new to download')
    else:
        print(f'Downloaded {len(result)} links for {len(set(result))} news')


async def download_content(url: str) -> (int, str):
    session_timeout = ClientTimeout(total=None, sock_connect=10, sock_read=10)
    try:
        async with ClientSession(timeout=session_timeout) as session:
            async with session.get(url, ssl=False) as response:
                if response.status == 200:
                    content = await response.read()
                    return response.status, content
                else:
                    return response.status, None
    except client_exceptions.ServerTimeoutError:
        pass
    return 500, None

def parse_args():
    parser = argparse.ArgumentParser(description='Basic async news crawler')
    parser.add_argument(
        '-d', '--directory', type=str, default='downloaded',
        help='Default directory to download, default - ./downloaded'
    )

    parser.add_argument(
        '-r', '--refresh', type=int, default=5,
        help='How often check for news in sec, default - 5'
    )

    parser.add_argument(
        '-w', '--workers', type=int, default=5,
        help='server workers count, default - 5'
    )
    return parser.parse_args()

async def main():
    args = parse_args()
    print(f'Crawler started to downloaded news from {URL} to ./{args.directory}, with {args.workers} workers')
    while True:
        status, html = await download_content(URL)
        if status != 200:
            print("Error fetching page")
            exit()
        news = await get_news_list(html.decode('utf-8'))
        await download_all(news, args)
        await asyncio.sleep(args.refresh)

try:
    asyncio.run(main())
except KeyboardInterrupt:
    print('Exit')
