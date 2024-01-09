import asyncio
import os
from bs4 import BeautifulSoup
import aiofiles
from aiohttp import ClientSession, client_exceptions

URL = 'https://news.ycombinator.com/'
NEWS_URL = 'https://news.ycombinator.com/item?id='
DIR = './downloaded/'
SESSIONS = 10


async def download_one(id, url, session, limit):
    news_dir = DIR + id + '/'
    try:
        os.mkdir(news_dir)
    except:
        pass
    async with limit:
        try:
            print(f'Downloading url({id}): {url}')
            status, content = await download_content(url)
            if status == 200:
                async with aiofiles.open(f'{news_dir}/{gen_url_filename(url)}', mode = 'wb') as f:
                    await f.write(content)
        except client_exceptions.ClientConnectorError:
            pass # TODO log error
        return id


def gen_url_filename(url):
    return url.replace("://", "_").replace(".", "_").replace("/", "_").replace("?", "_").replace("&", "_")


async def download(news):
    downloaded_news = next(os.walk(DIR))[1]
    tasks = list()
    limit = asyncio.Semaphore(SESSIONS)
    async with ClientSession() as session:
        for id in news.keys():
            if id is not None and id not in downloaded_news:
                for url in news[id]:
                    print(url)
                    tasks.append(asyncio.ensure_future(download_one(id, url, session=session, limit=limit)))
        result = await asyncio.gather(*tasks, return_exceptions=False)
        if len(result) == 0:
            print('There is nothing new to download')
        else:
            print(f'Downloaded {len(result)} news')

async def get_news_list(html):
    news = dict()
    soup = BeautifulSoup(html, "html.parser")
    news_list = soup.findAll(class_='athing')
    for data in news_list:
        url = data.find('span', class_='titleline').find('a', href=True)['href']
        id = data['id']
        if url.startswith('item?id='):
            url = URL + url
        news[id] = []
        news[id].append(url)
        comments = await get_comments_list(id)
        if len(comments) > 0:
            news[id] = news[id] + comments
        print(news)
    return news


async def get_comments_list(id):
    comments = []
    url = NEWS_URL + id
    status, html = await download_content(url)
    if status == 200:
        soup = BeautifulSoup(html.decode('utf-8'), "html.parser")
        comments_data = soup.find_all('span', class_='commtext c00')
        for data in comments_data:
            if url := data.find('a', href=True):
                comments.append(url['href'])
    return comments


async def download_content(url):
    async with ClientSession() as session:
        async with session.get(url, ssl=False) as response:
            if response.status == 200:
                content = await response.read()
                return response.status, content
            else:
                return response.status, None


async def main():
    while True:
        status, html = await download_content(URL)
        if status != 200:
            print("Error fetching page")
            exit()
        news = await get_news_list(html.decode('utf-8'))
        print(news)
        await download(news)
        await asyncio.sleep(5)

asyncio.run(main())
