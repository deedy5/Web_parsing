import json
import sqlite3
import time
import lxml.html
from random import choice
import asyncio
import aiohttp
from aiohttp_socks import ProxyConnector
from stem import Signal
from stem.control import Controller

async def tor_new_ip():
    with Controller.from_port(port = 9051) as controller:
        controller.authenticate(password="Password(not hashed)")
        controller.signal(Signal.NEWNYM)
        ip_api_list = ['https://api.my-ip.io/ip', 'https://api.ipify.org', 'https://api.ip.sb/ip',]
        ip = await fetch(choice(ip_api_list))
        print(f'Tor NEWNYM === ip: {ip}')

async def fetch(url):    
    for _ in range(3):
        try:
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; rv:68.0) Gecko/20100101 Firefox/68.0',
                       'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                       'Accept-Language': 'en-US,en;q=0.5',
                       'Accept-Encoding': 'gzip, deflate',
                       'Connection': 'keep-alive',
                       'Upgrade-Insecure-Requests': '1',}
            proxies = ['socks5://127.0.0.1:9000', 'socks5://127.0.0.1:9001', 'socks5://127.0.0.1:9002']
            connector = ProxyConnector.from_url(choice(proxies))
            async with aiohttp.request('get', url, connector=connector, headers=headers) as response:    
                print(response.status, response.url)
                if response.status == 200:
                    r = await response.text()                    
                else:
                    r = response.status
                await connector.close()
                return r
        except:
            await tor_new_ip()
            await asyncio.sleep(3)
            continue    
    return 'Error'

async def parse_habr(id):
    url = f'https://m.habr.com/kek/v2/articles/{id}'
    url2 = f'https://m.habr.com/ru/p/{id}'
    page = await fetch(url)
    if page == 403 or page == 404:
        id = url.split
        return (page, page, page, page, page, page, page, page, url2)    
    if page == 'Error':
        return ('Error', 'Error', 'Error', 'Error', 'Error', 0, 0, 0, url2)
    data = json.loads(page)
    time_row = data['timePublished']
    timestamp = ''.join({time_row[:-9].replace('T',', ')})
    title = data['titleHtml']
    text_row = data['textHtml']
    text_parsed = lxml.html.fromstring(text_row).xpath('//text()')
    text = "".join(x for x in text_parsed)
    author = data['author']['login']
    tags = ' ,'.join(tag['titleHtml'] for tag in data['tags'])
    rating = data['statistics']['score']
    views = data['statistics']['readingCount']
    comments = data['statistics']['commentsCount']
    return ((timestamp, title, text, author, tags, rating, views, comments, url2))

def save_sqlite(results):
    conn = sqlite3.connect('habr.sqlite3', check_same_thread = False)
    with conn:
        conn.executescript('PRAGMA journal_mode = wal; PRAGMA synchronous = 1;')
        conn.execute('''create table if not exists habr(id integer primary key, timestamp text, title text, text text,
                        author text, tags text, rating integer, views integer, comments integer, url text)''')
        conn.executemany('''insert into habr (timestamp, title, text, author, tags, rating, views,
                            comments, url) values (?, ?, ?, ?, ?, ?, ?, ?, ?)''', results)
    conn.close()
    print(f'SQLite3 commit {len(results)} pages')

async def main():
    start, step = 1, 10
    while start < 100:
        tasks = [asyncio.create_task(parse_habr(start+i)) for i in range(step)]
        await asyncio.gather(*tasks)
        await tor_new_ip()
        save_sqlite([task.result() for task in tasks])
        start += step
        time.sleep(3)

if __name__ == '__main__':    
    asyncio.run(main())
