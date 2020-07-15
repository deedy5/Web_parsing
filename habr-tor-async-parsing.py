import sqlite3
import time
from random import choice
import asyncio
import aiohttp
from aiohttp_socks import ProxyConnector
import lxml.html
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
                elif response.status == 403:
                    r = 403
                elif response.status == 404:
                    r = 404
                await connector.close()
                return r
        except:
            continue    
    return 'Error'

async def parse_habr(url):
    page = await fetch(url)
    if page == 403:
        return ('403', '403', '403', '403', '403', 403, 403, 403, url)
    if page == 404:
        return ('404', '404', '404', '404', '404', 404, 404, 404, url)
    if page == 'Error':
        return ('Error', 'Error', 'Error', 'Error', 'Error', 0, 0, 0, url)
    root = lxml.html.fromstring(page)
    timestamp = root.xpath('/html/body/div[2]/div/div[3]/main/div/div/div/div[2]/article/div[1]/div[1]/span[2]/@title')[0]
    title = root.xpath('/html/body/div[2]/div/div[3]/main/div/div/div/div[2]/article/div[1]/h1/text()')[0]
    text_row = root.xpath('/html/body/div[2]/div/div[3]/main/div/div/div/div[2]/article/div[2]//text()')
    text = '\n'.join(x.strip('\r\n') for x in text_row if x != '\r\n')
    author = root.xpath('/html/body/div[2]/div/div[3]/main/div/div/div/div[2]/article/div[1]/div[1]/span[1]/div/a/text()')[0].strip()
    tags_row = root.xpath('/html/body/div[2]/div/div[3]/main/div/div/div/div[2]/article/div[3]/div[1]//text()')
    tags = ', '.join(x for x in tags_row if x != 'Теги:')
    rating = root.xpath('/html/body/div[2]/div/div[3]/main/div/div/div/div[2]/div/div[1]/div/span[2]/text()')[0].strip()
    views = root.xpath('/html/body/div[2]/div/div[3]/main/div/div/div/div[2]/div/span[1]/span[2]/span/text()')[0].strip()
    if views[-1:] == 'k':
        if ',' in views:
            a ,b = views[:-1].split(',')
            views = int(a) * 1000 + int(b) * 100
        else:
            views = int(views[:-1]) * 1000
    try:
        comments = root.xpath('/html/body/div[2]/div/div[3]/main/div/div[1]/div/div[3]/a/div/text()')[0].split()[1]        
    except:
        comments = 0        
    return ((timestamp, title, text, author, tags, int(rating), int(views), int(comments), url))

def save_sqlite(results):
    sqlite3.threadsafety = 3
    conn = sqlite3.connect('habr.sqlite3', check_same_thread = False)
    with conn:
        conn.execute('PRAGMA journal_mode = wal')
        conn.execute('PRAGMA synchronous = 1')
        conn.execute('''create table if not exists habr(id integer primary key, timestamp text, title text, text text,
                        author text, tags text, rating integer, views integer, comments integer, url text)''')
        conn.executemany('''insert into habr (timestamp, title, text, author, tags, rating, views,
                            comments, url) values (?, ?, ?, ?, ?, ?, ?, ?, ?)''', results)
    conn.close()
    print(f'SQLite3 commit {len(results)} pages')

async def main():
    start, step = 1, 10
    while True:
        tasks = []
        for i in range(step):
            url = f'https://m.habr.com/ru/p/{start + i}'
            task = asyncio.create_task(parse_habr(url))
            tasks.append(task)                    
        await asyncio.gather(*tasks)
        await tor_new_ip()
        results = [task.result() for task in tasks]
        save_sqlite(results)
        start += step
        time.sleep(3)
    

if __name__ == '__main__':    
    asyncio.run(main())
