import sqlite3
import time
from random import choice
from concurrent.futures import ThreadPoolExecutor
import requests
import lxml.html
from stem import Signal
from stem.control import Controller

def tor_new_ip():
    with Controller.from_port(port = 9051) as controller:
        controller.authenticate(password="TorTor123_")
        controller.signal(Signal.NEWNYM)

def get_html(url, headers = None, proxies = None):
    for i in range(3):
        try:
            response = requests.get(url, headers = headers, proxies = proxies)
            if response.status_code == 200:
                return response
            if response.status_code == 403:
                return 403
            if response.status_code == 404:
                return 404
        except:
            tor_new_ip()
            time.sleep(3)
            continue
    return 'Error'

def parse_habr(url):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; rv:68.0) Gecko/20100101 Firefox/68.0',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',}
    proxieslist = [{'http': 'socks5h:127.0.0.1:9000',
                   'https': 'socks5h:127.0.0.1:9000',},
                   {'http': 'socks5h:127.0.0.1:9001',
                   'https': 'socks5h:127.0.0.1:9001',},
                   {'http': 'socks5h:127.0.0.1:9002',
                   'https': 'socks5h:127.0.0.1:9002',},
                   {'http': 'socks5h:127.0.0.1:9003',
                   'https': 'socks5h:127.0.0.1:9003',},]
    ip_api_list = ['https://ipapi.co/ip', 'https://api.my-ip.io/ip', 'https://api.ipify.org', 'https://api.ip.sb/ip',]
    proxies = choice(proxieslist)
    page = get_html(url, headers, proxies)
    ip_api = choice(ip_api_list)
    print(f"get {url} ip {get_html(ip_api, headers = headers, proxies = proxies).text}") 
    if page == 403:
        return ('403', '403', '403', '403', '403', 403, 403, 403, url)
    if page == 404:
        return ('404', '404', '404', '404', '404', 404, 404, 404, url)
    if page == 'Error':
        return ('Error', 'Error', 'Error', 'Error', 'Error', 0, 0, 0, url)
    root = lxml.html.fromstring(page.content)
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
            

def main():
    sqlite3.threadsafety = 3
    conn = sqlite3.connect('habr.sqlite3', check_same_thread = False)
    conn.execute('PRAGMA journal_mode = wal')
    conn.execute('PRAGMA synchronous = 1')
    conn.execute('''create table if not exists habr(id integer primary key, timestamp text, title text, text text,
                    author text, tags text, rating integer, views integer, comments integer, url text)''')
    with ThreadPoolExecutor() as executor:
        with conn:
            url_gen = (f'https://m.habr.com/ru/p/{i}' for i in range(27120, 30000))
            counter = 1
            t0 = time.monotonic()
            for result in executor.map(parse_habr, url_gen):            
                conn.execute('''insert into habr (timestamp, title, text, author, tags, rating, views, comments,
                                url) values (?, ?, ?, ?, ?, ?, ?, ?, ?)''', result)                
                if counter % 10 == 0:
                    tor_new_ip()                    
                    conn.commit()                    
                    print(f'Commit {counter} pages in {time.monotonic()-t0} sec ***********************************')
                    time.sleep(3)
                counter += 1
        conn.close()

if __name__ == '__main__':
    main()

