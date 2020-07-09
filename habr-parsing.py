import time
import sqlite3
from random import choice, randrange
from concurrent.futures import ThreadPoolExecutor
import lxml.html
import requests
from fake_useragent import UserAgent

def get_proxy_list():     
    proxy_url = 'https://www.ip-adress.com/proxy_list/'     
    page = requests.get(proxy_url)      
    page = lxml.html.fromstring(page.content)    
    proxy_list = page.xpath("//tbody/tr/td[1]")
    plist = []
    for proxy in proxy_list:
        ip = proxy.xpath('./a/text()')[0]
        port = proxy.xpath('./text()')[0]
        url = f"https://{ip}{port}"
        try:
            r = requests.get('https://m.habr.com/ru', proxies = {'https': url}, timeout = 1)
            if r.status_code == 200:
                plist.append({'https': url})
        except:
            continue
    print('Proxy list finished.')
    print(plist)
    return plist

def get_html(URL, headers = None, proxies = None):
    for i in range(3):
        try:
            time.sleep(randrange(5, 10) / 10)
            r = requests.get(URL, headers = headers, proxies = proxies)
            if r.status_code == 200:
                print(f"{URL} get 200")
                return r
            if r.status_code == 404:
                print(f"{URL} get 404")
                return 404
            if r.status_code == 403:
                print(f"{URL} get 403")
                return 403
        except:
            proxies = choice(proxylist)
    return 'Error'     

def parse_habr(URL):    
    ua = UserAgent()
    headers = {
        'User-Agent': ua.random,
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'DNT': '1',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',}
    proxies = choice(proxylist)        
    page = get_html(URL, headers, proxies)
    if page == 404:
        return (("404", "404", "404", "404", "404", 404, 404, 404, "404"))
    elif page == 403:
        return (("403", "403", "403", "403", "403", 403, 403, 403, "403"))
    elif page == 'Error':
        return (("Error", "Error", "Error", "Error", "Error", 0, 0, 0, "Error"))
    try:
        root = lxml.html.fromstring(page.content)            
        timestamp = root.xpath('/html/body/div[2]/div/div[3]/main/div/div[1]/div/div[2]/article/div[1]/div[1]/span[2]/@title')[0]
        title = root.xpath('/html/body/div[2]/div/div[3]/main/div/div[1]/div/div[2]/article/div[1]/h1/text()')[0]
        text_row = root.xpath('/html/body/div[2]/div/div[3]/main/div/div[1]/div/div[2]/article/div[2]/div/div//text()')
        text = '\n'.join(x.strip('\r\n') for x in text_row if x != '\r\n')
        author = root.xpath('/html/body/div[2]/div/div[3]/main/div/div[1]/div/div[2]/article/div[1]/div[1]/span[1]/div/a/text()')[0].strip()
        tags_row = root.xpath('/html/body/div[2]/div/div[3]/main/div/div[1]/div/div[2]/article/div[3]/div[1]//text()')
        tags = ', '.join(x for x in tags_row if x != 'Теги:')
        rating = root.xpath('/html/body/div[2]/div/div[3]/main/div/div[1]/div/div[2]/div/div[1]/div/span[2]/text()')[0].strip()
        views = root.xpath('/html/body/div[2]/div/div[3]/main/div/div[1]/div/div[2]/div/span[1]/span[2]/span/text()')[0].strip()
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
        return ((timestamp, title, text, author, tags, int(rating), int(views), int(comments), URL))
    except:
        return (("Error", "Error", "Error", "Error", "Error", 0, 0, 0, "Error"))
                        
def main():       
    conn = sqlite3.connect('habr.sqlite3', check_same_thread = False)
    conn.execute('PRAGMA journal_mode = wal')
    conn.execute('''CREATE TABLE IF NOT EXISTS habr(
                        id INTEGER PRIMARY KEY, timestamp TEXT, title TEXT,
                        text TEXT, author TEXT, tags TEXT, rating INTEGER,
                        views INTEGER, comments INTEGER, url TEXT)''')
    url_gen = (f'https://m.habr.com/ru/post/{x}/' for x in range(5481, 510115))
    count = 1
    with ThreadPoolExecutor(4) as executor:
        with conn:
            for result in executor.map(parse_habr, url_gen):
                conn.execute("INSERT INTO habr(timestamp, title, text, author, tags, rating, views, comments, URL) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", result)
                if count % 10 == 0:
                    conn.commit()
                    print(f"Progress commit {count} / 510115")
                count += 1
                print(f'Progress {count}')
    conn.close()            

if __name__ == '__main__':
    proxylist = get_proxy_list()    
    main()
