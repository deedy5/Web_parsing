import sqlite3
import requests
import lxml.html

def get_html(URL, headers):
    return requests.get(URL, headers)

def parse_reuters():
    URL = 'https://reuters.com/theWire'
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:78.0) Gecko/20100101 Firefox/78.0',
               'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
               'Accept-Language': 'en-US,en;q=0.5',
               'Referer': 'https://www.reuters.com/',
               'DNT': '1',
               'Connection': 'keep-alive',
               'Upgrade-Insecure-Requests': '1',
               'Cache-Control': 'max-age=0',}
    page = get_html(URL, headers)
    tree = lxml.html.fromstring(page.content)
    news = tree.xpath('//article[contains(@class,"story ")]')
    results = []
    for new in news:
        title = new.xpath('./div[2]/a/h3/text()')[0].lstrip()
        desc = new.xpath('./div[2]/p/text()')[0]
        time =  new.xpath('./div[2]/time/span/text()')[0]
        href =  f"https://reuters.com{new.xpath('./div[2]/a/@href')[0]}"
        source =  'reuters'
        results.append((title, desc, time, href, source))            
    return results

def save_sqlite3(sqlfile, results):
    conn = sqlite3.connect(sqlfile)
    with conn:
        conn.execute('''CREATE TABLE IF NOT EXISTS news  (id INTEGER PRIMARY KEY,
                                                        title TEXT,
                                                        desc TEXT,
                                                        time TEXT,
                                                        href TEXT,
                                                        source TEXT)''')
        conn.executemany("INSERT INTO news(title, desc, time, href, source) VALUES (?, ?, ?, ?, ?)", results)
    conn.close()

def main():
    results = parse_reuters()
    sqlfile = 'reuters.sqlite3'
    save_sqlite3(sqlfile, results)
    sqlite3_check(sqlfile)


if __name__ == '__main__':
    main()
