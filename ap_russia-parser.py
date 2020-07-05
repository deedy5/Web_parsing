import csv
from time import time
import lxml.html
import requests

def get_html(URL, headers):
    return requests.get(URL, headers)

def parse_ap_russia():
    URL = 'https://apnews.com/Russia'
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:78.0) Gecko/20100101 Firefox/78.0',
               'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
               'Accept-Language': 'en-US,en;q=0.5',
               'DNT': '1',
               'Connection': 'keep-alive',
               'Upgrade-Insecure-Requests': '1',}
    t0 = time()
    page = get_html(URL, headers)
    print(f'Page downloaded in {time()-t0:.2f}s')
    t0 = time()
    tree = lxml.html.fromstring(page.content)
    news = tree.xpath('//article[contains(@class,"feed")]//div[contains(@class,"FeedCard")]')
    results = []
    for new in news:
        results.append({'title': new.xpath('./div[1]/a/h1/text()')[0],
                        'desc' : new.xpath('./a/div/p/text()')[0],
                        'href': f"https://apnews.com{new.xpath('./div/a/@href')[0]}",
                        'data': new.xpath('./div[1]/div[1]/span/@data-source')[0].rstrip('Z').replace('T',' '),
                        'source': 'ap',})
    print(f'Page parsed in {time()-t0:.2f}s')
    return results

def save_csv(file, results):
    t0 = time()
    with open(file, 'w', encoding='utf8', newline='') as csvfile:
        writer = csv.writer(csvfile)
        for r in results:
            writer.writerow((r['title'], r['desc'], r['href'], r['data'], r['source']))
    print(f'Page saved in {file} in {time()-t0:.2f}s')

def main():
    csvfile = 'ap_russia.csv'
    results = parse_ap_russia()
    save_csv(csvfile, results)


if __name__ == '__main__':
    main()
