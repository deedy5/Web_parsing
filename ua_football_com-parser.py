import urllib.request
import csv
import time
from concurrent.futures import ThreadPoolExecutor
from bs4 import BeautifulSoup

def get_html(url):
    return urllib.request.urlopen(url)

def get_last_page(url):
    html = get_html(url)
    soup = BeautifulSoup(html, 'lxml')
    pagination = soup.find('ul', class_='pag-ul')
    lp = pagination.find_all('li')[-2].text    
    return int(lp)

def parse(url):
    html = get_html(url)
    soup = BeautifulSoup(html, 'lxml')
    news = soup.find_all('li', class_="liga-news-item")
    results = []
    for new in news:
        title = new.find('span', class_='d-block').text.strip()
        desc = new.find('span', class_='name-dop').text.strip()
        href = new.a.get('href')
        results.append({'title': title,
                        'desc': desc,
                        'href': f'https://www.ua-football.com{href}',})
    return results

def save(results, path):
    with open(path, 'w', newline='', encoding='utf8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(('Название', 'Описание', 'Ссылка'))
        for result in results:
            writer.writerow((result['title'], result['desc'], result['href']))
 

def main():
    start = time.time()
    URL = 'https://www.ua-football.com/sport'
    pages = get_last_page(URL)
    pages_gen = (f'{URL}?page={page}' for page in range(pages))
    results = []
    counter = 1
    with ThreadPoolExecutor() as executor:
        for result in executor.map(parse, pages_gen):
            print(f"Progress {counter}/{pages}")
            results.extend(result)
            counter += 1
##    results = parse(URL)
    save(results, 'ua-football_com.csv')
    print(f'Done in {time.time()-start}sec')

if __name__ == '__main__':
    main()

