import urllib.request
import csv
from bs4 import BeautifulSoup

def get_html(url):
    return urllib.request.urlopen(url)

def parse(url):
    html = get_html(url)
    soup = BeautifulSoup(html, 'lxml')
    news = soup.find_all('li', class_="liga-news-item")
    results = []
    for new in news:
        title = new.find('span', class_='d-block').text.strip()
        desc = new.find('span', class_='name-dop').text
        href = new.a.get('href')
        results.append({'title': title,
                        'desc': desc,
                        'href': f'https://www.ua-football.com{href}',})
    print(results)
    return results

def save(results, path):
    with open(path, 'w', newline='', encoding='utf8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(('Название', 'Описание', 'Ссылка'))
        for result in results:
            writer.writerow((result['title'], result['desc'], result['href']))
    
    

def main():
    URL = 'https://www.ua-football.com/sport'
    results = parse(URL)
    save(results, 'ua-football_com.csv')

if __name__ == '__main__':
    main()
