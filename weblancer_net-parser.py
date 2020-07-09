import csv
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from bs4 import BeautifulSoup

def get_html(url):
    response = urllib.request.urlopen(url)
    time.sleep(1)
    return response.read()

def get_page_count(url):
    soup = BeautifulSoup(get_html(url), 'lxml')
    lp = soup.find('div', {'class': 'col-1 col-sm-2 text-right'}).find('a').get('href')
    r = int(''.join(x for x in lp if str.isdigit(x)))
    return r

def parse(url):
    response = urllib.request.urlopen(url)
    html = response.read()
    soup = BeautifulSoup(html, 'lxml')
    rows = soup.find_all('div', class_='row click_container-link set_href')
    projects = []    
    for row in rows:
        title = row.find('a', class_='text-bold show_visited').text
        category = row.find('a', class_='text-muted').text.replace(u'\xa0', u' ')
        price = row.find('div', class_='float-right float-sm-none title amount indent-xs-b0').text
        application = row.find('div', class_='float-left float-sm-none text_field').text.strip().split()[0]
        projects.append({'title': title,
                         'category': category,
                         'price': price,
                         'application': application,})
    return projects


def save(projects, path):
    with open(path, 'w', newline='', encoding='utf8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(('Проект', 'Категории', 'Цена', 'Заявки'))
        for project in projects:
            writer.writerow((project['title'], project['category'], project['price'], project['application']))
            

def main():
    BASE_URL = 'https://weblancer.net/jobs/'
    start = time.time()
    page_count = get_page_count(BASE_URL)
    urls_gen = (f'{BASE_URL}?page={page}' for page in range(page_count))
    projects = []
    counter = 1
    with ThreadPoolExecutor() as executor:
        for result in executor.map(parse, urls_gen, chunksize=15):
            print(f'Progress {counter}/{page_count}')            
            projects.extend(result)
            counter += 1
    print(f'Done in {time.time()-start} sec.')
    save(projects, 'weblancer_net.csv')
    
if __name__ == '__main__':
    main()
