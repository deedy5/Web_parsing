import csv
import requests
import lxml.html

def get_html(URL, headers):
    return requests.get(URL, headers=headers)

def parse_abcnews():
    URL = 'https://abcnews.go.com/international'
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:78.0) Gecko/20100101 Firefox/78.0',
               'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
               'Accept-Language': 'en-US,en;q=0.5',
               'DNT': '1',
               'Connection': 'keep-alive',
               'Upgrade-Insecure-Requests': '1',} 
    page = get_html(URL, headers)
    tree = lxml.html.fromstring(page.content)
    news = tree.xpath('//section[@class="ContentRoll__Item"]')
    results = []
    for new in news:        
        results.append({'title': new.xpath('./div[2]/h2/a/text()')[0],
                        'desc': new.xpath('./div[2]/div/div/text()')[0],
                        'date': new.xpath('./div[1]/div/text()')[0],
                        'href': new.xpath('./div[2]/h2/a/@href')[0],
                        'source': 'abcnews',})
    return results

def save_csv(file, results):
    with open(file, 'a', encoding='utf8', newline='') as csvfile:
        writer = csv.writer(csvfile)
        for r in results:
            writer.writerow((r['title'], r['desc'], r['date'], r['href'], r['source']))

def main():
    csvfile = 'abcnews.csv'
    abcnews = parse_abcnews()
    save_csv(csvfile, abcnews)
    
    
if __name__ == "__main__":
    main()
