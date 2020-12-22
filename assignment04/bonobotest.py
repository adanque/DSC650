
import ssl
import bonobo
import requests
from bs4 import BeautifulSoup
from fake_useragent import UserAgent

ssl._create_default_https_context = ssl._create_unverified_context
def scrape_zillow():
    price = ''
    status = ''
    """
    #headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
    #headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
    ua = UserAgent()
    hdr = {'User-Agent': ua.random,
           'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
           'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.3',
           'Accept-Encoding': 'none',
           'Accept-Language': 'en-US,en;q=0.8',
           'Connection': 'keep-alive'}
    url = 'https://www.zillow.com/homedetails/340-Huntsville-Idetown-Rd-Dallas-PA-18612/2078882377_zpid/'
    r = requests.get(url, headers=hdr)
    if r.status_code == 200:
        #print(r.status_code)
        html = r.text.strip()
        soup = BeautifulSoup(html, 'lxml')
        print(soup)
        htag = soup.findall('h4')
        for h in htag:
            print(h.text)

        #soup = BeautifulSoup(html)
        #print(soup)
        #price_status_section = soup.select('.home-summary-row')
        price_status_section = soup.find_all("H4", id=lambda value: value and value.startswith("Text-c11n-8-18-0__aiai24-0 StyledHeading-c11n-8-18-0__ktujwe-0 gcaUyc hdp__qf5kuj-3 czKGOp"))
        #price_status_section = soup.select("div[class*=ds-summary-row]")  # with *= means: contains
        #soup.findAll("div", {"class": "stylelistrow"})
        #ds - summary - row
        #price_status_section = soup.find('span', {'class' : 'Text-c11n-8-18-0__aiai24-0 einFCw'})
        #print(soup.select('Text-c11n-8-18-0__aiai24-0 einFCw'))
        print(price_status_section)
        if len(price_status_section) > 1:
            price = price_status_section[1].text.strip()
    print("PRICE")
    print(price)
    """
    price="2,314"
    return price


def scrape_redfin():
    price = ''
    status = ''
    url = 'https://www.redfin.com/TX/Dallas/2619-Colby-St-75204/unit-B/home/32251730'
    r = requests.get(url, headers=headers)
    if r.status_code == 200:
        html = r.text.strip()
        soup = BeautifulSoup(html, 'lxml')
        price_section = soup.find('span', {'itemprop': 'price'})
        if price_section:
            price = price_section.text.strip()
        print(price)
    price="3,244"
    return price


def extract():
    yield scrape_zillow()
    yield scrape_redfin()


def transform(price: str):
    print(price)
    t_price = price.replace(',', '').lstrip('$')
    return float(t_price)


def load(price: float):
    with open('pricing.txt', 'a+', encoding='utf8') as f:
        f.write((str(price) + '\n'))


if __name__ == '__main__':
    headers = {
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36',
        'referrer': 'https://google.com'
    }
    # scrape_redfin()
    graph = bonobo.Graph(
        extract,
        transform,
        load,
    )
    bonobo.run(graph)