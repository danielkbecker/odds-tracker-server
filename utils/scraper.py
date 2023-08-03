import requests
from bs4 import BeautifulSoup


class Scraper:
    def __init__(self):
        # self.soup = self.get_soup()
        self.headers = {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'GET',
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Max-Age': '3600',
            'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:52.0) Gecko/20100101 Firefox/52.0'
        }
        self.parser = 'html.parser'

    def get_soup(self, url):
        soup = BeautifulSoup(requests.get(url, headers=self.headers).content, self.parser)
        return soup
