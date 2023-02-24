from bs4 import BeautifulSoup
import pandas as pd
import requests
from typing import Optional
from abc import ABC, abstractmethod
import json
import re


class NewsScraper(ABC):
    def __init__(self,
                 min_date: str) -> None:
        self.min_date = min_date

        super().__init__()

    def save_scraped_df(self, path: str, zipped: bool = True) -> None:
        if zipped:
            self.scraped_df.to_csv(path, index=False, compression='zip')
        else:
            self.scraped_df.to_csv(path, index=False)

    @abstractmethod
    def scrape(self) -> pd.DataFrame:
        """Method scraping the news website."""


class RIAScraper(NewsScraper):
    def __init__(self,
                 min_date: str,
                 start_url: str) -> None:
        self.start_url = start_url

        super().__init__(min_date)

    def scrape(self) -> pd.DataFrame:
        self.scraped_df = pd.DataFrame()
        self.source_data = []

        page = 0

        scraping = True

        current_url = self.start_url

        while scraping:
            r = requests.get(current_url)

            soup = BeautifulSoup(r.text, 'lxml')

            if page == 0:
                current_url = 'https://ria.ru' + soup.find_all('div', class_='list-more')[0]['data-url']
            else:
                current_url = 'https://ria.ru' + soup.find_all('div', class_='list-items-loaded')[0]['data-next-url']

            if page == 0:
                articles = soup.find_all('div', class_='rubric-list')[0].find_all('a', class_='list-item__title')
            else:
                articles = soup.find_all('div', class_='list-items-loaded')[0].find_all('a', class_='list-item__title')

            urls = [article['href'] for article in articles]
            headlines = [article.text for article in articles]
            timestamps = [pd.to_datetime(re.search(r'\/(\d{8})\/', url)[1]) for url in urls]
            bodies = [self._get_content(url) for url in urls]

            df = pd.DataFrame({
                'url': urls,
                'timestamp': timestamps,
                'headline': headlines,
                'body': bodies
            })

            self.scraped_df = pd.concat([self.scraped_df, df])

            page += 1

            if min(timestamps) < pd.to_datetime(self.min_date):
                scraping = False

        return self.scraped_df

    def _get_content(self, url: str) -> str:
        r = requests.get(url)

        soup = BeautifulSoup(r.text, 'lxml')

        article_text = ''.join([el.text for el in soup.find_all('div', {"data-type" : "text"}, class_='article__block')])

        article_text = re.sub(r'^(.+ [-—-] РИА Новости.)', '', article_text)

        return article_text


class MeduzaScraper(NewsScraper):
    def __init__(self) -> None:
        super().__init__()

    def scrape(self) -> pd.DataFrame:
        pass


class CNNScraper(NewsScraper):
    API_URL = 'https://search.api.cnn.com/content?sort=newest&types=article'

    def __init__(self,
                 query: str,
                 min_date: str,
                 items_per_page: int = 50,
                 api_url: Optional[str] = None) -> None:
        self.query = query
        self.items_per_page = items_per_page
        self.api_url = api_url if api_url is not None else self.API_URL

        super().__init__(min_date)

    def scrape(self) -> pd.DataFrame:
        self.scraped_df = pd.DataFrame()
        self.source_data = []

        current_index = 0

        scraping = True

        while scraping:
            params = {
                'q': self.query,
                'size': self.items_per_page,
                'from': current_index
            }

            r = requests.get(self.api_url, params=params)

            result = r.json()['result']
            self.source_data.append(result)

            df = pd.DataFrame({
                'url': [item['url'] for item in result],
                'timestamp': pd.to_datetime([item['firstPublishDate'] for item in result]),
                'headline': [item['headline'] for item in result],
                'body': [item['body'] for item in result]
            })

            df = df[df['timestamp'] >= self.min_date]

            if len(df) > 0:
                current_index += self.items_per_page
            else:
                scraping = False

            self.scraped_df = pd.concat([self.scraped_df, df])

        return self.scraped_df


class FOXScraper(NewsScraper):
    API_URL = 'https://www.foxnews.com/api/article-search?searchBy=tags&excludeBy=tags&excludeValues='

    def __init__(self,
                 query: str,
                 min_date: str,
                 items_per_page: int = 30,
                 api_url: Optional[str] = None) -> None:
        self.query = query
        self.items_per_page = items_per_page
        self.api_url = api_url if api_url is not None else self.API_URL

        super().__init__(min_date)

    def _get_content(self, url: str) -> str:
        r = requests.get(url)

        # '<script data-n-head="ssr" type="application\/ld\+json">\n      ({\n        "@context": "http:\/\/schema.org".\n        "@type": "NewsArticle".+?)<\/script>'

        regex = (r'<script data-n-head="ssr" type="application\/ld\+json">\n      '
                 r'({\n        "@context": "http:\/\/schema.org".'
                 r'\n        "@type": "NewsArticle".+?)<\/script>')

        article_json = json.loads(re.search(regex, r.text, re.DOTALL)[1])

        return article_json['articleBody']

    def scrape(self) -> pd.DataFrame:
        self.scraped_df = pd.DataFrame()

        current_index = 0

        scraping = True

        while scraping:
            params = {
                'values': self.query,
                'size': self.items_per_page,
                'from': current_index
            }

            r = requests.get(self.api_url, params=params)

            result = r.json()

            result = list(filter(lambda i: i['category']['name'] != 'VIDEO', result))

            urls = ['https://foxnews.com' + item['url'] for item in result]
            timestamps = [pd.to_datetime(item['publicationDate']) for item in result]
            headlines = [item['title'] for item in result]
            bodies = [self._get_content(url) for url in urls]

            df = pd.DataFrame({
                'url': urls,
                'timestamp': timestamps,
                'headline': headlines,
                'body': bodies
            })

            df = df[df['timestamp'] >= pd.to_datetime(self.min_date).tz_localize('utc')]

            if len(df) > 0:
                current_index += self.items_per_page
            else:
                scraping = False

            self.scraped_df = pd.concat([self.scraped_df, df])

        return self.scraped_df
