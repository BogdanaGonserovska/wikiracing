from typing import List
from bs4 import BeautifulSoup
import requests
from urllib.parse import unquote
import psycopg2
import time
from ratelimit import limits, sleep_and_retry

requests_per_minute = 100
links_per_page = 200
one_minute = 60


class WikiRacer:

    def __init__(self):
        self.conn = psycopg2.connect(dbname='postgres', user='postgres', password='postgres', host='localhost')
        self.cursor = self.conn.cursor()
        self.cursor.execute('CREATE TABLE IF NOT EXISTS links (from_link VARCHAR(255), to_link VARCHAR(255))')
        self.conn.commit()


    def __filter(self, link: str) -> bool:
        words = [
            'Користувач:', 'Вікіпедія:', 'Файл:', 'MediaWiki:', 'Шаблон:',
            'Довідка:', 'Категорія:', 'Портал:', 'Модуль:', 'Додаток:', 'Спеціальна:'
        ]
        for w in words:
            if link.startswith(w):
                return False
        return True


    @sleep_and_retry
    @limits(calls=requests_per_minute, period=one_minute)
    def __request(self, link: str) -> requests.models.Response:
        to_link = f'https://uk.wikipedia.org/wiki/{link}'
        return requests.get(to_link)


    def __fill(self, to: str) -> List[str]:
        temp = to.replace("'", "''") if "''" not in to else to
        self.cursor.execute(f"SELECT * FROM links WHERE from_link='{temp}'")
        if self.cursor.fetchall():
            return []

        response = self.__request(to)

        soup = BeautifulSoup(response.text, 'html.parser')
        div = soup.find("div", {"class": "mw-parser-output"})
        links = div.find_all("a")
        count = 0
        for l in links:
            try:
                if l['href'].startswith('/wiki'):
                    link = unquote(l['href']).lstrip('/wiki/')
                    if self.__filter(link):
                        to = to.replace("'", "''") if "''" not in to else to
                        link = link.replace("'", "''") if "''" not in link else link
                        
                        self.cursor.execute(f"INSERT INTO links (from_link, to_link) VALUES ('{to}', '{link}')")
                        self.conn.commit()
                        if to == 'Дружина_(військо)':
                            print(to, link)
                        if to == 'Друга_світова_війна':
                            print(to, link)
                        count += 1
                if count == links_per_page:
                    break
            except KeyError:
                continue


    def find_path(self, start: str, finish: str) -> List[str]:
        
        start = start.replace(' ', '_')
        finish = finish.replace(' ', '_')
        if start == finish:
            return [start, finish]

        queue = []
        visited = []
        visited.append(start)
        queue.append((start, [start.replace('_', ' ')]))
        timeout = time.time() + 1500

        while queue and time.time() < timeout:
            node, path = queue.pop(0)
            
            self.__fill(node)
            node = node.replace("'", "''") if "''" not in node else node
            self.cursor.execute(f"SELECT to_link FROM links WHERE from_link='{node}'")
            links = self.cursor.fetchall()
            
            for l in links:
                link = l[0]
                if link == finish:
                    return path + [finish.replace('_', ' ')]
                if link not in visited:
                    visited.append(link)
                    queue.append((link, path + [link.replace('_', ' ')]))

        return []


if __name__ == '__main__':
    start_link = 'Степан (кіт)'
    end_link = 'Патрон (пес)'
    racer = WikiRacer()
    print(racer.find_path(start_link, end_link))
