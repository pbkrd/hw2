import json
import logging

import requests as rq

from bs4 import BeautifulSoup

from common import init_db, dump_vacancies

LIMIT = 100

BASE_URL_WEB = 'https://hh.ru/search/vacancy'
AREAS = 1, 2, 3, 4
PARAMS = {
    'text': 'middle python',
    'search_field': 'name',
    'area': AREAS,
}
HEADERS = {'User-agent': 'Mozilla/5.0'}


def parse_vacancy(session, url):
    response = session.get(url, headers=HEADERS)
    soup = BeautifulSoup(response.text, 'lxml')

    skills = soup.find_all('div', attrs={'data-qa': 'bloko-tag bloko-tag_inline skills-element'})
    if skills:
        skills = ', '.join(skill.text for skill in skills)
        title = soup.find('h1', attrs={'data-qa': 'vacancy-title'}).text
        company_name = soup.find('div', attrs={'data-qa': 'vacancy-company__details'}).text
        description = soup.find('div', attrs={'data-qa': 'vacancy-description'})
        description = description.text if description else None

        return title, company_name, description, skills


def get_page_vacancies_web(session, page):
    params = PARAMS | {'page': page}
    response = session.get(BASE_URL_WEB, headers=HEADERS, params=params)
    soup = BeautifulSoup(response.text, 'lxml')
    # 20 основных
    links = soup.find_all('a', attrs={'data-qa': 'serp-item__title'})
    vacancy_urls = [link.attrs.get('href') for link in links]

    # extra by template
    data = json.loads(soup.find('template', attrs={'id': 'HH-Lux-InitialState'}).text)
    extra_urls = [vacancy.get('links').get('desktop') for vacancy in data['vacancySearchResult']['vacancies']]
    vacancy_urls.extend(extra_urls)

    vacancies_data = [parse_vacancy(session, url) for url in vacancy_urls if url]
    return [*filter(None, vacancies_data)]


def main():
    connection, cursor = init_db(del_old=True)

    if connection and cursor:
        session = rq.Session()
        flag = LIMIT
        for page in range(100):
            vacancies_data = get_page_vacancies_web(session, page)
            dump_vacancies(connection, cursor, vacancies_data)

            if not vacancies_data:
                break

            if (flag := flag - len(vacancies_data)) < 1:
                break

        session.close()
        cursor.close(), connection.close()


if __name__ == '__main__':
    main()
