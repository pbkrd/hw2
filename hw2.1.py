import logging

import requests as rq

from common import init_db, dump_vacancies

BASE_URL_API = 'https://api.hh.ru'

LIMIT = 100
CHUNK_SIZE = 10

def get_page_vacancies_api(session, page):
    url_vacancies = BASE_URL_API + '/vacancies'
    params = {
        'page': page,
        'per_page': min(CHUNK_SIZE, 100),
        'text': 'python middle',
        'search_field': 'name',
        'host': 'hh.ru',
    }
    vacancies = session.get(url_vacancies, params=params).json().get('items')
    return vacancies


# parse и dump
def parse_vacancy_by_id(vacancy_id):
    url = BASE_URL_API + f'/vacancies/{vacancy_id}'
    params = {'host': 'hh.ru'}
    vacancy_data = rq.get(url, params=params).json()
    try:
        skills = vacancy_data['key_skills']
        if skills:
            title = vacancy_data['name']
            company_name = vacancy_data['employer']['name']
            description = vacancy_data['description']
            skills = ', '.join(skill['name'] for skill in skills)
            logging.debug(f'Вакансия {vacancy_id} валидна')
            return title, company_name, description, skills
        logging.debug(f'Вакансия {vacancy_id} невалидна')
    except KeyError:
        logging.debug('Ошибка, поймана CAPTCHA')


def parse_chunk_vacancies(session, page):
    vacancies = get_page_vacancies_api(session, page)
    vacancies_data = [parse_vacancy_by_id(vacancy['id']) for vacancy in vacancies]
    return [*filter(None, vacancies_data)]


def main():
    connection, cursor = init_db(del_old=True)

    if connection and cursor:
        session = rq.Session()
        flag = LIMIT
        for page in range(100):
            vacancies_data = parse_chunk_vacancies(session, page)
            dump_vacancies(connection, cursor, vacancies_data)

            if (flag := flag - len(vacancies_data)) < 1:
                break

        cursor.close(), connection.close()


if __name__ == '__main__':
    main()
