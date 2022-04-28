import os
from time import sleep
from typing import List, Tuple
from bs4 import BeautifulSoup
import requests


URL =  "https://environment.data.gov.uk/water-quality/view/download"
BASE_URL = 'https://environment.data.gov.uk'
FILE_NAME = 0
FILE_URL = 1
FILES_PATH = 'files_cambridgeshire_bedfordshire'


def get_all_monitorig_datasets_links() -> List[Tuple['str', 'str']]:
    resp = requests.get(URL)
    page = BeautifulSoup(resp.text, 'html.parser')
    all_of_eng = page.find('h2', text='Cambridgeshire and Bedfordshire')
    a_tags = all_of_eng \
        .find_next('table') \
        .find_all('tr')[1] \
        .find_all('a')

    links = list(map(lambda x:  (x.text, f"{BASE_URL}{x.attrs['href']}"), a_tags))

    return links


def download_files(links: List[Tuple['str', 'str']]) -> None:
    if not os.path.exists(FILES_PATH):
        os.mkdir(FILES_PATH)

    for link in links:
        resp = requests.get(link[FILE_URL])
        with open(f'{FILES_PATH}/{link[FILE_NAME]}', 'w', newline='') as f:
            f.write(resp.text)
        sleep(5)



if __name__ == '__main__':
    links = get_all_monitorig_datasets_links()
    download_files(links)