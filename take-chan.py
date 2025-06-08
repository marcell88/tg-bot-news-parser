import typing
import requests
from bs4 import BeautifulSoup

def scrapeDataFromSpreadsheet() -> typing.List[typing.List[str]]:
    html = requests.get('https://docs.google.com/spreadsheets/d/1o8QeMOKWm4VeZ9VecgnL8BWaOlX5kdCDkXoAph37sQM/edit#gid=1691373423').text
    soup = BeautifulSoup(html, 'lxml')
    salas_cine = soup.find_all('table')[0]
    rows = [[td.text for td in row.find_all("td")] for row in salas_cine.find_all('tr')]
    return rows