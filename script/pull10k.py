import requests
from bs4 import BeautifulSoup

url = "https://www.sec.gov/Archives/edgar/data/1318605/000095017022000796/tsla-20211231.htm"

headers = {
    'User-Agent': "Tony Hurioglu academic research, tony@trhur.com",
    'Accept-Encoding': "gzip, deflate",
    'Host': "www.sec.gov"
}

response = requests.get(url, headers=headers)
html = response.text

soup = BeautifulSoup(html, "html.parser")
text = soup.get_text(" ", strip=True)

