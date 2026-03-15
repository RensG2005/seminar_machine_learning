import requests
from bs4 import BeautifulSoup
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
data_dir = BASE_DIR / "data" / "raw 10k"
data_dir.mkdir(parents=True, exist_ok=True)

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

file_path = data_dir / "tesla_2021.txt"

with open(file_path, "w", encoding="utf-8") as f:
    f.write(text)
