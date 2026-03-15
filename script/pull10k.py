import requests
from bs4 import BeautifulSoup
from pathlib import Path
import re

BASE_DIR = Path(__file__).resolve().parent.parent
data_dir = BASE_DIR / "data" / "raw 10k"
data_dir.mkdir(parents=True, exist_ok=True)
file_path = data_dir / "tesla_2021.txt"

url = "https://www.sec.gov/Archives/edgar/data/1318605/000095017022000796/tsla-20211231.htm"

headers = {
    'User-Agent': "Tony Hurioglu academic research, tony@trhur.com",
    'Accept-Encoding': "gzip, deflate",
    'Host': "www.sec.gov"
}

response = requests.get(url, headers=headers)
response.raise_for_status()

html = response.text

soup = BeautifulSoup(html, "html.parser")

for tag in soup(["script", "style"]):
    tag.decompose()

text = soup.get_text("\n")

text = re.sub(r"\r", "\n", text)
text = re.sub(r"\n{3,}", "\n\n", text)
text = re.sub(r"[ \t]+", " ", text)

with open(file_path, "w", encoding="utf-8") as f:
    f.write(text)

print("Saved 10-K to:", file_path)