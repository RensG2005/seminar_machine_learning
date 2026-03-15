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

soup = BeautifulSoup(response.content, "html.parser")

for tag in soup(["script", "style", "header", "footer", "title"]):
    tag.decompose()

for td in soup.find_all("td"):
    td.insert_after(" ") 
for tr in soup.find_all("tr"):
    tr.insert_after("\n") 

text = soup.get_text(separator="\n")

text = text.replace("\xa0", " ")
text = text.replace("\xad", "")

text = re.sub(r"\n{3,}", "\n\n", text)
text = re.sub(r"[ \t]+", " ", text)

text = "\n".join([line.strip() for line in text.splitlines() if line.strip()])

with open(file_path, "w", encoding="utf-8") as f:
    f.write(text)

print("Saved 10-K to:", file_path)