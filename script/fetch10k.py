import os
from edgar import Filing, set_identity, find
from pathlib import Path

#set path
BASE_DIR = Path(__file__).resolve().parent.parent
data_dir = BASE_DIR / "data" / "raw_10k"
data_dir.mkdir(parents=True, exist_ok=True)

file_path = data_dir / "tesla_2021_clean.txt"

#set identity (required)
set_identity("Tony Hurioglu tony@trhur.com")

#fetch filing
accession_number = "0000950170-22-000796"
filing = find(accession_number)

if filing:
    text_content = filing.markdown()

    with open(file_path, "w", encoding="utf-8") as f:
        f.write(text_content)

    print(f"Saved to: {file_path}")
else:
    print("Filing not found. Check the accession number.")

