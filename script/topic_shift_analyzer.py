import pandas as pd
from pathlib import Path
from bertopic import BERTopic
from sentence_transformers import SentenceTransformer

BASE_DIR = Path(__file__).resolve().parent.parent
data_dir = BASE_DIR / "data" / "raw_10k"
docs = []
timestamps = []
metadata = []

for file in data_dir.glob("*_strategy.txt"):
    try:
        parts = file.name.split("_")
        ticker = parts[0]
        year = int(parts[1])

        content = file.read_text(encoding="utf-8")

        paragraphs = [str(p).strip() for p in content.split("\n\n") if len(p.strip()) > 100]

        for p in paragraphs:
            if p and p.lower() != "none":
                docs.append(p)
                timestamps.append(year)

    except Exception as e:
        print(f"Skipping file {file.name} due to error: {e}")

docs = [str(d) for d in docs if isinstance(d, str) and len(d) > 10]

if not docs:
    print("Error: No documents found")
    exit()

embedding_model = SentenceTransformer("all-mpnet-base-v2")

topic_model = BERTopic(
    embedding_model=embedding_model,
    nr_topics="auto",
    min_topic_size=10,
    verbose=True
)

topics, probs = topic_model.fit_transform(docs)

topics_over_time = topic_model.topics_over_time(docs, timestamps)

output_path = BASE_DIR / "strategy_topics_evo.csv"
topics_over_time.to_csv(output_path, index=False)

