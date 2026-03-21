from pypdf import PdfReader
from sec_edgar_downloader import Downloader
import re
import csv
import os
import pandas as pd


def esg_words(pdf_file, raw_text_file, csv_file):
    """
    Extract ESG dictionary words from Baier et al. Table 3 and save them to CSV.
    """
    reader = PdfReader(pdf_file)
    text = ""

    # Extract pages 12–15 - ESG word list is on these pages
    for i in range(11, 15):
        page = reader.pages[i]
        page_text = page.extract_text()

        if page_text:  # successful extraction check
            text += "\n" + page_text

    # Save raw text
    with open(raw_text_file, "w", encoding="utf-8") as f:
        f.write(text)
    print("Saved raw text to:", raw_text_file)

    # Fix spaced words
    text = re.sub(
        r"\b(?:[A-Za-z]\s+){2,}[A-Za-z]\b",
        lambda m: m.group(0).replace(" ", ""),
        text
    )

    # Clean formatting
    text = text.replace("\n", " ")
    text = re.sub(r"\s+", " ", text)

    junk_patterns = [
        r"BAIER ET AL\s*\.?",
        r"TABLE 3 \(Continued\)",
        r"TABLE 3 ESG word list",
        r"Topic Category Subcategory",
        r"\ba\b"
    ]

    for pattern in junk_patterns:
        text = re.sub(pattern, " ", text, flags=re.IGNORECASE)
    text = re.sub(r"\s+", " ", text).strip()

    # Isolate dictionary
    start = text.find("Governance:")
    end = text.find("unemployment")

    if end == -1:
        end = len(text)
    else:
        end += len("unemployment")

    text = text[start:end]

    # Extract rows
    pattern = r"([A-Za-z&\-\s]+?):\s*(.*?)(?=\s+[A-Za-z&\-\s]+?:|$)"
    matches = re.findall(pattern, text)

    rows = []
    bad_words = {"", "-", "table", "continued", "topic", "category", "subcategory"}

    for section, words_block in matches:
        section = re.sub(r"\s+", " ", section).strip()
        words = words_block.split(",")

        for word in words:
            word = word.strip().lower()
            word = re.sub(r"[^a-z\- ]", "", word)
            word = re.sub(r"\s+", " ", word).strip()

            if "downloaded" in word or "wiley" in word or "library" in word:
                continue

            if word in bad_words:
                continue

            if len(word) <= 1:
                continue

            rows.append((section, word))

    rows = sorted(set(rows))

    # Save CSV
    with open(csv_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["subcategory", "word"])
        writer.writerows(rows)

    print("Saved CSV to:", csv_file)
    print("Total ESG word rows extracted:", len(rows))


def lm_words(master_csv, output_csv):
    """
    Extract selected sentiment categories from the
    Loughran-McDonald Master Dictionary and save to CSV.
    """
    df = pd.read_csv(master_csv)

    categories = [
        c for c in [
            "Negative",
            "Positive",
            "Uncertainty",
            "Litigious",
            "Constraining"
        ] if c in df.columns
    ]

    # keep only rows where Word exists
    df = df[df["Word"].notna()].copy()
    df["Word"] = df["Word"].astype(str).str.lower().str.strip()

    rows = []

    for cat in categories:
        temp = df[df[cat] > 0][["Word"]].copy()
        temp["category"] = cat
        temp = temp[["category", "Word"]]
        rows.append(temp)

    result = pd.concat(rows, ignore_index=True).drop_duplicates()
    result = result.rename(columns={"Word": "word"})

    result.to_csv(output_csv, index=False)

    print("Saved LM dictionary to:", output_csv)
    print("Total words extracted:", len(result))


def dictionary_score(text, dictionary_csv, word_column="word"):
    """
    Count dictionary word matches in a text.
    Works for ESG or Loughran-McDonald dictionaries.
    """
    df = pd.read_csv(dictionary_csv)
    words = set(df[word_column].dropna().str.lower())

    text = text.lower()
    tokens = re.findall(r"[a-z\-]+", text)

    total_words = len(tokens)
    matches = sum(1 for w in tokens if w in words)
    score = matches / total_words if total_words > 0 else 0

    return matches, total_words, score


def download_10k(save_path, ticker, company_name, email, limit=1):
    """
    Download the latest 10-K filing(s) for a ticker.
    """
    dl = Downloader(company_name, email, save_path)
    dl.get("10-K", ticker, limit=limit)
    print(f"Downloaded {limit} 10-K filing(s) for {ticker} to {save_path}")


def load_latest_10k(base_path, ticker="AAPL"):
    base = os.path.join(base_path, "sec-edgar-filings", ticker, "10-K")

    folders = os.listdir(base)
    folders.sort(reverse=True)

    latest_folder = folders[0]
    file_path = os.path.join(base, latest_folder, "full-submission.txt")

    with open(file_path, encoding="utf-8") as f:
        return f.read()


def dictionary_scores(text, dictionary_csv, category_column, word_column="word"):
    """
    Compute:
    - total dictionary score
    - category-specific scores

    Returns a dictionary with:
    - total_matches
    - total_words
    - total_score
    - category_scores
    """
    df = pd.read_csv(dictionary_csv)

    # clean dictionary
    df = df[df[word_column].notna()].copy()
    df[word_column] = df[word_column].astype(str).str.lower().str.strip()

    # clean text
    text = text.lower()
    tokens = re.findall(r"[a-z\-]+", text)
    total_words = len(tokens)

    # total dictionary words
    all_words = set(df[word_column])
    total_matches = sum(1 for token in tokens if token in all_words)
    total_score = total_matches / total_words if total_words > 0 else 0

    # category scores
    category_scores = {}

    for category in df[category_column].dropna().unique():
        category_words = set(df[df[category_column] == category][word_column])
        matches = sum(1 for token in tokens if token in category_words)
        score = matches / total_words if total_words > 0 else 0

        category_scores[category] = {
            "matches": matches,
            "score": score
        }

    return {
        "total_matches": total_matches,
        "total_words": total_words,
        "total_score": total_score,
        "category_scores": category_scores
    }


def main():
    PDF_FILE = r"C:\Users\DELL\Desktop\ERASMUS\FML\project\Baier.pdf"
    RAW_TEXT_FILE = r"C:\Users\DELL\Desktop\ERASMUS\FML\project\raw_esg_text.txt"
    CSV_FILE = r"C:\Users\DELL\Desktop\ERASMUS\FML\project\esg_dictionary_baier.csv"

    esg_words(PDF_FILE, RAW_TEXT_FILE, CSV_FILE)

    LM_MASTER = r"C:\Users\DELL\Desktop\ERASMUS\FML\project\Loughran-McDonald_MasterDictionary_1993-2024.csv"
    LM_OUTPUT = r"C:\Users\DELL\Desktop\ERASMUS\FML\project\lm_dictionary.csv"
    lm_words(LM_MASTER, LM_OUTPUT)

    DOWNLOAD_PATH = r"C:\Users\DELL\Desktop\ERASMUS\FML\project"
    TICKER = "AAPL"

    download_10k(
        save_path=DOWNLOAD_PATH,
        ticker=TICKER,
        company_name="YourNameOrSchoolProject",
        email="your_email@example.com",
        limit=1
    )

    # FIXED: ESG topic mapping belongs here, after CSV_FILE exists
    esg_df = pd.read_csv(CSV_FILE)

    def assign_topic(subcat):
        governance = [
            "Governance",
            "Corporate governance",
            "Audit and control",
            "Board structure",
            "Remuneration",
            "Shareholder rights",
            "Transparency",
            "Talent",
            "Business ethics",
            "Bribery and corruption",
            "Political influence",
            "Responsible marketing",
            "Whistle-blowing system",
            "Sustainability management and reporting",
            "Disclosure and reporting",
            "Governance of sustainability issues",
            "Stakeholder engagement",
            "UNGC compliance"
        ]

        environmental = [
            "Environmental",
            "Climate change",
            "Biofuels",
            "Climate change strategy",
            "Emissions management and reporting",
            "Ecosystem Service",
            "Access to land",
            "Biodiversity management",
            "Water",
            "Environmental management",
            "Environmental standards",
            "Pollution control",
            "Product opportunities",
            "Supply chain environmental standards",
            "Waste and recycling"
        ]

        social = [
            "Social",
            "Public health",
            "Access to medicine",
            "HIV and AIDS",
            "Nutrition",
            "Product safety",
            "Human rights",
            "Community relations",
            "Privacy and free expression",
            "Security",
            "Weak governance zones",
            "Labor standards",
            "Diversity",
            "Health and safety",
            "ILO core conventions",
            "Supply chain labor standards",
            "Society",
            "Charity",
            "Education",
            "Employment"
        ]

        if subcat in governance:
            return "Governance"
        elif subcat in environmental:
            return "Environmental"
        elif subcat in social:
            return "Social"
        else:
            return None

    esg_df["topic"] = esg_df["subcategory"].apply(assign_topic)
    esg_df = esg_df[esg_df["topic"].notna()]

    ESG_TOPIC_FILE = r"C:\Users\DELL\Desktop\ERASMUS\FML\project\esg_topic_dictionary.csv"
    esg_df[["topic", "word"]].to_csv(ESG_TOPIC_FILE, index=False)

    text = load_latest_10k(
        r"C:\Users\DELL\Desktop\ERASMUS\FML\project",
        "AAPL"
    )

    matches, total, score = dictionary_score(
        text,
        r"C:\Users\DELL\Desktop\ERASMUS\FML\project\esg_dictionary_baier.csv"
    )

    lm_results = dictionary_scores(
        text,
        r"C:\Users\DELL\Desktop\ERASMUS\FML\project\lm_dictionary.csv",
        category_column="category",
        word_column="word"
    )

    print("\nLM TOTAL SCORE:", lm_results["total_score"])
    for cat, values in lm_results["category_scores"].items():
        print(f"{cat}: matches={values['matches']}, score={values['score']}")

    esg_results = dictionary_scores(
        text,
        ESG_TOPIC_FILE,
        category_column="topic",
        word_column="word"
    )

    print("\nESG TOTAL SCORE:", esg_results["total_score"])
    for topic, values in esg_results["category_scores"].items():
        print(f"{topic}: matches={values['matches']}, score={values['score']}")


if __name__ == "__main__":
    main()