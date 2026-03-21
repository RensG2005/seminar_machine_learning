import wrds
import pandas as pd
import numpy as np
from scipy import stats

SIC2_LABELS = {
    1: "Agriculture / Crops",
    2: "Agriculture / Livestock",
    7: "Agricultural Services",
    8: "Forestry",
    9: "Fishing / Hunting",
    10: "Metal Mining",
    11: "Coal Mining",
    12: "Coal Mining",
    13: "Oil & Gas Extraction",
    14: "Mining / Quarrying",
    15: "Building Construction",
    16: "Heavy Construction",
    17: "Special Trade Contractors",
    20: "Food & Kindred Products",
    21: "Tobacco",
    22: "Textile Mill Products",
    23: "Apparel",
    24: "Lumber & Wood",
    25: "Furniture & Fixtures",
    26: "Paper & Allied Products",
    27: "Printing & Publishing",
    28: "Chemicals & Pharma",
    29: "Petroleum Refining",
    30: "Rubber & Plastics",
    31: "Leather",
    32: "Stone / Glass / Concrete",
    33: "Primary Metals",
    34: "Fabricated Metals",
    35: "Industrial Machinery",
    36: "Electronics",
    37: "Transportation Equipment",
    38: "Instruments / Medical Devices",
    39: "Misc Manufacturing",
    40: "Railroad Transportation",
    41: "Local Transit",
    42: "Trucking",
    43: "US Postal Service",
    44: "Water Transportation",
    45: "Air Transportation",
    46: "Pipelines",
    47: "Transportation Services",
    48: "Communications",
    49: "Utilities",
    50: "Wholesale - Durable Goods",
    51: "Wholesale - Nondurable Goods",
    52: "Building Materials Retail",
    53: "General Merchandise Retail",
    54: "Food Stores",
    55: "Auto Dealers",
    56: "Apparel Retail",
    57: "Furniture Retail",
    58: "Eating & Drinking",
    59: "Misc Retail",
    60: "Banks",
    61: "Credit Institutions",
    62: "Security Brokers",
    63: "Insurance",
    64: "Insurance Agents",
    65: "Real Estate",
    67: "Holding Companies",
    70: "Hotels & Lodging",
    72: "Personal Services",
    73: "Business Services / Software",
    74: "Auto Repair",
    75: "Auto Services",
    76: "Misc Repair",
    78: "Motion Pictures",
    79: "Amusement & Recreation",
    80: "Health Services",
    81: "Legal Services",
    82: "Education",
    83: "Social Services",
    84: "Museums",
    86: "Membership Organizations",
    87: "Engineering & Management",
    89: "Misc Services",
    99: "Nonclassifiable",
}

# ---------------------------------------------------------------------------
# Fetch
# ---------------------------------------------------------------------------


def fetch_compustat_data(db, start_date="2014-01-01", end_date="2024-12-31"):
    query = f"""
        SELECT
            f.gvkey,
            n.tic,
            n.conm,
            n.cik,
            n.sic,
            f.datadate,
            f.fyear,
            f.at,
            f.ni,
            f.dltt,
            f.dlc,
            f.seq,
            f.ceq,
            f.pstk,
            f.pstkl,
            f.pstkrv,
            f.txditc
        FROM comp.funda f
        LEFT JOIN comp.names n
            ON f.gvkey = n.gvkey
        WHERE f.indfmt = 'INDL'
          AND f.datafmt = 'STD'
          AND f.consol = 'C'
          AND f.popsrc = 'D'
          AND f.datadate BETWEEN '{start_date}' AND '{end_date}'
          AND f.at IS NOT NULL
    """
    return db.raw_sql(query, date_cols=["datadate"])


def fetch_ccm_link(db):
    query = """
        SELECT
            gvkey,
            lpermno AS permno,
            linkdt,
            linkenddt
        FROM crsp.ccmxpf_linktable
        WHERE linktype IN ('LC', 'LU')
          AND linkprim IN ('P', 'C')
    """
    return db.raw_sql(query, date_cols=["linkdt", "linkenddt"])


def fetch_firm_first_year(db):
    query = """
        SELECT
            gvkey,
            MIN(EXTRACT(YEAR FROM datadate)) AS first_year
        FROM comp.funda
        WHERE indfmt = 'INDL'
          AND datafmt = 'STD'
          AND consol = 'C'
          AND popsrc = 'D'
          AND at IS NOT NULL
        GROUP BY gvkey
    """
    return db.raw_sql(query)


def fetch_crsp_monthly(db, start_date="2014-01-01", end_date="2024-12-31"):
    query = f"""
        SELECT
            permno,
            date,
            ret,
            prc,
            shrout
        FROM crsp.msf
        WHERE date BETWEEN '{start_date}' AND '{end_date}'
    """
    return db.raw_sql(query, date_cols=["date"])


# ---------------------------------------------------------------------------
# Build
# ---------------------------------------------------------------------------

def prepare_compustat_data(df):
    df = df[df["at"] > 0].copy()

    df["firm_size"] = np.log(df["at"])
    df["roa"] = df["ni"] / df["at"]
    df["leverage"] = (df["dltt"].fillna(0) + df["dlc"].fillna(0)) / df["at"]

    df["sic"] = pd.to_numeric(df["sic"], errors="coerce")
    df["industry"] = (df["sic"] // 100).astype("Int64")

    df["industry_label"] = df["industry"].map(SIC2_LABELS).fillna("Unknown")

    df["year"] = df["datadate"].dt.year
    
    return df

def add_firm_age(comp, first_years):
    # first_years: DataFrame with columns gvkey, first_year
    # fetched from the full Compustat history (no date filter) so firm_age
    # reflects the true age of the firm, not just years in the sample window
    first_years = first_years.copy()
    first_years["gvkey"] = first_years["gvkey"].astype(str)
    first_years["first_year"] = pd.to_numeric(first_years["first_year"], errors="coerce").astype("Int64")

    df = comp.copy()
    df["gvkey"] = df["gvkey"].astype(str)
    df = df.merge(first_years, on="gvkey", how="left")
    df["firm_age"] = df["year"] - df["first_year"] + 1
    df = df.drop(columns=["first_year"])

    return df

def compute_book_equity(df):
    df = df.copy()

    pref_stock = df["pstkrv"]
    pref_stock = pref_stock.fillna(df["pstkl"])
    pref_stock = pref_stock.fillna(df["pstk"])
    pref_stock = pref_stock.fillna(0)

    df["book_equity"] = df["seq"] + df["txditc"].fillna(0) - pref_stock

    missing_seq = df["book_equity"].isna()
    df.loc[missing_seq, "book_equity"] = (
        df.loc[missing_seq, "ceq"]
        + df.loc[missing_seq, "txditc"].fillna(0)
        - pref_stock[missing_seq]
    )

    df.loc[df["book_equity"] <= 0, "book_equity"] = np.nan

    return df


def merge_ccm(comp, ccm, end_date="2024-12-31"):
    comp = comp.copy()
    ccm = ccm.copy()

    ccm["permno"] = pd.to_numeric(ccm["permno"], errors="coerce").astype("Int64")

    df = comp.merge(ccm, on="gvkey", how="left")
    df["linkenddt"] = df["linkenddt"].fillna(pd.Timestamp(end_date))

    df = df[(df["datadate"] >= df["linkdt"]) & (df["datadate"] <= df["linkenddt"])].copy()
    df = df.sort_values(["gvkey", "datadate", "permno"])
    df = df.drop_duplicates(subset=["gvkey", "datadate"], keep="first")

    return df


def prepare_crsp_data(crsp):
    crsp = crsp.copy()

    crsp["permno"] = pd.to_numeric(crsp["permno"], errors="coerce").astype("Int64")
    crsp["ret"] = pd.to_numeric(crsp["ret"], errors="coerce")
    crsp["prc"] = pd.to_numeric(crsp["prc"], errors="coerce")
    crsp["shrout"] = pd.to_numeric(crsp["shrout"], errors="coerce")

    crsp["market_equity"] = crsp["prc"].abs() * crsp["shrout"] / 1000
    crsp["year_month"] = crsp["date"].dt.to_period("M")

    return crsp


def add_btm(comp, crsp):
    df = comp.copy()
    df["year_month"] = df["datadate"].dt.to_period("M")

    me = crsp[["permno", "year_month", "market_equity"]].copy()
    me = me.sort_values(["permno", "year_month"]).drop_duplicates(
        subset=["permno", "year_month"], keep="last"
    )

    df = df.merge(me, on=["permno", "year_month"], how="left")

    df["btm"] = df["book_equity"] / df["market_equity"]
    df.loc[df["market_equity"] <= 0, "btm"] = np.nan

    return df


def add_volatility(comp, crsp, window=12, min_periods=6):
    df = comp.copy()
    df["year_month"] = df["datadate"].dt.to_period("M")

    crsp_ret = crsp[["permno", "date", "ret"]].copy()
    crsp_ret["permno"] = pd.to_numeric(crsp_ret["permno"], errors="coerce").astype("Int64")
    crsp_ret["ret"] = pd.to_numeric(crsp_ret["ret"], errors="coerce")
    crsp_ret["year_month"] = crsp_ret["date"].dt.to_period("M")

    crsp_ret = crsp_ret.dropna(subset=["permno", "ret"]).copy()
    crsp_ret = crsp_ret.sort_values(["permno", "date"])

    crsp_ret["volatility"] = (
        crsp_ret.groupby("permno")["ret"]
        .rolling(window=window, min_periods=min_periods)
        .std()
        .reset_index(level=0, drop=True)
    )

    vol = crsp_ret[["permno", "year_month", "volatility"]].drop_duplicates(
        subset=["permno", "year_month"], keep="last"
    )

    df = df.merge(vol, on=["permno", "year_month"], how="left")

    return df


WINSORIZE_VARS = ["roa", "leverage", "btm", "volatility"]


def winsorize_features(df, features=None, lower=0.01, upper=0.99):
    if features is None:
        features = WINSORIZE_VARS
    df = df.copy()
    for col in features:
        bounds = df.groupby("year")[col].quantile([lower, upper]).unstack()
        lo = df["year"].map(bounds[lower])
        hi = df["year"].map(bounds[upper])
        df[col] = df[col].clip(lower=lo, upper=hi)
    return df


# ---------------------------------------------------------------------------
# Diagnostics
# ---------------------------------------------------------------------------

ALL_FEATURES     = ["firm_size", "roa", "leverage", "firm_age", "industry_label", "btm", "volatility"]
NUMERIC_FEATURES = ["firm_size", "roa", "leverage", "firm_age", "btm", "volatility"]


def coverage_report(df, features):
    records = []
    for col in features:
        n_total   = len(df)
        n_missing = df[col].isna().sum()
        records.append({
            "variable":    col,
            "n_total":     n_total,
            "n_missing":   n_missing,
            "pct_missing": round(n_missing / n_total * 100, 2),
            "n_valid":     n_total - n_missing,
        })
    return pd.DataFrame(records)


def coverage_by_year(df, features):
    rows = []
    for year, grp in df.groupby("year"):
        for col in features:
            n_total   = len(grp)
            n_missing = grp[col].isna().sum()
            rows.append({
                "year":        year,
                "variable":    col,
                "pct_missing": round(n_missing / n_total * 100, 2),
            })
    return (
        pd.DataFrame(rows)
        .pivot(index="year", columns="variable", values="pct_missing")
    )


def distributional_stats(df, features):
    records = []
    for col in features:
        s = df[col].dropna()
        records.append({
            "variable":    col,
            "mean":        round(s.mean(), 4),
            "std":         round(s.std(), 4),
            "skew":        round(stats.skew(s), 4),
            "excess_kurt": round(stats.kurtosis(s), 4),
            "p1":          round(s.quantile(0.01), 4),
            "median":      round(s.quantile(0.50), 4),
            "p99":         round(s.quantile(0.99), 4),
            "min":         round(s.min(), 4),
            "max":         round(s.max(), 4),
        })
    return pd.DataFrame(records)


def industry_summary(df):
    counts = (
        df.groupby(["industry", "industry_label"])
        .size()
        .reset_index(name="n_obs")
        .sort_values("n_obs", ascending=False)
    )
    counts["pct"] = (counts["n_obs"] / len(df) * 100).round(2)
    return counts


def correlation_matrix(df, features):
    return df[features].corr().round(4)


def high_correlations(corr, threshold=0.7):
    pairs = []
    cols = corr.columns.tolist()
    for i in range(len(cols)):
        for j in range(i + 1, len(cols)):
            r = corr.iloc[i, j]
            if abs(r) >= threshold:
                pairs.append({
                    "var_1": cols[i],
                    "var_2": cols[j],
                    "r":     round(r, 4),
                })
    return pd.DataFrame(pairs) if pairs else pd.DataFrame(columns=["var_1", "var_2", "r"])


def yearly_stability(df, features):
    means = df.groupby("year")[features].mean().round(4)
    stds  = df.groupby("year")[features].std().round(4)
    means.columns = [f"{c}_mean" for c in means.columns]
    stds.columns  = [f"{c}_std"  for c in stds.columns]
    return pd.concat([means, stds], axis=1).sort_index(axis=1)


#def univariate_return_predictability(df, features, return_col="ret_fwd"):
    if return_col not in df.columns:
        print(
            f"\n[Skipping return predictability] Column '{return_col}' not found. "
            "Merge your forward return series into the CSV and re-run."
        )
        return pd.DataFrame()

    records = []
    for col in features:
        sub = df[[col, return_col]].dropna()
        if len(sub) < 30:
            continue
        rho, pval = stats.spearmanr(sub[col], sub[return_col])
        records.append({
            "variable":       col,
            "spearman_rho":   round(rho, 4),
            "p_value":        round(pval, 4),
            "n_obs":          len(sub),
            "significant_5%": "yes" if pval < 0.05 else "no",
        })
    return pd.DataFrame(records)


def run_diagnostics(df):
    df = df.copy()
    df["year"] = df["datadate"].dt.year

    corr      = correlation_matrix(df, NUMERIC_FEATURES)
    high_corr = high_correlations(corr, threshold=0.7)
    stability = yearly_stability(df, NUMERIC_FEATURES)

    print("\n=== 1. Coverage ===")
    print(coverage_report(df, ALL_FEATURES).to_string(index=False))

    print("\n=== 1b. Coverage by Year (% missing) ===")
    print(coverage_by_year(df, ALL_FEATURES).to_string())

    print("\n=== 2. Distributional Stats ===")
    print(distributional_stats(df, NUMERIC_FEATURES).to_string(index=False))

    print("\n=== 2b. Industry Distribution (top 20) ===")
    print(industry_summary(df).head(20).to_string(index=False))

    print("\n=== 3. Correlation Matrix ===")
    print(corr.to_string())

    print("\n=== 3b. High Correlations (|r| >= 0.7) ===")
    print(high_corr.to_string(index=False) if not high_corr.empty else "None found.")

    print("\n=== 4. Yearly Stability ===")
    print(stability.to_string())

    #return_pred = univariate_return_predictability(df, NUMERIC_FEATURES)
    #if not return_pred.empty:
    #    print("\n=== 5. Univariate Return Predictability ===")
    #    print(return_pred.to_string(index=False))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    db = wrds.Connection()

    start_date = "2014-01-01"
    end_date   = "2024-12-31"

    comp        = fetch_compustat_data(db, start_date, end_date)
    first_years = fetch_firm_first_year(db)
    ccm         = fetch_ccm_link(db)
    crsp        = fetch_crsp_monthly(db, start_date, end_date)

    comp = prepare_compustat_data(comp)
    comp = add_firm_age(comp, first_years)
    comp = compute_book_equity(comp)
    comp = merge_ccm(comp, ccm, end_date)

    crsp = prepare_crsp_data(crsp)

    valid_permnos = comp["permno"].dropna().unique()
    crsp = crsp[crsp["permno"].isin(valid_permnos)].copy()

    comp = add_btm(comp, crsp)
    comp = add_volatility(comp, crsp, window=12, min_periods=6)

    sample = comp[comp["datadate"].between("2015-01-01", end_date)].copy()
    sample = winsorize_features(sample)

    final = sample[[
        "gvkey",
        "permno",
        "datadate",
        "tic",
        "conm",
        "firm_size",
        "roa",
        "leverage",
        "firm_age",
        "industry",
        "industry_label",
        "btm",
        "volatility",
    ]].copy()

    preview = final.head(10).copy()
    num_cols = preview.select_dtypes(include=[np.number]).columns
    preview[num_cols] = preview[num_cols].round(3)

    print(preview.to_string(index=False))

    final.to_csv(
        r"C:\Users\DELL\Desktop\ERASMUS\FML\project\compustat_firm_panel.csv",
        index=False,
    )

    run_diagnostics(final)

if __name__ == "__main__":
    main()