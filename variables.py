import wrds
import pandas as pd
import numpy as np


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


def prepare_compustat_data(df):
    df = df[df["at"] > 0].copy()

    df["firm_size"] = np.log(df["at"])
    df["roa"] = df["ni"] / df["at"]
    df["leverage"] = (df["dltt"].fillna(0) + df["dlc"].fillna(0)) / df["at"]

    df["sic"] = pd.to_numeric(df["sic"], errors="coerce")
    df["industry"] = (df["sic"] // 100).astype("Int64")

    df["year"] = df["datadate"].dt.year
    first_year = df.groupby("gvkey")["year"].transform("min")
    df["firm_age"] = df["year"] - first_year

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


def merge_ccm(comp, ccm):
    comp = comp.copy()
    ccm = ccm.copy()

    ccm["permno"] = pd.to_numeric(ccm["permno"], errors="coerce").astype("Int64")

    df = comp.merge(ccm, on="gvkey", how="left")
    df["linkenddt"] = df["linkenddt"].fillna(pd.Timestamp("today").normalize())

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

    # Market equity in millions of dollars
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


def main():
    db = wrds.Connection()

    # Only pull what you need
    start_date = "2014-01-01"
    end_date = "2024-12-31"

    comp = fetch_compustat_data(db, start_date, end_date)
    ccm = fetch_ccm_link(db)
    crsp = fetch_crsp_monthly(db, start_date, end_date)

    comp = prepare_compustat_data(comp)
    comp = compute_book_equity(comp)
    comp = merge_ccm(comp, ccm)

    crsp = prepare_crsp_data(crsp)

    # Keep only CRSP firms that actually appear in Compustat sample
    valid_permnos = comp["permno"].dropna().unique()
    crsp = crsp[crsp["permno"].isin(valid_permnos)].copy()

    comp = add_btm(comp, crsp)
    comp = add_volatility(comp, crsp, window=12, min_periods=6)

    sample = comp[comp["datadate"].between("2015-01-01", "2024-12-31")].copy()

    final = sample[[
        "tic",     
        "conm",    
        "firm_size",
        "roa",
        "leverage",
        "firm_age",
        "industry",
        "btm",
        "volatility"
    ]].copy()

    preview = final.head(10).copy()
    num_cols = preview.select_dtypes(include=[np.number]).columns
    preview[num_cols] = preview[num_cols].round(3)

    print(preview.to_string(index=False))

    final.to_csv(
        r"C:\Users\DELL\Desktop\ERASMUS\FML\project\compustat_firm_panel.csv",
        index=False
    )


if __name__ == "__main__":
    main()