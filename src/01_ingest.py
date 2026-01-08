import time
import logging

import pandas as pd
import yfinance as yf

def get_SP500_stock_list():

    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    tables = pd.read_html(url)  # returns all tables on the page
    sp500_table = tables[0]     # first table is the constituents

    # Wikipedia typically has columns like 'Symbol', 'Security', 'GICS Sector', ...
    sp500 = sp500_table.rename(columns={"Symbol": "ticker"})
    sp500["ticker"] = sp500["ticker"].str.replace(".", "-", regex=False)  # adjust for yfinance

    sp500.to_csv("data/raw/universe/sp500.csv", index=False)

    return sp500["ticker"].tolist()

# ---------- config ----------
TICKER_LIST = get_SP500_stock_list()
OUTPUT_PATH = "data/raw/fundamentals_snapshot.csv"
SLEEP_BETWEEN_REQUESTS = 0.5  # seconds

# ---------- logging ----------
logging.basicConfig(
    filename="logs/ingest.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


def fetch_market_fields(ticker: str) -> pd.DataFrame:
    """
    Fetch key fundamentals for one ticker using yfinance.
    Returns a single-row DataFrame with flat columns.
    """
    tk = yf.Ticker(ticker)

    info = tk.info

    fields = {
        "ticker": ticker,
        "shortName": info.get("shortName"),
        "sector": info.get("sector"),
        "industry": info.get("industry"),
        "currentPrice": info.get("currentPrice") or info.get("regularMarketPrice"),
        "sharesOutstanding": info.get("sharesOutstanding"),
        "marketCap": info.get("marketCap"),
        "trailingEps": info.get("trailingEps"),
        "forwardEps": info.get("forwardEps"),
        "dividendRate": info.get("dividendRate"),
        "dividendYield": info.get("dividendYield"),
        "priceToBook": info.get("priceToBook"),
    }

    df = pd.DataFrame([fields])
    return df

def fetch_income_statement(ticker: str) -> pd.DataFrame:
    tk = yf.Ticker(ticker)
    df = tk.financials  # annual income statement; index = line items, columns = dates
    if df is None or df.empty:
        return pd.DataFrame()

    # take latest column
    latest_date = df.columns[0]
    col = df[latest_date]

    fields = {
        "ticker": ticker,
        "is_date": latest_date,
        "revenue": col.get("Total Revenue"),
        "cogs": col.get("Cost Of Revenue"),
        "gross_profit": col.get("Gross Profit"),
        "operating_income": col.get("Operating Income"),
        "interest_expense": col.get("Interest Expense"),
        "net_income": col.get("Net Income"),
    }
    return pd.DataFrame([fields])

def fetch_balance_sheet(ticker: str) -> pd.DataFrame:
    tk = yf.Ticker(ticker)
    df = tk.balance_sheet  # annual BS; index = line items, columns = dates
    if df is None or df.empty:
        return pd.DataFrame()

    latest_date = df.columns[0]
    col = df[latest_date]

    total_debt = 0
    if "Short Long Term Debt" in col.index:
        total_debt += col.get("Short Long Term Debt", 0)
    if "Long Term Debt" in col.index:
        total_debt += col.get("Long Term Debt", 0)

    fields = {
        "ticker": ticker,
        "bs_date": latest_date,
        "total_assets": col.get("Total Assets"),
        "total_liab": col.get("Total Liab"),
        "total_equity": col.get("Total Stockholder Equity"),
        "current_assets": col.get("Total Current Assets"),
        "current_liab": col.get("Total Current Liabilities"),
        "short_long_term_debt": col.get("Short Long Term Debt"),
        "long_term_debt": col.get("Long Term Debt"),
        "total_debt": total_debt,
    }
    return pd.DataFrame([fields])

def fetch_cashflow(ticker: str) -> pd.DataFrame:
    tk = yf.Ticker(ticker)
    df = tk.cashflow  # annual CF; index = line items, columns = dates
    if df is None or df.empty:
        return pd.DataFrame()

    latest_date = df.columns[0]
    col = df[latest_date]

    op_cf = col.get("Total Cash From Operating Activities")
    capex = col.get("Capital Expenditures")

    if op_cf is not None and capex is not None:
        free_cash_flow = op_cf + capex  # capex usually negative
    else:
        free_cash_flow = None

    fields = {
        "ticker": ticker,
        "cf_date": latest_date,
        "operating_cash_flow": op_cf,
        "capex": capex,
        "free_cash_flow": free_cash_flow,
    }
    return pd.DataFrame([fields])

def fetch_all_for_ticker(ticker: str) -> pd.DataFrame:
    mkt = fetch_market_fields(ticker)
    inc = fetch_income_statement(ticker)
    bs = fetch_balance_sheet(ticker)
    cf = fetch_cashflow(ticker)

    # merge on ticker (and date columns if you want to keep them)
    dfs = [df for df in [mkt, inc, bs, cf] if not df.empty]
    if not dfs:
        return pd.DataFrame()

    out = dfs[0]
    for df in dfs[1:]:
        # Avoid duplicate 'ticker' columns
        df = df.loc[:, ~df.columns.duplicated()]
        out = out.merge(df, on="ticker", how="outer")
    return out

def main():
    rows = []
    for t in TICKER_LIST:
        try:
            df = fetch_all_for_ticker(t)
            if not df.empty:
                rows.append(df)
        except Exception as e:
            logging.error(f"Failed {t}: {e}")
        time.sleep(0.5)

    if rows:
        result = pd.concat(rows, ignore_index=True)
        result.to_csv( OUTPUT_PATH, index=False)

if __name__ == "__main__":
    main()