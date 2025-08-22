# analysis.py
# Ingest, preview, clean, store to SQLite, run queries, and export charts.

from __future__ import annotations

import os
import sqlite3
from pathlib import Path
from typing import Dict, Tuple

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns


# =========================== Path helpers ===========================

def _script_dir() -> Path:
    """Absolute directory of this script."""
    return Path(__file__).resolve().parent


def _find_data_file(*names: str) -> Path:
    """
    Return the first existing path matching any of the filenames in *names*.

    Search order:
      1) DATA_DIR/<name>, DATA_DIR/data/<name>               (if env var is set)
      2) <script_dir>/<name>, <script_dir>/data/<name>
      3) <cwd>/<name>, <cwd>/data/<name>
      4) first hit from <script_dir>.rglob(<name>)
    """
    script_dir = _script_dir()
    env_dir = os.getenv("DATA_DIR")
    env_dir = Path(env_dir) if env_dir else None

    candidates: list[Path] = []
    for nm in names:
        if env_dir:
            candidates += [env_dir / nm, env_dir / "data" / nm]
        candidates += [
            script_dir / nm,
            script_dir / "data" / nm,
            Path.cwd() / nm,
            Path.cwd() / "data" / nm,
        ]

    for p in candidates:
        if p.is_file():
            return p

    # very last resort: recursive search under script_dir
    for nm in names:
        hits = list(script_dir.rglob(nm))
        if hits:
            return hits[0]

    raise FileNotFoundError(f"Could not find any of: {', '.join(names)}")


# =========================== Readers / Cleaning ===========================

def _clean_amount_series(s: pd.Series) -> pd.Series:
    return (
        s.astype(str)
         .str.replace("$", "", regex=False)
         .str.replace(",", "", regex=False)
         .astype(float)
    )


def read_transactions(path: Path) -> pd.DataFrame:
    """
    Read transactions from a plain CSV file.
    Normalises:
      - 'date' -> datetime
      - 'amount' -> float (currency symbols/commas stripped)
    """
    df = pd.read_csv(path)
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"])
    if "amount" in df.columns:
        df["amount"] = _clean_amount_series(df["amount"])
    return df


def load_data(users_path: Path, cards_path: Path, transactions_path: Path) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    users = pd.read_csv(users_path)
    cards = pd.read_csv(cards_path)
    transactions = read_transactions(transactions_path)
    return users, cards, transactions


# =========================== Preview logging ===========================

def _preview_df(label: str, df: pd.DataFrame, path: Path) -> str:
    """Return a human-readable preview string and also print it."""
    lines: list[str] = []
    lines.append(f"[{label}]  file: {path}")
    lines.append(f"[{label}]  shape: {df.shape[0]} rows × {df.shape[1]} cols")
    lines.append(f"[{label}]  columns: {', '.join(map(str, df.columns))}")
    # dtypes + head
    with pd.option_context("display.max_rows", 200, "display.max_columns", 200):
        lines.append(f"[{label}]  dtypes:\n{df.dtypes.to_string()}")
        head_str = df.head(5).to_string(index=False)
        lines.append(f"[{label}]  head(5):\n{head_str}")
    # common key presence
    keys = ["id", "client_id", "card_id", "user_id", "customer_id"]
    present = ", ".join(f"{k}={'Y' if k in df.columns else 'N'}" for k in keys)
    lines.append(f"[{label}]  key columns present: {present}")
    text = "\n".join(lines)
    print(text + "\n" + "-" * 80)
    return text


# =========================== Storage & Queries ===========================

def normalise_and_store_sqlite(users: pd.DataFrame, cards: pd.DataFrame, transactions: pd.DataFrame, db_path: Path) -> None:
    conn = sqlite3.connect(str(db_path))
    try:
        users.to_sql("users", conn, if_exists="replace", index=False)
        cards.to_sql("cards", conn, if_exists="replace", index=False)
        transactions.to_sql("transactions", conn, if_exists="replace", index=False)
    finally:
        conn.close()


def run_queries(db_path: Path) -> Dict[str, pd.DataFrame]:
    conn = sqlite3.connect(str(db_path))
    try:
        out: Dict[str, pd.DataFrame] = {}

        out["gender_distribution"] = pd.read_sql_query(
            """
            SELECT gender, COUNT(*) AS num_users
            FROM users
            GROUP BY gender;
            """,
            conn,
        )

        out["age_distribution"] = pd.read_sql_query(
            """
            SELECT
              CASE
                WHEN current_age < 30 THEN '<30'
                WHEN current_age BETWEEN 30 AND 39 THEN '30-39'
                WHEN current_age BETWEEN 40 AND 49 THEN '40-49'
                WHEN current_age BETWEEN 50 AND 59 THEN '50-59'
                WHEN current_age BETWEEN 60 AND 69 THEN '60-69'
                ELSE '70+'
              END AS age_bucket,
              COUNT(*) AS num_users
            FROM users
            GROUP BY age_bucket
            ORDER BY age_bucket;
            """,
            conn,
        )

        out["card_brand_distribution"] = pd.read_sql_query(
            """
            SELECT card_brand, COUNT(*) AS num_cards
            FROM cards
            GROUP BY card_brand
            ORDER BY num_cards DESC;
            """,
            conn,
        )

        out["card_type_distribution"] = pd.read_sql_query(
            """
            SELECT card_type, COUNT(*) AS num_cards
            FROM cards
            GROUP BY card_type
            ORDER BY num_cards DESC;
            """,
            conn,
        )

        out["top_mcc"] = pd.read_sql_query(
            """
            SELECT mcc, COUNT(*) AS txn_count
            FROM transactions
            GROUP BY mcc
            ORDER BY txn_count DESC
            LIMIT 5;
            """,
            conn,
        )

        out["transaction_methods"] = pd.read_sql_query(
            """
            SELECT use_chip, COUNT(*) AS txn_count
            FROM transactions
            GROUP BY use_chip;
            """,
            conn,
        )

        # amount is stored as REAL; no string cleaning needed in SQL
        out["avg_txn_amount_by_brand"] = pd.read_sql_query(
            """
            SELECT
              c.card_brand,
              AVG(t.amount) AS avg_amount
            FROM transactions t
            JOIN cards c ON t.card_id = c.id
            GROUP BY c.card_brand
            ORDER BY avg_amount DESC;
            """,
            conn,
        )

        return out
    finally:
        conn.close()


def save_query_results(results: Dict[str, pd.DataFrame], out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    for name, df in results.items():
        df.to_csv(out_dir / f"{name}.csv", index=False)


# =========================== Charts ===========================

def create_charts(
    users: pd.DataFrame,
    cards: pd.DataFrame,
    transactions: pd.DataFrame,
    out_dir: Path,
) -> None:
    """Generate charts and save under out_dir."""
    out_dir.mkdir(parents=True, exist_ok=True)
    sns.set_style("whitegrid")

    # Gender
    if "gender" in users.columns:
        gender_counts = users["gender"].value_counts().rename_axis("gender").reset_index(name="count")
        plt.figure(figsize=(5, 3))
        sns.barplot(x="gender", y="count", data=gender_counts, palette="pastel")
        plt.title("Gender distribution of users")
        plt.ylabel("Number of users")
        plt.savefig(out_dir / "gender_distribution.png", bbox_inches="tight")
        plt.close()

    # Card type
    if "card_type" in cards.columns:
        type_counts = cards["card_type"].value_counts().rename_axis("card_type").reset_index(name="count")
        plt.figure(figsize=(5, 3))
        plt.pie(
            type_counts["count"],
            labels=type_counts["card_type"],
            autopct="%.1f%%",
            colors=sns.color_palette("pastel"),
        )
        plt.title("Card type distribution")
        plt.savefig(out_dir / "card_type_distribution.png", bbox_inches="tight")
        plt.close()

    # Age buckets
    if "current_age" in users.columns:
        def bucket(a: int) -> str:
            if a < 30:
                return "<30"
            if a < 40:
                return "30-39"
            if a < 50:
                return "40-49"
            if a < 60:
                return "50-59"
            if a < 70:
                return "60-69"
            return "70+"

        age_buckets = (
            users["current_age"].apply(bucket).value_counts().rename_axis("age_bucket").reset_index(name="count")
        )
        age_buckets = age_buckets.sort_values("age_bucket")
        plt.figure(figsize=(5, 3))
        sns.barplot(x="age_bucket", y="count", data=age_buckets, palette="pastel")
        plt.title("Age distribution of users")
        plt.ylabel("Number of users")
        plt.xlabel("Age bracket")
        plt.savefig(out_dir / "age_distribution.png", bbox_inches="tight")
        plt.close()

    # Transaction methods
    if "use_chip" in transactions.columns:
        method_counts = transactions["use_chip"].value_counts().rename_axis("method").reset_index(name="count")
        plt.figure(figsize=(5, 3))
        sns.barplot(x="method", y="count", data=method_counts, palette="pastel")
        plt.title("Transaction method distribution")
        plt.ylabel("Number of transactions")
        plt.xticks(rotation=45, ha="right")
        plt.savefig(out_dir / "transaction_method_distribution.png", bbox_inches="tight")
        plt.close()

    # Top MCC
    if "mcc" in transactions.columns:
        top_mcc = transactions["mcc"].astype(str).value_counts().head(5).rename_axis("mcc").reset_index(name="count")
        plt.figure(figsize=(5, 3))
        sns.barplot(x="mcc", y="count", data=top_mcc, palette="pastel")
        plt.title("Top merchant categories by frequency")
        plt.ylabel("Number of transactions")
        plt.xlabel("MCC code")
        plt.savefig(out_dir / "top_mcc_frequency.png", bbox_inches="tight")
        plt.close()

    # Avg transaction amount by brand (map card_id -> card_brand to avoid huge join)
    if {"card_brand", "id"} <= set(cards.columns) and {"card_id", "amount"} <= set(transactions.columns):
        brand_map = cards.set_index("id")["card_brand"]
        tx_with_brand = transactions.assign(card_brand=transactions["card_id"].map(brand_map))
        brand_avg = (
            tx_with_brand.groupby("card_brand", dropna=True)["amount"]
            .mean()
            .reset_index()
            .sort_values("amount", ascending=False)
        )
        plt.figure(figsize=(5, 3))
        sns.barplot(x="card_brand", y="amount", data=brand_avg, palette="pastel")
        plt.title("Average transaction amount by card brand")
        plt.ylabel("Average amount")
        plt.xlabel("Card brand")
        plt.savefig(out_dir / "avg_amount_by_brand.png", bbox_inches="tight")
        plt.close()


# =========================== Main ===========================

def main() -> None:
    # Data files (transactions are CSV only)
    users_path = _find_data_file("users_data.csv")
    cards_path = _find_data_file("cards_data.csv")
    transactions_path = _find_data_file("transactions_data.csv")

    print("Using data files:")
    print(f"  {users_path}")
    print(f"  {cards_path}")
    print(f"  {transactions_path}\n")

    users, cards, transactions = load_data(users_path, cards_path, transactions_path)

    # ---------- PREVIEW ----------
    preview_txt = []
    preview_txt.append(_preview_df("USERS", users, users_path))
    preview_txt.append(_preview_df("CARDS", cards, cards_path))
    preview_txt.append(_preview_df("TRANSACTIONS", transactions, transactions_path))

    log_path = _script_dir() / "data_preview.txt"
    with open(log_path, "w", encoding="utf-8") as f:
        f.write("\n\n".join(preview_txt))
    print(f"Preview written to: {log_path}\n")

    # Optional: stop after preview (no DB/plots)
    if os.getenv("PREVIEW_ONLY", "0") == "1":
        print("PREVIEW_ONLY=1 => stopping after preview.")
        return

    # ---------- Persist & Query ----------
    proj = _script_dir()
    db_path = proj / "user_behavior.db"
    results_dir = proj / "results"
    charts_dir = proj / "charts"

    normalise_and_store_sqlite(users, cards, transactions, db_path)
    results = run_queries(db_path)
    save_query_results(results, results_dir)

    # ---------- Charts ----------
    create_charts(users, cards, transactions, charts_dir)

    print("Done.")
    print(f"SQLite : {db_path}")
    print(f"Results: {results_dir}/")
    print(f"Charts : {charts_dir}/")


if __name__ == "__main__":
    main()
