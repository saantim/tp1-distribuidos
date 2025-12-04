#!/usr/bin/env python3
"""
Generate expected results from local CSV files.
Works with both MIN and FULL datasets.

Usage:
    python generate_expected_results.py --dataset min
    python generate_expected_results.py --dataset full
"""

import argparse
import json
from pathlib import Path

import pandas as pd


# Configuración
pd.set_option("display.max_columns", None)
pd.set_option("display.max_colwidth", 100)


def load_transactions(base_path: Path, dataset_type: str) -> pd.DataFrame:
    """Load transactions for 2024 and 2025."""
    print("\n📁 Loading transactions...")

    drop_idx = {2, 3, 6}
    sample = pd.read_csv(base_path / "transactions/transactions_202401.csv", nrows=0)
    keep_idx = [i for i in range(sample.shape[1]) if i not in drop_idx]

    if dataset_type == "min":
        # Solo enero para MIN
        months = ["202401", "202501"]
    else:
        # Todos los meses para FULL
        months = [
            "202401",
            "202402",
            "202403",
            "202404",
            "202405",
            "202406",
            "202407",
            "202408",
            "202409",
            "202410",
            "202411",
            "202412",
            "202501",
            "202502",
            "202503",
            "202504",
            "202505",
            "202506",
        ]

    dfs = []
    for month in months:
        file_path = base_path / f"transactions/transactions_{month}.csv"
        if file_path.exists():
            df = pd.read_csv(file_path, usecols=keep_idx, low_memory=False)
            dfs.append(df)
            print(f"  ✓ Loaded transactions_{month}.csv ({len(df):,} rows)")

    transactions = pd.concat(dfs, ignore_index=True)
    print(f"✓ Total: {len(transactions):,} transactions")

    # Cast types
    transactions["transaction_id"] = transactions["transaction_id"].astype("string").str.strip()
    transactions["store_id"] = pd.to_numeric(transactions["store_id"], errors="coerce").astype("Int64")
    transactions["user_id"] = pd.to_numeric(transactions["user_id"], errors="coerce").astype("Int64")
    transactions["original_amount"] = pd.to_numeric(transactions["original_amount"], errors="coerce").astype("float64")
    transactions["final_amount"] = pd.to_numeric(transactions["final_amount"], errors="coerce").astype("float64")
    transactions["created_at"] = pd.to_datetime(transactions["created_at"], errors="coerce")

    return transactions


def load_transaction_items(base_path: Path, dataset_type: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Load transaction items for 2024 and 2025."""
    print("\n📁 Loading transaction items...")

    if dataset_type == "min":
        months_2024 = ["202401"]
        months_2025 = ["202501"]
    else:
        months_2024 = [f"2024{str(m).zfill(2)}" for m in range(1, 13)]
        months_2025 = [f"2025{str(m).zfill(2)}" for m in range(1, 7)]

    dfs_2024 = []
    for month in months_2024:
        file_path = base_path / f"transaction_items/transaction_items_{month}.csv"
        if file_path.exists():
            df = pd.read_csv(file_path, low_memory=False)
            dfs_2024.append(df)

    dfs_2025 = []
    for month in months_2025:
        file_path = base_path / f"transaction_items/transaction_items_{month}.csv"
        if file_path.exists():
            df = pd.read_csv(file_path, low_memory=False)
            dfs_2025.append(df)

    transactions_items_2024 = pd.concat(dfs_2024, ignore_index=True) if dfs_2024 else pd.DataFrame()
    transactions_items_2025 = pd.concat(dfs_2025, ignore_index=True) if dfs_2025 else pd.DataFrame()

    print(f"✓ Loaded {len(transactions_items_2024):,} items from 2024")
    print(f"✓ Loaded {len(transactions_items_2025):,} items from 2025")

    # Cast types
    for df in [transactions_items_2024, transactions_items_2025]:
        if not df.empty:
            df["transaction_id"] = df["transaction_id"].astype("string").str.strip()
            df["item_id"] = pd.to_numeric(df["item_id"], errors="coerce").astype("Int64")
            df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce").astype("Int64")
            df["unit_price"] = pd.to_numeric(df["unit_price"], errors="coerce").astype("float64")
            df["subtotal"] = pd.to_numeric(df["subtotal"], errors="coerce").astype("float64")
            df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")

    return transactions_items_2024, transactions_items_2025


def load_users(base_path: Path) -> pd.DataFrame:
    """Load all users (all months)."""
    print("\n📁 Loading users...")

    drop_idx = {1}
    sample = pd.read_csv(base_path / "users/users_202307.csv", nrows=0)
    keep_idx = [i for i in range(sample.shape[1]) if i not in drop_idx]

    user_files = sorted((base_path / "users").glob("users_*.csv"))
    users_dfs = [pd.read_csv(f, usecols=keep_idx, low_memory=False) for f in user_files]
    users = pd.concat(users_dfs, ignore_index=True)
    print(f"✓ Loaded {len(users):,} users")

    # Cast types
    users["user_id"] = pd.to_numeric(users["user_id"], errors="coerce").astype("Int64")
    users["birthdate"] = pd.to_datetime(users["birthdate"], errors="coerce").dt.normalize()
    users["registered_at"] = pd.to_datetime(users["registered_at"], errors="coerce")

    return users


def load_menu_items(base_path: Path) -> pd.DataFrame:
    """Load menu items."""
    print("\n📁 Loading menu items...")

    drop_idx = {4, 5, 6}
    sample = pd.read_csv(base_path / "menu_items/menu_items.csv", nrows=0)
    keep_idx = [i for i in range(sample.shape[1]) if i not in drop_idx]

    menu_items = pd.read_csv(base_path / "menu_items/menu_items.csv", usecols=keep_idx, low_memory=False)
    print(f"✓ Loaded {len(menu_items)} menu items")

    # Cast types
    menu_items["item_name"] = menu_items["item_name"].astype("string").str.strip()
    menu_items["category"] = menu_items["category"].astype("string").str.strip()
    menu_items["item_id"] = pd.to_numeric(menu_items["item_id"], errors="coerce").astype("Int64")
    menu_items["price"] = pd.to_numeric(menu_items["price"], errors="coerce").astype("float64")

    return menu_items


def load_stores(base_path: Path) -> pd.DataFrame:
    """Load stores."""
    print("\n📁 Loading stores...")

    drop_idx = {2, 3, 6, 7}
    sample = pd.read_csv(base_path / "stores/stores.csv", nrows=0)
    keep_idx = [i for i in range(sample.shape[1]) if i not in drop_idx]

    stores = pd.read_csv(base_path / "stores/stores.csv", usecols=keep_idx, low_memory=False)
    print(f"✓ Loaded {len(stores)} stores")

    # Cast types
    stores["store_name"] = stores["store_name"].astype("string").str.strip()
    stores["city"] = stores["city"].astype("string").str.strip()
    stores["state"] = stores["state"].astype("string").str.strip()
    stores["store_id"] = pd.to_numeric(stores["store_id"], errors="coerce").astype("Int64")

    return stores


def generate_q1(transactions: pd.DataFrame, output_path: Path):
    """Q1: Transactions between 6AM-11PM with amount >= 75."""
    print("\n🔍 Generating Q1...")

    q1_transactions_6_to_23_hours = transactions.set_index("created_at").between_time("6:00", "23:00")
    q1_transactions_6_to_23_hours.reset_index(inplace=True)
    q1_transactions_filtered = q1_transactions_6_to_23_hours[q1_transactions_6_to_23_hours["final_amount"] >= 75]
    q1_result = (
        q1_transactions_filtered[["transaction_id", "final_amount"]]
        .sort_values(by=["transaction_id"])
        .to_dict("records")
    )

    with open(output_path / "q1.json", "w") as f:
        json.dump(q1_result, f, indent=2)
    print(f"✓ Q1: {len(q1_result):,} transactions saved")

    return q1_transactions_6_to_23_hours


def generate_q2(
    transactions_items_2024: pd.DataFrame,
    transactions_items_2025: pd.DataFrame,
    menu_items: pd.DataFrame,
    output_path: Path,
):
    """Q2: Top products per period."""
    print("\n🔍 Generating Q2...")

    transaction_items = pd.concat([transactions_items_2024, transactions_items_2025], ignore_index=True)
    transaction_items["year_month_created_at"] = transaction_items["created_at"].dt.strftime("%Y-%m")
    transaction_items_by_year_month = transaction_items.groupby(["year_month_created_at", "item_id"])
    menu_items_names_only = menu_items[["item_id", "item_name"]]

    # Most sold (by quantity)
    q2_groups_with_quantity = (
        transaction_items_by_year_month["quantity"]
        .sum()
        .reset_index(drop=False)
        .rename(columns={"quantity": "sellings_qty"})
    )
    q2_best_selling = (
        q2_groups_with_quantity.sort_values(by=["year_month_created_at", "sellings_qty"], ascending=[True, False])
        .groupby(["year_month_created_at"])
        .head(1)
    )

    # Most revenue (by subtotal)
    q2_groups_with_subtotal = (
        transaction_items_by_year_month["subtotal"]
        .sum()
        .reset_index(drop=False)
        .rename(columns={"subtotal": "profit_sum"})
    )
    q2_most_profits = (
        q2_groups_with_subtotal.sort_values(by=["year_month_created_at", "profit_sum"], ascending=[True, False])
        .groupby(["year_month_created_at"])
        .head(1)
    )

    # Merge with names
    q2_best_selling_with_name = pd.merge(q2_best_selling, menu_items_names_only, on="item_id")
    q2_most_profits_with_name = pd.merge(q2_most_profits, menu_items_names_only, on="item_id")

    # Build result
    q2_results = []
    for period in sorted(q2_best_selling_with_name["year_month_created_at"].unique()):
        sold = q2_best_selling_with_name[q2_best_selling_with_name["year_month_created_at"] == period].iloc[0]
        rev = q2_most_profits_with_name[q2_most_profits_with_name["year_month_created_at"] == period].iloc[0]
        q2_results.append(
            {
                "period": period,
                "most_sold_product": {
                    "item_id": str(sold["item_id"]),
                    "item_name": sold["item_name"],
                    "quantity": int(sold["sellings_qty"]),
                },
                "highest_revenue_product": {
                    "item_id": str(rev["item_id"]),
                    "item_name": rev["item_name"],
                    "revenue": float(rev["profit_sum"]),
                },
            }
        )

    q2_result = {"query": "Q2", "description": "Top products per period (2024-2025)", "results": q2_results}

    with open(output_path / "q2.json", "w") as f:
        json.dump(q2_result, f, indent=2)
    print(f"✓ Q2: {len(q2_results)} periods saved")


def generate_q3(q1_transactions_6_to_23_hours: pd.DataFrame, stores: pd.DataFrame, output_path: Path):
    """Q3: TPV per semester per store (6AM-11PM)."""
    print("\n🔍 Generating Q3...")

    q3_transactions_6_to_23_hours = q1_transactions_6_to_23_hours[["created_at", "store_id", "final_amount"]].copy()
    q3_transactions_6_to_23_hours.loc[q3_transactions_6_to_23_hours["created_at"].dt.month <= 6, "half_created_at"] = (
        "1"
    )
    q3_transactions_6_to_23_hours.loc[q3_transactions_6_to_23_hours["created_at"].dt.month >= 7, "half_created_at"] = (
        "2"
    )
    q3_transactions_6_to_23_hours["year_half_created_at"] = (
        q3_transactions_6_to_23_hours["created_at"].dt.year.astype(str)
        + "-H"
        + q3_transactions_6_to_23_hours["half_created_at"]
    )
    q3_transactions_by_year_half = q3_transactions_6_to_23_hours.groupby(["year_half_created_at", "store_id"])
    stores_names_only = stores[["store_id", "store_name"]]

    q3_groups_with_tpv = (
        q3_transactions_by_year_half["final_amount"]
        .sum()
        .reset_index(drop=False)
        .rename(columns={"final_amount": "tpv"})
    )
    q3_tpv_with_name = pd.merge(q3_groups_with_tpv, stores_names_only, on="store_id")
    q3_result_df = q3_tpv_with_name[["year_half_created_at", "store_name", "tpv", "store_id"]].sort_values(
        by=["year_half_created_at", "store_name"]
    )

    # Rename columns to match expected format
    q3_result_df = q3_result_df.rename(columns={"year_half_created_at": "semester"})
    q3_results = q3_result_df[["semester", "store_id", "store_name", "tpv"]].to_dict("records")

    # Convert store_id to string for consistency
    for r in q3_results:
        r["store_id"] = str(r["store_id"])

    q3_result = {
        "query": "Q3",
        "description": "TPV per semester per store (6AM-11PM transactions)",
        "results": q3_results,
    }

    with open(output_path / "q3.json", "w") as f:
        json.dump(q3_result, f, indent=2)
    print(f"✓ Q3: {len(q3_results)} store-semesters saved")


def generate_q4(transactions: pd.DataFrame, stores: pd.DataFrame, users: pd.DataFrame, output_path: Path):
    """Q4: Top 3 customers per store (with ties, taking top 35 per store)."""
    print("\n🔍 Generating Q4...")

    transactions_by_store_user = transactions.dropna(subset=["user_id"]).groupby(["store_id", "user_id"])
    stores_names_only = stores[["store_id", "store_name"]]
    users_birthdates_only = users[["user_id", "birthdate"]]

    q4_groups_with_most_purchases = (
        transactions_by_store_user["transaction_id"]
        .count()
        .reset_index(drop=False)
        .rename(columns={"transaction_id": "purchases_qty"})
    )

    # Tomar top 3500 por tienda (suficiente para cubrir empates del top 3)
    q4_top_candidates = (
        q4_groups_with_most_purchases.sort_values(
            by=["store_id", "purchases_qty", "user_id"], ascending=[True, False, True]
        )
        .groupby(["store_id"])
        .head(3500)
    )

    q4_most_purchases_with_store = pd.merge(q4_top_candidates, stores_names_only, on="store_id")
    q4_most_purchases_with_store_and_user = pd.merge(q4_most_purchases_with_store, users_birthdates_only, on="user_id")

    q4_result_df = q4_most_purchases_with_store_and_user[["store_name", "birthdate", "purchases_qty"]].sort_values(
        by=["store_name", "purchases_qty", "birthdate"], ascending=[True, False, True]
    )

    # Convert birthdate to string
    q4_result_df["birthdate"] = q4_result_df["birthdate"].astype(str)
    q4_results = q4_result_df.to_dict("records")

    q4_result = {
        "query": "Q4",
        "description": "Top 3500 customers per store (covers top 3 with ties)",
        "results": q4_results,
    }

    with open(output_path / "q4.json", "w") as f:
        json.dump(q4_result, f, indent=2)
    print(f"✓ Q4: {len(q4_results)} customers saved (top 3500 per store)")


def main():
    parser = argparse.ArgumentParser(description="Generate expected results from local CSV files")
    parser.add_argument(
        "--dataset",
        choices=["min", "full"],
        required=True,
        help="Dataset type: 'min' (Jan only) or 'full' (all months)",
    )
    args = parser.parse_args()

    # Setup paths
    base_path = Path(f".data/dataset_{args.dataset}")
    output_path = Path(f".results/expected/{args.dataset}")
    output_path.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print(f"GENERATING EXPECTED RESULTS FOR {args.dataset.upper()} DATASET")
    print("=" * 60)
    print(f"Base path: {base_path}")
    print(f"Output path: {output_path}")

    # Load data
    transactions = load_transactions(base_path, args.dataset)
    transactions_items_2024, transactions_items_2025 = load_transaction_items(base_path, args.dataset)
    users = load_users(base_path)
    menu_items = load_menu_items(base_path)
    stores = load_stores(base_path)

    # Generate queries
    print("\n" + "=" * 60)
    print("GENERATING QUERY RESULTS")
    print("=" * 60)

    q1_transactions = generate_q1(transactions, output_path)
    generate_q2(transactions_items_2024, transactions_items_2025, menu_items, output_path)
    generate_q3(q1_transactions, stores, output_path)
    generate_q4(transactions, stores, users, output_path)

    print("\n" + "=" * 60)
    print("✅ ALL EXPECTED RESULTS GENERATED SUCCESSFULLY!")
    print("=" * 60)
    print(f"\nResults saved to: {output_path}")
    print("  - q1.json")
    print("  - q2.json")
    print("  - q3.json")
    print("  - q4.json")


if __name__ == "__main__":
    main()
