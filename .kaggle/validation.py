#!/usr/bin/env python3
"""
Validate pipeline results against expected outputs.

Usage:
    python validate_results.py
    python validate_results.py --dataset min
    python validate_results.py --dataset full
"""

import argparse
import json
import sys
from pathlib import Path
from typing import Dict, List, Tuple


class ResultsValidator:
    """Validates pipeline results against expected outputs."""

    def __init__(self, dataset_mode: str = "min", session_id: str = None):
        self.dataset_mode = dataset_mode
        self.session_id = session_id

        if session_id:
            self.pipeline_dir = Path(f".results/{session_id}/pipeline")
        else:
            self.pipeline_dir = Path(".results/pipeline")

        self.expected_dir = Path(f".results/expected/{dataset_mode}")
        self.report = {
            "dataset_mode": dataset_mode,
            "session_id": session_id,
            "queries": {},
            "summary": {"total": 0, "passed": 0, "failed": 0},
        }

    def validate_all(self) -> bool:
        """Validate all queries. Returns True if all pass."""
        queries = ["q1", "q2", "q3", "q4"]
        all_passed = True

        for query in queries:
            passed = self.validate_query(query)
            all_passed = all_passed and passed

        self._print_summary()
        self._save_report()
        return all_passed

    def validate_query(self, query: str) -> bool:
        """Validate a single query."""
        print(f"\n{'='*60}")
        print(f"Validating {query.upper()}")
        print(f"{'='*60}")

        pipeline_file = self.pipeline_dir / f"{query}.json"
        expected_file = self.expected_dir / f"{query}.json"

        if not pipeline_file.exists():
            print(f"❌ Pipeline results not found: {pipeline_file}")
            self.report["queries"][query] = {"status": "ERROR", "reason": "Missing pipeline results"}
            self.report["summary"]["total"] += 1
            self.report["summary"]["failed"] += 1
            return False

        if not expected_file.exists():
            print(f"❌ Expected results not found: {expected_file}")
            self.report["queries"][query] = {"status": "ERROR", "reason": "Missing expected results"}
            self.report["summary"]["total"] += 1
            self.report["summary"]["failed"] += 1
            return False

        with open(pipeline_file) as f:
            pipeline_data = json.load(f)

        with open(expected_file) as f:
            expected_data = json.load(f)

        # Dispatch to query-specific validator
        validator_fn = getattr(self, f"_validate_{query}")
        passed, details = validator_fn(pipeline_data, expected_data)

        self.report["queries"][query] = {"status": "PASS" if passed else "FAIL", **details}
        self.report["summary"]["total"] += 1
        if passed:
            self.report["summary"]["passed"] += 1
            print(f"✅ {query.upper()} PASSED")
        else:
            self.report["summary"]["failed"] += 1
            print(f"❌ {query.upper()} FAILED")
            if "reason" in details:
                print(f"   Reason: {details['reason']}")
            if "examples" in details:
                print(f"   Examples: {details['examples']}")

        return passed

    @staticmethod
    def _validate_q1(pipeline: List[Dict], expected: List[Dict]) -> Tuple[bool, Dict]:
        """
        Validate Q1: List of transactions with id and amount.
        Expected format: [{"transaction_id": "...", "final_amount": ...}, ...]
        """
        # Convert to sets of transaction IDs for comparison
        pipeline_ids = {tx["transaction_id"] for tx in pipeline}
        expected_ids = {tx["transaction_id"] for tx in expected}

        if pipeline_ids == expected_ids:
            # Check amounts match
            pipeline_map = {tx["transaction_id"]: tx["final_amount"] for tx in pipeline}
            expected_map = {tx["transaction_id"]: tx["final_amount"] for tx in expected}

            mismatches = []
            for tx_id in pipeline_ids:
                if pipeline_map[tx_id] != expected_map[tx_id]:
                    mismatches.append(
                        {"transaction_id": tx_id, "expected": expected_map[tx_id], "got": pipeline_map[tx_id]}
                    )

            if mismatches:
                return False, {"reason": "Amount mismatches", "count": len(mismatches), "examples": mismatches[:5]}

            return True, {"transaction_count": len(pipeline), "all_ids_match": True, "all_amounts_match": True}

        missing = expected_ids - pipeline_ids
        extra = pipeline_ids - expected_ids

        return False, {
            "reason": "Transaction ID mismatch",
            "expected_count": len(expected),
            "got_count": len(pipeline),
            "missing_count": len(missing),
            "extra_count": len(extra),
            "missing_examples": list(missing)[:5] if missing else [],
            "extra_examples": list(extra)[:5] if extra else [],
        }

    @staticmethod
    def _validate_q2(pipeline: Dict, expected: Dict) -> Tuple[bool, Dict]:
        """
        Validate Q2: Top products per period.
        Expected format: {
            "query": "Q2",
            "results": [
                {
                    "period": "2024-01",
                    "most_sold_product": {"item_id": "3", "item_name": "Latte", "quantity": 311361},
                    "highest_revenue_product": {"item_id": "8", "item_name": "Matcha Latte", "revenue": 3098440.0}
                }
            ]
        }
        """
        pipeline_results = pipeline.get("results", [])
        expected_results = expected.get("results", [])

        if len(pipeline_results) != len(expected_results):
            return False, {
                "reason": "Period count mismatch",
                "expected": len(expected_results),
                "got": len(pipeline_results),
            }

        # Sort both by period for consistent comparison
        pipeline_sorted = sorted(pipeline_results, key=lambda x: x["period"])
        expected_sorted = sorted(expected_results, key=lambda x: x["period"])

        for p_result, e_result in zip(pipeline_sorted, expected_sorted):
            if p_result["period"] != e_result["period"]:
                return False, {"reason": "Period mismatch", "expected": e_result["period"], "got": p_result["period"]}

            # Check most sold product
            if p_result["most_sold_product"]["item_id"] != e_result["most_sold_product"]["item_id"]:
                return False, {
                    "reason": f"Most sold product mismatch for {p_result['period']}",
                    "expected": e_result["most_sold_product"],
                    "got": p_result["most_sold_product"],
                }

            # Check highest revenue product
            if p_result["highest_revenue_product"]["item_id"] != e_result["highest_revenue_product"]["item_id"]:
                return False, {
                    "reason": f"Highest revenue product mismatch for {p_result['period']}",
                    "expected": e_result["highest_revenue_product"],
                    "got": p_result["highest_revenue_product"],
                }

        return True, {"periods_validated": len(pipeline_results)}

    @staticmethod
    def _validate_q3(pipeline: Dict, expected: Dict) -> Tuple[bool, Dict]:
        """
        Validate Q3: TPV per semester per store.
        Expected format: {
            "query": "Q3",
            "results": [
                {
                    "semester": "2024-H1",
                    "store_id": "1",
                    "store_name": "G Coffee @ USJ 89q",
                    "tpv": 2058314.0
                }
            ]
        }
        """
        pipeline_results = pipeline.get("results", [])
        expected_results = expected.get("results", [])

        if len(pipeline_results) != len(expected_results):
            return False, {
                "reason": "Result count mismatch",
                "expected": len(expected_results),
                "got": len(pipeline_results),
            }

        # Create lookup maps
        pipeline_map = {(r["store_id"], r["semester"]): r["tpv"] for r in pipeline_results}
        expected_map = {(r["store_id"], r["semester"]): r["tpv"] for r in expected_results}

        if set(pipeline_map.keys()) != set(expected_map.keys()):
            missing = set(expected_map.keys()) - set(pipeline_map.keys())
            extra = set(pipeline_map.keys()) - set(expected_map.keys())
            return False, {
                "reason": "Store/semester combinations don't match",
                "missing": [{"store_id": k[0], "semester": k[1]} for k in list(missing)[:5]],
                "extra": [{"store_id": k[0], "semester": k[1]} for k in list(extra)[:5]],
            }

        # Check TPV values (with small tolerance for floating point)
        mismatches = []
        for key in pipeline_map:
            # Allow 0.01 difference for floating point errors
            if abs(pipeline_map[key] - expected_map[key]) > 0.01:
                mismatches.append(
                    {
                        "store_id": key[0],
                        "semester": key[1],
                        "expected": expected_map[key],
                        "got": pipeline_map[key],
                        "diff": abs(pipeline_map[key] - expected_map[key]),
                    }
                )

        if mismatches:
            return False, {"reason": "TPV value mismatches", "count": len(mismatches), "examples": mismatches[:5]}

        return True, {"store_semester_count": len(pipeline_results)}

    @staticmethod
    def _validate_q4(pipeline: Dict, expected: Dict) -> Tuple[bool, Dict]:
        """
        Validate Q4: Top 3 customers per store.

        Strategy:
        1. Each store must have exactly 3 customers in pipeline
        2. Each customer must appear in the expected results (top 35 per store)
        3. Purchase quantities must match
        """
        pipeline_results = pipeline.get("results", [])
        expected_results = expected.get("results", [])

        # Group by store
        pipeline_by_store = {}
        for r in pipeline_results:
            store = r["store_name"]
            if store not in pipeline_by_store:
                pipeline_by_store[store] = []
            # Normalize birthdate format (remove time if present)
            birthdate = r["birthdate"].split()[0] if " " in r["birthdate"] else r["birthdate"]
            pipeline_by_store[store].append({"birthdate": birthdate, "purchases_qty": r["purchases_qty"]})

        expected_by_store = {}
        for r in expected_results:
            store = r["store_name"]
            if store not in expected_by_store:
                expected_by_store[store] = []
            birthdate = r["birthdate"].split()[0] if " " in r["birthdate"] else r["birthdate"]
            expected_by_store[store].append({"birthdate": birthdate, "purchases_qty": r["purchases_qty"]})

        # Check each store has exactly 3 results in pipeline
        for store, results in pipeline_by_store.items():
            if len(results) != 3:
                return False, {
                    "reason": f"Store '{store}' doesn't have exactly 3 customers",
                    "got": len(results),
                    "expected": 3,
                }

        # Check all pipeline stores exist in expected
        missing_stores = set(expected_by_store.keys()) - set(pipeline_by_store.keys())
        if missing_stores:
            return False, {
                "reason": "Pipeline is missing stores from expected results",
                "missing_stores": list(missing_stores),
            }

        extra_stores = set(pipeline_by_store.keys()) - set(expected_by_store.keys())
        if extra_stores:
            return False, {"reason": "Pipeline has stores not in expected results", "extra_stores": list(extra_stores)}

        # For each store, check that all 3 pipeline customers exist in expected top 35
        for store, p_results in pipeline_by_store.items():
            e_results = expected_by_store[store]

            # Create set of (birthdate, qty) tuples from expected
            expected_set = {(e["birthdate"], e["purchases_qty"]) for e in e_results}

            # Check each pipeline customer is in expected
            for p_result in p_results:
                customer_tuple = (p_result["birthdate"], p_result["purchases_qty"])
                if customer_tuple not in expected_set:
                    return False, {
                        "reason": f"Store '{store}' has customer not in expected top 35",
                        "store": store,
                        "pipeline_customer": p_result,
                        "note": "Customer must be in top 35 candidates for this store",
                    }

        return True, {
            "store_count": len(pipeline_by_store),
            "total_customers": sum(len(v) for v in pipeline_by_store.values()),
            "validation_strategy": "verified all pipeline customers are in expected top 35 per store",
        }

    def _print_summary(self):
        """Print validation summary."""
        print(f"\n{'='*60}")
        print("VALIDATION SUMMARY")
        print(f"{'='*60}")
        print(f"Dataset Mode: {self.dataset_mode}")
        print(f"Total Queries: {self.report['summary']['total']}")
        print(f"✅ Passed: {self.report['summary']['passed']}")
        if self.report["summary"]["failed"] > 0:
            print(f"❌ Failed: {self.report['summary']['failed']}")

    def _save_report(self):
        """Save validation report to disk."""
        report_file = Path(".results") / f"validation_report_{self.dataset_mode}.json"
        with open(report_file, "w") as f:
            json.dump(self.report, f, indent=2)
        print(f"\nDetailed report saved to: {report_file}")


def detect_dataset_mode() -> str:
    """Auto-detect dataset mode from config file."""
    try:
        config_file = Path("compose_config.json")
        if config_file.exists():
            with open(config_file) as f:
                config = json.load(f)
            dataset_full = config.get("dataset", {}).get("full", "false").lower() == "true"
            return "full" if dataset_full else "min"
    except Exception as e:
        print(f"Warning: Could not read config file: {e}")

    return "min"  # Default


def main():
    parser = argparse.ArgumentParser(
        description="Validate pipeline results against expected outputs",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python validation.py --dataset min
  python validation.py --dataset min --session <uuid>
  python validation.py --dataset full --session <uuid>
        """,
    )
    parser.add_argument(
        "--dataset",
        choices=["min", "full"],
        help="Dataset type (auto-detected from compose_config.json if not specified)",
    )
    parser.add_argument(
        "--session",
        type=str,
        help="Session ID (UUID) to validate. If not specified, uses .results/pipeline/",
    )
    args = parser.parse_args()

    # Determine dataset mode
    if args.dataset:
        mode = args.dataset
        print(f"Using dataset mode from command line: {mode}")
    else:
        mode = detect_dataset_mode()
        print(f"Auto-detected dataset mode from config: {mode}")

    # Session
    if args.session:
        print(f"Validating session: {args.session}")

    # Validate
    validator = ResultsValidator(dataset_mode=mode, session_id=args.session)
    success = validator.validate_all()

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
