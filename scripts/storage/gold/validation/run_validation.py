#!/usr/bin/env python
"""
Gold Layer Validation Master Runner.

Runs all validation suites in sequence:
1. Schema & Row Counts
2. Primary Keys & Foreign Key Integrity
3. Business Rules & Cross-Table Reconciliation

Usage:
    python -m scripts.storage.gold.validation.run_validation
    python -m scripts.storage.gold.validation.run_validation --suite schema
    python -m scripts.storage.gold.validation.run_validation --suite keys
    python -m scripts.storage.gold.validation.run_validation --suite business
"""

import argparse
import sys
from scripts.storage.config import SparkSessionManager
from scripts.storage.gold.validation.schema_and_counts import validate_all_schemas_and_counts
from scripts.storage.gold.validation.keys_and_integrity import validate_all_keys_and_integrity
from scripts.storage.gold.validation.business_rules import validate_all_business_rules
from scripts.storage.gold.validation.assertions import ValidationSuite


def run_validation(suite_name: str = "all") -> ValidationSuite:
    """
    Run specified validation suite(s).
    
    Args:
        suite_name: One of 'all', 'schema', 'keys', 'business'
    
    Returns:
        Combined ValidationSuite with all results
    """
    master_suite = ValidationSuite()
    
    with SparkSessionManager("gold_validation_master") as spark:
        if suite_name in ("all", "schema"):
            print("=" * 60)
            print("Running Schema & Row Count Validation...")
            print("=" * 60)
            schema_suite = validate_all_schemas_and_counts(spark)
            for r in schema_suite.results:
                master_suite.add(r)
            print(f"Schema & Counts: {schema_suite.passed} passed, {schema_suite.failed} failed")
        
        if suite_name in ("all", "keys"):
            print("=" * 60)
            print("Running Keys & Integrity Validation...")
            print("=" * 60)
            keys_suite = validate_all_keys_and_integrity(spark)
            for r in keys_suite.results:
                master_suite.add(r)
            print(f"Keys & Integrity: {keys_suite.passed} passed, {keys_suite.failed} failed")
        
        if suite_name in ("all", "business"):
            print("=" * 60)
            print("Running Business Rules Validation...")
            print("=" * 60)
            business_suite = validate_all_business_rules(spark)
            for r in business_suite.results:
                master_suite.add(r)
            print(f"Business Rules: {business_suite.passed} passed, {business_suite.failed} failed")
    
    return master_suite


def print_summary(suite: ValidationSuite, verbose: bool = False):
    """Print validation summary."""
    print("\n" + "=" * 60)
    print("GOLD LAYER VALIDATION SUMMARY")
    print("=" * 60)
    print(f"Total Checks: {len(suite.results)}")
    print(f"Passed:       {suite.passed}")
    print(f"Failed:       {suite.failed}")
    print(f"Success Rate: {suite.passed / len(suite.results) * 100:.1f}%" if suite.results else "N/A")
    print("=" * 60)
    
    if verbose or not suite.all_passed:
        print("\nDetailed Results:")
        for r in suite.results:
            status = "PASS" if r.passed else "FAIL"
            print(f"  [{status}] {r.check_name}: {r.message}")
            if not r.passed and r.details:
                for k, v in r.details.items():
                    print(f"    {k}: {v}")


def main():
    parser = argparse.ArgumentParser(description="Gold Layer Validation Runner")
    parser.add_argument(
        "--suite",
        choices=["all", "schema", "keys", "business"],
        default="all",
        help="Which validation suite to run (default: all)"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Print detailed results for all checks"
    )
    parser.add_argument(
        "--fail-fast",
        action="store_true",
        help="Exit on first failure"
    )
    
    args = parser.parse_args()
    
    print(f"Starting Gold Layer Validation (suite: {args.suite})...\n")
    
    suite = run_validation(args.suite)
    
    print_summary(suite, verbose=args.verbose)
    
    if not suite.all_passed:
        print("\nVALIDATION FAILED")
        if args.fail_fast:
            sys.exit(1)
        sys.exit(1)
    else:
        print("\nALL VALIDATIONS PASSED")
        sys.exit(0)


if __name__ == "__main__":
    main()