"""
Reusable DataFrame Validation Assertions.

Core validation functions that can be composed into validation suites.
All functions are pure - they take DataFrames and return ValidationResult objects.
No I/O, no side effects, fully testable.
"""
from dataclasses import dataclass, field
from typing import List, Callable, Optional, Any
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import StructType


@dataclass
class ValidationResult:
    """Result of a single validation check."""
    check_name: str
    passed: bool
    message: str
    details: dict = field(default_factory=dict)
    
    def __bool__(self) -> bool:
        return self.passed


@dataclass
class ValidationSuite:
    """Collection of validation results with summary."""
    results: List[ValidationResult] = field(default_factory=list)
    
    def add(self, result: ValidationResult) -> "ValidationSuite":
        self.results.append(result)
        return self
    
    @property
    def passed(self) -> int:
        return sum(1 for r in self.results if r.passed)
    
    @property
    def failed(self) -> int:
        return sum(1 for r in self.results if not r.passed)
    
    @property
    def all_passed(self) -> bool:
        return all(r.passed for r in self.results)
    
    @property
    def errors(self) -> List[str]:
        return [r.message for r in self.results if not r.passed]
    
    def raise_if_failed(self, context: str = "") -> None:
        """Raise AssertionError if any validations failed."""
        if not self.all_passed:
            ctx = f" ({context})" if context else ""
            raise AssertionError(
                f"Validation failed{ctx}: {self.failed} checks failed. "
                f"Errors: {'; '.join(self.errors)}"
            )
    
    def __bool__(self) -> bool:
        return self.all_passed


def _make_result(check_name: str, passed: bool, message: str, **details) -> ValidationResult:
    """Helper to create ValidationResult."""
    return ValidationResult(check_name=check_name, passed=passed, message=message, details=details)
# ==================== CORE ASSERTIONS ====================

def assert_pk_unique(
    df: DataFrame,
    pk_columns: List[str],
    check_name: Optional[str] = None
) -> ValidationResult:
    """
    Assert primary key uniqueness.
    
    Args:
        df: DataFrame to validate
        pk_columns: List of column names forming the primary key
        check_name: Optional custom name for this check
        
    Returns:
        ValidationResult with pass/fail status
    """
    name = check_name or f"pk_unique_{'_'.join(pk_columns)}"
    
    if not pk_columns:
        return _make_result(name, False, "No primary key columns provided")
    
    dup_count = df.groupBy(*pk_columns).count().filter(F.col("count") > 1).count()
    
    if dup_count > 0:
        return _make_result(
            name, False,
            f"Primary key violation: {dup_count} duplicate key groups found in {pk_columns}",
            duplicate_count=dup_count
        )
    
    return _make_result(name, True, f"Primary key {pk_columns} is unique")


def assert_no_nulls(
    df: DataFrame,
    columns: List[str],
    check_name: Optional[str] = None
) -> ValidationResult:
    """
    Assert specified columns have no null values.
    """
    name = check_name or f"no_nulls_{'_'.join(columns)}"
    
    if not columns:
        return _make_result(name, True, "No columns specified for null check")
    
    violations = {}
    for col in columns:
        if col not in df.columns:
            violations[col] = "column_not_found"
            continue
        null_count = df.filter(F.col(col).isNull()).count()
        if null_count > 0:
            violations[col] = null_count
    
    if violations:
        return _make_result(
            name, False,
            f"Null violations in columns: {violations}",
            violations=violations
        )
    
    return _make_result(name, True, f"No nulls in {columns}")


def assert_column_values_in_set(
    df: DataFrame,
    column: str,
    valid_values: set,
    check_name: Optional[str] = None,
    allow_null: bool = True
) -> ValidationResult:
    """
    Assert a column's values are all within an allowed set.
    """
    name = check_name or f"values_in_set_{column}"
    
    if column not in df.columns:
        return _make_result(name, False, f"Column '{column}' not found")
    
    if allow_null:
        invalid_filter = ~F.col(column).isin(*list(valid_values)) & F.col(column).isNotNull()
    else:
        invalid_filter = ~F.col(column).isin(*list(valid_values))
    
    invalid_count = df.filter(invalid_filter).count()
    
    if invalid_count > 0:
        samples = df.filter(invalid_filter).select(column).distinct().limit(10).collect()
        sample_values = [row[column] for row in samples]
        
        return _make_result(
            name, False,
            f"Column '{column}' has {invalid_count} values outside allowed set",
            invalid_count=invalid_count,
            sample_invalid=sample_values,
            allowed_values=list(valid_values)
        )
    
    return _make_result(name, True, f"All values in '{column}' are valid")
def assert_no_timestamp_drift(
    bronze_df: DataFrame,
    silver_df: DataFrame,
    pk_columns: List[str],
    timestamp_columns: List[str],
    check_name: Optional[str] = None,
    tolerance_seconds: int = 0
) -> ValidationResult:
    """
    Assert timestamps are preserved between Bronze and Silver for matching keys.
    """
    name = check_name or "no_timestamp_drift"
    
    if not pk_columns or not timestamp_columns:
        return _make_result(name, False, "PK and timestamp columns required")
    
    bronze_select = bronze_df.select(*pk_columns, *timestamp_columns)
    silver_select = silver_df.select(*pk_columns, *timestamp_columns)
    
    bronze_ts = bronze_select.collect()
    silver_ts = silver_select.collect()
    
    bronze_dict = {tuple(row[pk] for pk in pk_columns): row for row in bronze_ts}
    silver_dict = {tuple(row[pk] for pk in pk_columns): row for row in silver_ts}
    
    total_compared = 0
    mismatches = 0
    mismatch_details = {}
    
    for pk, bronze_row in bronze_dict.items():
        silver_row = silver_dict.get(pk)
        if silver_row is None:
            continue
        total_compared += 1
        
        for ts_col in timestamp_columns:
            bronze_val = bronze_row[ts_col]
            silver_val = silver_row[ts_col]
            
            if bronze_val is not None and silver_val is not None:
                diff = abs((bronze_val - silver_val).total_seconds())
                if diff > tolerance_seconds:
                    mismatches += 1
                    if mismatches <= 5:
                        mismatch_details.setdefault(ts_col, []).append({
                            "pk": pk,
                            "bronze": str(bronze_val),
                            "silver": str(silver_val),
                            "diff_seconds": diff
                        })
    
    if total_compared == 0:
        return _make_result(
            name, False,
            "No matching keys found between Bronze and Silver for timestamp comparison"
        )
    
    if mismatches > 0:
        return _make_result(
            name, False,
            f"Timestamp drift detected: {mismatches}/{total_compared} records exceed {tolerance_seconds}s tolerance",
            mismatch_count=mismatches,
            total_compared=total_compared,
            details=mismatch_details
        )
    
    return _make_result(name, True, f"All {len(timestamp_columns)} timestamp columns preserved within tolerance")


def assert_flag_consistency(
    df: DataFrame,
    flag_column: str,
    condition_column: str,
    expected_true_sql: str,
    check_name: Optional[str] = None
) -> ValidationResult:
    """
    Assert a boolean flag column matches its defining SQL condition.
    """
    name = check_name or f"flag_consistency_{flag_column}"
    
    if flag_column not in df.columns or condition_column not in df.columns:
        return _make_result(name, False, f"Columns not found: {flag_column}, {condition_column}")
    
    false_positives = df.filter(
        (F.col(flag_column) == True) & (~F.expr(expected_true_sql))
    ).count()
    
    false_negatives = df.filter(
        (F.col(flag_column) == False) & (F.expr(expected_true_sql))
    ).count()
    
    if false_positives > 0 or false_negatives > 0:
        return _make_result(
            name, False,
            f"Flag '{flag_column}' inconsistent: {false_positives} false positives, "
            f"{false_negatives} false negatives",
            false_positives=false_positives,
            false_negatives=false_negatives
        )
    
    return _make_result(name, True, f"Flag '{flag_column}' consistent with condition")


def assert_row_count_match(
    df_expected: DataFrame,
    df_actual: DataFrame,
    check_name: Optional[str] = None
) -> ValidationResult:
    """
    Assert two DataFrames have the same row count.
    """
    name = check_name or "row_count_match"
    
    expected = df_expected.count()
    actual = df_actual.count()
    
    if expected != actual:
        return _make_result(
            name, False,
            f"Row count mismatch: expected {expected}, got {actual}",
            expected=expected,
            actual=actual,
            diff=abs(expected - actual)
        )
    
    return _make_result(name, True, f"Row count matches: {expected}")
def assert_schema_conformance(
    df: DataFrame,
    expected_schema: StructType,
    check_name: Optional[str] = None,
    strict: bool = True
) -> ValidationResult:
    """
    Assert DataFrame conforms to expected schema.
    """
    name = check_name or "schema_conformance"
    
    actual_fields = {f.name: f.dataType for f in df.schema.fields}
    expected_fields = {f.name: f.dataType for f in expected_schema.fields}
    
    missing = set(expected_fields) - set(actual_fields)
    extra = set(actual_fields) - set(expected_fields)
    
    type_mismatches = {}
    for col, expected_type in expected_fields.items():
        if col in actual_fields and actual_fields[col] != expected_type:
            type_mismatches[col] = (str(actual_fields[col]), str(expected_type))
    
    errors = []
    if missing:
        errors.append(f"missing columns: {sorted(missing)}")
    if strict and extra:
        errors.append(f"extra columns: {sorted(extra)}")
    if type_mismatches:
        errors.append(f"type mismatches: {type_mismatches}")
    
    if errors:
        return _make_result(
            name, False,
            f"Schema validation failed: {'; '.join(errors)}",
            missing=sorted(missing),
            extra=sorted(extra),
            type_mismatches=type_mismatches
        )
    
    return _make_result(name, True, "Schema conforms to expected contract")


def assert_date_bounds(
    df: DataFrame,
    column: str,
    min_date: Optional[str] = None,
    max_date: Optional[str] = None,
    check_name: Optional[str] = None
) -> ValidationResult:
    """
    Assert a date/timestamp column falls within expected bounds.
    """
    name = check_name or f"date_bounds_{column}"
    
    if column not in df.columns:
        return _make_result(name, False, f"Column '{column}' not found")
    
    filter_cond = F.lit(True)
    if min_date:
        filter_cond = filter_cond & (F.col(column) >= F.lit(min_date))
    if max_date:
        filter_cond = filter_cond & (F.col(column) <= F.lit(max_date))
    
    violations = df.filter(~filter_cond & F.col(column).isNotNull()).count()
    
    if violations > 0:
        return _make_result(
            name, False,
            f"Column '{column}' has {violations} values outside bounds [{min_date}, {max_date}]",
            violation_count=violations,
            min_date=min_date,
            max_date=max_date
        )
    
    return _make_result(name, True, f"All values in '{column}' within bounds")