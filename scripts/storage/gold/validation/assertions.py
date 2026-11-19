"""
Gold Layer Reusable DataFrame Validation Assertions.

Extends Silver assertions with Gold-specific checks:
- FK referential integrity
- Aggregate reconciliation (child sums == parent)
- Orphan detection (FK not null -> parent exists)
"""

from dataclasses import dataclass, field
from typing import List, Callable, Optional, Any, Union
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


# ==================== CORE ASSERTIONS (from Silver) ====================

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
    
    violations = []
    for col in columns:
        if col not in df.columns:
            violations.append(f"column '{col}' not found")
            continue
        null_count = df.filter(F.col(col).isNull()).count()
        if null_count > 0:
            violations.append(f"'{col}': {null_count} nulls")
    
    if violations:
        return _make_result(
            name, False,
            f"Null violations: {'; '.join(violations)}",
            violations=violations
        )
    
    return _make_result(name, True, f"No nulls in {columns}")


def assert_column_values_in_set(
    df: DataFrame,
    column: str,
    allowed_values: List[Any],
    check_name: Optional[str] = None,
    case_insensitive: bool = False
) -> ValidationResult:
    """
    Assert column values are within allowed set.
    """
    name = check_name or f"values_in_set_{column}"
    
    if column not in df.columns:
        return _make_result(name, False, f"Column '{column}' not found")
    
    col_expr = F.upper(F.trim(F.col(column))) if case_insensitive else F.col(column)
    allowed = [str(v).upper() if case_insensitive else v for v in allowed_values]
    
    violations = df.filter(~col_expr.isin(allowed) & F.col(column).isNotNull()).count()
    
    if violations > 0:
        sample = df.filter(~col_expr.isin(allowed) & F.col(column).isNotNull()).select(column).distinct().limit(10).collect()
        bad_values = [row[column] for row in sample]
        return _make_result(
            name, False,
            f"Column '{column}' has {violations} values outside allowed set",
            violation_count=violations,
            sample_bad_values=bad_values,
            allowed_values=allowed_values
        )
    
    return _make_result(name, True, f"All values in '{column}' within allowed set")
# ==================== SCHEMA AND ROW COUNT ====================

def assert_schema_conformance(
    df: DataFrame,
    expected_schema: StructType,
    check_name: Optional[str] = None,
    strict: bool = True
) -> ValidationResult:
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
        return _make_result(name, False, f"Schema validation failed: {'; '.join(errors)}", missing=sorted(missing), extra=sorted(extra), type_mismatches=type_mismatches)
    return _make_result(name, True, "Schema conforms to expected contract")


def assert_date_bounds(
    df: DataFrame,
    column: str,
    min_date: Optional[str] = None,
    max_date: Optional[str] = None,
    check_name: Optional[str] = None
) -> ValidationResult:
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
        return _make_result(name, False, f"Column '{column}' has {violations} values outside bounds [{min_date}, {max_date}]", violation_count=violations, min_date=min_date, max_date=max_date)
    return _make_result(name, True, f"All values in '{column}' within bounds")


def assert_row_count_match(
    df_expected: DataFrame,
    df_actual: DataFrame,
    check_name: Optional[str] = None
) -> ValidationResult:
    name = check_name or "row_count_match"
    expected = df_expected.count()
    actual = df_actual.count()
    if expected != actual:
        return _make_result(name, False, f"Row count mismatch: expected {expected}, got {actual}", expected=expected, actual=actual, diff=abs(expected - actual))
    return _make_result(name, True, f"Row count matches: {expected}")
# ==================== GOLD-SPECIFIC ASSERTIONS ====================

def assert_fk_integrity(
    df_child: DataFrame,
    fk_column: str,
    df_parent: DataFrame,
    pk_column: str,
    check_name: Optional[str] = None,
    allow_null_fk: bool = True
) -> ValidationResult:
    name = check_name or f"fk_integrity_{fk_column}_to_{pk_column}"
    if fk_column not in df_child.columns:
        return _make_result(name, False, f"FK column '{fk_column}' not found in child")
    if pk_column not in df_parent.columns:
        return _make_result(name, False, f"PK column '{pk_column}' not found in parent")
    parent_keys = df_parent.select(pk_column).distinct()
    if allow_null_fk:
        orphans = df_child.join(parent_keys, df_child[fk_column] == parent_keys[pk_column], how="left_anti").filter(F.col(fk_column).isNotNull())
    else:
        orphans = df_child.join(parent_keys, df_child[fk_column] == parent_keys[pk_column], how="left_anti")
    orphan_count = orphans.count()
    if orphan_count > 0:
        sample = orphans.select(fk_column).distinct().limit(10).collect()
        bad_values = [row[fk_column] for row in sample]
        return _make_result(name, False, f"FK integrity violation: {orphan_count} rows in child have {fk_column} not found in parent {pk_column}", orphan_count=orphan_count, sample_orphan_values=bad_values)
    return _make_result(name, True, f"FK {fk_column} -> {pk_column} integrity OK")


def assert_no_orphans(
    df_child: DataFrame,
    fk_column: str,
    df_parent: DataFrame,
    pk_column: str,
    check_name: Optional[str] = None
) -> ValidationResult:
    return assert_fk_integrity(df_child, fk_column, df_parent, pk_column, check_name=check_name or f"no_orphans_{fk_column}", allow_null_fk=True)


def assert_aggregate_reconciliation(
    df_parent: DataFrame,
    df_child: DataFrame,
    parent_group_cols: List[str],
    child_group_cols: List[str],
    measure_cols: List[str],
    agg_func: str = "sum",
    tolerance: float = 0.01,
    check_name: Optional[str] = None,
    child_measure_exprs: Optional[dict] = None,
) -> ValidationResult:
    """
    Assert aggregate reconciliation between parent and child DataFrames.
    
    Args:
        df_parent: Parent DataFrame (pre-aggregated, e.g., fact_video_engagement)
        df_child: Child DataFrame (raw, e.g., fact_event)
        parent_group_cols: Grouping columns in parent
        child_group_cols: Grouping columns in child
        measure_cols: Measure column names in parent to reconcile
        agg_func: Aggregation function ('sum', 'count', 'countDistinct')
        tolerance: Relative tolerance for mismatch (default 1%)
        check_name: Optional custom check name
        child_measure_exprs: Optional dict mapping parent measure column name -> 
            Spark column expression on child DataFrame. If not provided, assumes
            child has same column names as parent.
            
            Example: {"views": F.when(F.col("interaction_type") == "play", 1).otherwise(0),
                      "completes": F.when(F.col("interaction_type") == "complete", 1).otherwise(0)}
    """
    name = check_name or f"aggregate_reconciliation_{'_'.join(measure_cols)}"
    
    # Build aggregation expressions for child
    if child_measure_exprs:
        # Use custom expressions for each measure column
        agg_exprs = []
        for col in measure_cols:
            if col in child_measure_exprs:
                expr = child_measure_exprs[col]
            else:
                expr = F.col(col)  # Fallback to same column name
            if agg_func == "sum":
                agg_exprs.append(F.sum(expr).alias(col))
            elif agg_func == "count":
                agg_exprs.append(F.count(expr).alias(col))
            elif agg_func == "countDistinct":
                agg_exprs.append(F.countDistinct(expr).alias(col))
            else:
                return _make_result(name, False, f"Unsupported agg_func: {agg_func}")
    else:
        # Original behavior: assume child has same column names
        if agg_func == "sum":
            agg_exprs = [F.sum(F.col(c)).alias(c) for c in measure_cols]
        elif agg_func == "count":
            agg_exprs = [F.count(F.col(c)).alias(c) for c in measure_cols]
        elif agg_func == "countDistinct":
            agg_exprs = [F.countDistinct(F.col(c)).alias(c) for c in measure_cols]
        else:
            return _make_result(name, False, f"Unsupported agg_func: {agg_func}")
    
    child_agg = df_child.groupBy(*child_group_cols).agg(*agg_exprs)
    
    # Alias child aggregation columns to child_{col} BEFORE join to avoid ambiguous column names
    child_agg_aliased = child_agg
    for col in measure_cols:
        child_agg_aliased = child_agg_aliased.withColumnRenamed(col, f"child_{col}")
    
    join_cond = [df_parent[pc] == child_agg_aliased[cc] for pc, cc in zip(parent_group_cols, child_group_cols)]
    joined = df_parent.join(child_agg_aliased, join_cond, how="full")
    
    violations = []
    details = {"measure_cols": measure_cols, "agg_func": agg_func}
    for col in measure_cols:
        parent_col = f"parent_{col}"
        child_col = f"child_{col}"
        # Rename parent measure column to parent_{col} for clarity
        joined = joined.withColumnRenamed(col, parent_col) if col in df_parent.columns else joined
        diff_col = F.abs(F.coalesce(F.col(parent_col), F.lit(0)) - F.coalesce(F.col(child_col), F.lit(0)))
        rel_diff = F.when(F.coalesce(F.col(parent_col), F.lit(0)) != 0, diff_col / F.abs(F.coalesce(F.col(parent_col), F.lit(0)))).otherwise(F.lit(0))
        mismatch_count = joined.filter(rel_diff > tolerance).count()
        if mismatch_count > 0:
            violations.append(f"{col}: {mismatch_count} groups exceed {tolerance*100}% tolerance")
            sample = joined.filter(rel_diff > tolerance).select(*[df_parent[c] for c in parent_group_cols], parent_col, child_col).limit(5).collect()
            details[f"{col}_samples"] = [row.asDict() for row in sample]
    if violations:
        return _make_result(name, False, f"Aggregate reconciliation failed: {'; '.join(violations)}", **details)
    return _make_result(name, True, f"Aggregate reconciliation OK for {measure_cols} ({agg_func})")
def assert_rate_bounds(
    df: DataFrame,
    rate_column: str,
    min_val: float = 0.0,
    max_val: float = 1.0,
    check_name: Optional[str] = None
) -> ValidationResult:
    name = check_name or f"rate_bounds_{rate_column}"
    if rate_column not in df.columns:
        return _make_result(name, False, f"Column '{rate_column}' not found")
    violations = df.filter((F.col(rate_column).isNotNull()) & ((F.col(rate_column) < min_val) | (F.col(rate_column) > max_val))).count()
    if violations > 0:
        return _make_result(name, False, f"Rate column '{rate_column}' has {violations} values outside [{min_val}, {max_val}]", violation_count=violations, min_val=min_val, max_val=max_val)
    return _make_result(name, True, f"All values in '{rate_column}' within [{min_val}, {max_val}]")


def assert_boolean_consistency(
    df: DataFrame,
    boolean_col: str,
    condition_col: str,
    condition_expr,
    check_name: Optional[str] = None
) -> ValidationResult:
    name = check_name or f"boolean_consistency_{boolean_col}"
    if boolean_col not in df.columns:
        return _make_result(name, False, f"Boolean column '{boolean_col}' not found")
    if condition_col not in df.columns:
        return _make_result(name, False, f"Condition column '{condition_col}' not found")
    inconsistent = df.filter((F.col(boolean_col) != condition_expr) & F.col(boolean_col).isNotNull() & F.col(condition_col).isNotNull()).count()
    if inconsistent > 0:
        return _make_result(name, False, f"Boolean '{boolean_col}' inconsistent with condition in {inconsistent} rows", inconsistent_count=inconsistent)
    return _make_result(name, True, f"Boolean '{boolean_col}' consistent with condition")


def assert_flag_rate_below_threshold(
    df: DataFrame,
    flag_column: str,
    max_rate: float = 0.05,
    check_name: Optional[str] = None
) -> ValidationResult:
    name = check_name or f"flag_rate_{flag_column}"
    if flag_column not in df.columns:
        return _make_result(name, False, f"Flag column '{flag_column}' not found")
    total = df.count()
    if total == 0:
        return _make_result(name, True, "Empty DataFrame, flag rate check skipped")
    true_count = df.filter(F.col(flag_column) == True).count()
    rate = true_count / total
    if rate > max_rate:
        return _make_result(name, False, f"Flag '{flag_column}' true rate {rate:.2%} exceeds threshold {max_rate:.2%}", flag_rate=rate, threshold=max_rate, true_count=true_count, total=total)
    return _make_result(name, True, f"Flag '{flag_column}' rate {rate:.2%} within threshold {max_rate:.2%}")


def assert_not_null_rate_above_threshold(
    df: DataFrame,
    column: str,
    min_not_null_rate: float = 0.90,
    check_name: Optional[str] = None
) -> ValidationResult:
    name = check_name or f"not_null_rate_{column}"
    if column not in df.columns:
        return _make_result(name, False, f"Column '{column}' not found")
    total = df.count()
    if total == 0:
        return _make_result(name, True, "Empty DataFrame, not-null rate check skipped")
    not_null_count = df.filter(F.col(column).isNotNull()).count()
    rate = not_null_count / total
    if rate < min_not_null_rate:
        return _make_result(name, False, f"Column '{column}' not-null rate {rate:.2%} below threshold {min_not_null_rate:.2%}", not_null_rate=rate, threshold=min_not_null_rate, not_null_count=not_null_count, total=total)
    return _make_result(name, True, f"Column '{column}' not-null rate {rate:.2%} above threshold {min_not_null_rate:.2%}")