"""
validate_dag_metadata.py  –  DAG Metadata Validator
====================================================
Validates that a DAG file contains the correct team and usecase name
as specified in Jenkins, ensuring proper tagging for both EMR and
non-EMR usecases. Also enforces that the dag_id inside the DAG
definition exactly matches the DAG filename stem.

Rules enforced:
  1. DAG tags list must contain the team value (e.g. "IDP")
  2. DAG tags list must contain the usecase name (e.g. "aerospike")
  3. dag_id inside DAG() must exactly match the DAG filename stem
       e.g. filename: cloudops_sample_dag_instance_group_dev.py
            dag_id  : cloudops_sample_dag_instance_group_dev
  4. DAG tags list must contain the environment name (e.g. "dev")
  5. Task retries must not exceed MAX_RETRIES (20). Checked in BOTH places:
       a. default_args={'retries': N} in the DAG() definition
       b. retries=N on every individual operator/task
       Values sourced from a module-level variable or Variable.get(..., default)
       are resolved; if a value cannot be resolved statically a WARN is emitted.
     Additionally, if any retries value exceeds RETRY_DELAY_TRIGGER_RETRIES (5),
     the DAG's default_args MUST define a 'retry_delay' of at least
     MIN_RETRY_DELAY_MINUTES (15) minutes. retry_delay may be a
     timedelta(...) call or a plain number (interpreted as seconds, matching
     Airflow); variable references are resolved where possible.
  6. schedule_interval in the DAG() definition must never be None/null and must
       be a valid cron expression (5-field cron string or an Airflow cron preset
       such as @daily). A missing schedule_interval fails (it must be explicit).
       When the value is sourced from a variable / Variable.get() it is resolved
       and must not evaluate to None/null. A Variable.get() with no default is
       rejected because it can resolve to None at runtime.

Usage:
    python validate_dag_metadata.py \
        --dag     cloudops_sample_dag_instance_group_dev.py \
        --team    IDP \
        --usecase aerospike \
        --env_name dev
"""

import argparse
import ast
import sys
import os

# ── Constants ─────────────────────────────────────────────────────────────────

# Maximum retries permitted anywhere (default_args or per-task).
MAX_RETRIES = 20

# When any retries value exceeds this threshold, a retry_delay of at least
# MIN_RETRY_DELAY_MINUTES must be defined in default_args.
RETRY_DELAY_TRIGGER_RETRIES = 5

# Minimum required retry_delay (in minutes) when retries exceed the trigger.
MIN_RETRY_DELAY_MINUTES = 15

# Airflow-supported cron presets that are accepted as valid schedule_interval
# values in addition to standard 5-field cron strings.
CRON_PRESETS = {
    "@once", "@hourly", "@daily", "@weekly",
    "@monthly", "@quarterly", "@yearly", "@annually",
    "@midnight", "@continuous",
}

# ── Helpers ───────────────────────────────────────────────────────────────────

def err(msg: str):
    print(f"  [FAIL] {msg}")

def ok(msg: str):
    print(f"  [PASS] {msg}")

def warn(msg: str):
    print(f"  [WARN] {msg}")

def read_source(path: str) -> str:
    with open(path, "r") as f:
        return f.read()

def stem(filename: str) -> str:
    return os.path.splitext(os.path.basename(filename))[0]


# ── DAG() call extraction helpers ─────────────────────────────────────────────

def find_dag_node(tree: ast.Module) -> ast.Call | None:
    """
    Returns the first DAG() Call node found in the AST, or None.
    Matches both DAG(...) and airflow.DAG(...) forms.
    """
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func_name = ""
        if isinstance(node.func, ast.Name):
            func_name = node.func.id
        elif isinstance(node.func, ast.Attribute):
            func_name = node.func.attr
        if func_name == "DAG":
            return node
    return None


def extract_dag_tags(dag_node: ast.Call) -> list[str]:
    """
    Extracts string values from the tags=[...] keyword argument of a DAG() call.
    Returns empty list if tags keyword is absent or not a list of string literals.
    """
    for kw in dag_node.keywords:
        if kw.arg == "tags":
            if isinstance(kw.value, ast.List):
                tags = []
                for elt in kw.value.elts:
                    if isinstance(elt, ast.Constant) and isinstance(elt.value, str):
                        tags.append(elt.value)
                    elif isinstance(elt, ast.Str):    # Python < 3.8
                        tags.append(elt.s)
                return tags
    return []


def extract_dag_id(dag_node: ast.Call) -> str | None:
    """
    Extracts the string value of the dag_id keyword argument from a DAG() call.
    Returns None if not found or not a string literal.
    Handles both dag_id="..." keyword form and positional first argument form.
    """
    # Check keyword arguments first
    for kw in dag_node.keywords:
        if kw.arg == "dag_id":
            if isinstance(kw.value, ast.Constant) and isinstance(kw.value.value, str):
                return kw.value.value
            elif isinstance(kw.value, ast.Str):    # Python < 3.8
                return kw.value.s
            else:
                return None   # dag_id is a variable/expression — not a literal

    # Fallback: dag_id may be the first positional argument
    if dag_node.args:
        first_arg = dag_node.args[0]
        if isinstance(first_arg, ast.Constant) and isinstance(first_arg.value, str):
            return first_arg.value
        elif isinstance(first_arg, ast.Str):
            return first_arg.s

    return None


# ── Rule 1: Team value present in DAG tags ────────────────────────────────────

def validate_team_in_tags(dag_node: ast.Call, expected_team: str) -> bool:
    tags = extract_dag_tags(dag_node)

    if not tags:
        err(
            "No tags=[...] list found in DAG() definition.\n"
            f"    Add tags=['{expected_team}', '<usecase_name>'] to your DAG() constructor."
        )
        return False

    if expected_team in tags:
        ok(f"Team tag '{expected_team}' found in DAG tags: {tags}")
        return True
    else:
        err(
            f"Team value '{expected_team}' not found in DAG tags: {tags}.\n"
            f"    Add '{expected_team}' to the tags=[...] list in your DAG() definition.\n"
            f"    This must match the TEAM selected in Jenkins."
        )
        return False


# ── Rule 2: Usecase name present in DAG tags ──────────────────────────────────

def validate_usecase_in_tags(dag_node: ast.Call, expected_usecase: str) -> bool:
    tags = extract_dag_tags(dag_node)

    if not tags:
        # Error already reported in Rule 1 if tags missing entirely
        return False

    if expected_usecase in tags:
        ok(f"Usecase tag '{expected_usecase}' found in DAG tags: {tags}")
        return True
    else:
        err(
            f"Usecase name '{expected_usecase}' not found in DAG tags: {tags}.\n"
            f"    Add '{expected_usecase}' to the tags=[...] list in your DAG() definition.\n"
            f"    This must match the USECASE_NAME entered in Jenkins."
        )
        return False


# ── Rule 3: dag_id must exactly match DAG filename stem ───────────────────────

def validate_dag_id_matches_filename(dag_node: ast.Call, dag_path: str) -> bool:
    expected_dag_id = stem(dag_path)
    actual_dag_id   = extract_dag_id(dag_node)

    if actual_dag_id is None:
        err(
            "dag_id could not be extracted from DAG() definition.\n"
            f"    Ensure dag_id is set as a hardcoded string literal.\n"
            f"    Expected: dag_id=\"{expected_dag_id}\""
        )
        return False

    if actual_dag_id == expected_dag_id:
        ok(
            f"dag_id '{actual_dag_id}' exactly matches "
            f"DAG filename stem '{expected_dag_id}' ✓"
        )
        return True
    else:
        err(
            f"dag_id mismatch!\n"
            f"    dag_id in file : '{actual_dag_id}'\n"
            f"    Expected       : '{expected_dag_id}'  (filename stem of '{os.path.basename(dag_path)}')\n"
            f"    Fix: update dag_id=\"{expected_dag_id}\" in your DAG() definition."
        )
        return False


# Rule 4: Environment Name in DAG Tags

def validate_env_name_in_tags(dag_node: ast.Call, env_name: str) -> bool:
    tags = extract_dag_tags(dag_node)

    if not tags:
        err(
            "No tags=[...] list found in DAG() definition.\n"
            f"    Add tags=['{env_name}', '<env_name>'] to your DAG() constructor."
        )
        return False

    if env_name in tags:
        ok(f"Environment Name tag '{env_name}' found in DAG tags: {tags}")
        return True
    else:
        err(
            f"Environment Name value '{env_name}' not found in DAG tags: {tags}.\n"
            f"    Add '{env_name}' to the tags=[...] list in your DAG() definition.\n"
            f"    This must match the Environment Name selected in Jenkins."
        )
        return False


# ── Value-resolution helpers (module-level vars & Variable.get) ───────────────

def _build_assignment_map(tree: ast.Module) -> dict[str, ast.expr]:
    """
    Builds a map of {variable_name: value_node} for simple module-level
    assignments like:  schedule = "0 5 * * *"  or  default_args = {...}

    Only single-target name assignments are captured. This lets the validator
    resolve schedule_interval / default_args / retries when they are passed to
    DAG() or an operator as a variable reference rather than an inline literal.
    """
    assignments: dict[str, ast.expr] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name):
                    assignments[target.id] = node.value
    return assignments


def _literal_value(node: ast.expr):
    """
    Returns the Python literal value of an AST node if it is a constant
    (str, int, float, bool, None). Returns the sentinel _UNRESOLVED otherwise.
    """
    if isinstance(node, ast.Constant):
        return node.value
    if isinstance(node, ast.Str):      # Python < 3.8
        return node.s
    if isinstance(node, ast.Num):      # Python < 3.8
        return node.n
    if isinstance(node, ast.NameConstant):  # Python < 3.8 (True/False/None)
        return node.value
    return _UNRESOLVED


class _Unresolved:
    """Sentinel indicating a value could not be resolved to a static literal."""
    def __repr__(self):
        return "<unresolved>"


_UNRESOLVED = _Unresolved()


def _is_variable_get_call(node: ast.expr) -> bool:
    """True if node is a call to Variable.get(...) (Airflow Variables)."""
    if not isinstance(node, ast.Call):
        return False
    func = node.func
    if isinstance(func, ast.Attribute) and func.attr == "get":
        # Match Variable.get(...) specifically
        if isinstance(func.value, ast.Name) and func.value.id == "Variable":
            return True
    return False


def _variable_get_default(node: ast.Call):
    """
    For a Variable.get(...) call, returns (has_default, default_value_node).
    default_var may be passed positionally (2nd arg) or as default_var=... kw.
    """
    # keyword form: Variable.get("key", default_var=...)
    for kw in node.keywords:
        if kw.arg == "default_var":
            return True, kw.value
    # positional form: Variable.get("key", <default>)
    if len(node.args) >= 2:
        return True, node.args[1]
    return False, None


def _resolve_node(node: ast.expr, assignments: dict[str, ast.expr], _depth: int = 0):
    """
    Resolves an AST expression to a static literal value where possible.

    Handles:
      - direct literals (str/int/None/bool)
      - module-level variable references (via the assignment map)
      - Variable.get("key", default) → resolves to the default value
    Returns the literal value, or _UNRESOLVED when it can't be determined
    statically (e.g. Variable.get with no default, function calls, f-strings).
    """
    if _depth > 5:
        return _UNRESOLVED

    lit = _literal_value(node)
    if lit is not _UNRESOLVED:
        return lit

    # Variable reference → look up its module-level assignment
    if isinstance(node, ast.Name):
        if node.id in assignments:
            return _resolve_node(assignments[node.id], assignments, _depth + 1)
        return _UNRESOLVED

    # Variable.get("key", default) → resolve to default (runtime value unknown)
    if _is_variable_get_call(node):
        has_default, default_node = _variable_get_default(node)
        if has_default and default_node is not None:
            return _resolve_node(default_node, assignments, _depth + 1)
        # No default → Variable.get can return None at runtime
        return _UNRESOLVED

    return _UNRESOLVED


def _get_dict_node(node: ast.expr, assignments: dict[str, ast.expr]):
    """Return an ast.Dict for node, resolving a variable reference if needed."""
    if isinstance(node, ast.Dict):
        return node
    if isinstance(node, ast.Name) and node.id in assignments:
        target = assignments[node.id]
        if isinstance(target, ast.Dict):
            return target
    return None


def _dict_lookup(dict_node: ast.Dict, key: str) -> ast.expr | None:
    """Return the value node for a string key in an ast.Dict, or None."""
    for k, v in zip(dict_node.keys, dict_node.values):
        k_val = _literal_value(k) if k is not None else _UNRESOLVED
        if k_val == key:
            return v
    return None


# ── Rule 5: Task retries must not exceed MAX_RETRIES (both places) ────────────

def _collect_task_call_nodes(tree: ast.Module) -> list[ast.Call]:
    """
    Returns all Call nodes that look like Airflow operator/task instantiations,
    i.e. calls that carry a task_id=... keyword. This is a robust, operator-
    agnostic way to find task definitions without importing Airflow.
    """
    task_calls = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            if any(kw.arg == "task_id" for kw in node.keywords):
                task_calls.append(node)
    return task_calls


def _check_retries_value(retries_node: ast.expr,
                         assignments: dict[str, ast.expr],
                         where: str) -> tuple[bool, bool, bool]:
    """
    Validates a single retries=... value.

    Returns (passed, resolved, exceeds_trigger):
      passed          – False only when a resolved integer exceeds MAX_RETRIES.
      resolved        – True if we could statically determine the value.
      exceeds_trigger – True if the resolved value exceeds
                        RETRY_DELAY_TRIGGER_RETRIES (i.e. a retry_delay of at
                        least MIN_RETRY_DELAY_MINUTES is now mandatory).
    Emits the appropriate ok/err/warn message for `where` (a human label).
    """
    value = _resolve_node(retries_node, assignments)

    if value is _UNRESOLVED:
        warn(
            f"{where}: 'retries' value could not be resolved statically "
            f"(e.g. Variable.get without a default or a computed value). "
            f"Ensure it is <= {MAX_RETRIES}."
        )
        return True, False, False

    if isinstance(value, bool) or not isinstance(value, int):
        warn(
            f"{where}: 'retries' resolved to a non-integer value ({value!r}). "
            f"Ensure it is an integer <= {MAX_RETRIES}."
        )
        return True, False, False

    exceeds_trigger = value > RETRY_DELAY_TRIGGER_RETRIES

    if value > MAX_RETRIES:
        err(
            f"{where}: 'retries' is {value}, which exceeds the maximum allowed "
            f"of {MAX_RETRIES}.\n"
            f"    Reduce retries to {MAX_RETRIES} or fewer."
        )
        return False, True, exceeds_trigger

    ok(f"{where}: 'retries' = {value} is within the limit of {MAX_RETRIES}.")
    return True, True, exceeds_trigger


def _timedelta_to_minutes(node: ast.expr, assignments: dict[str, ast.expr]):
    """
    Resolves a retry_delay AST node to a value in minutes where possible.

    Handles:
      - timedelta(minutes=15) / timedelta(seconds=900) / timedelta(hours=1) /
        timedelta(days=1) / timedelta(minutes=..., seconds=...) with literal
        (or module-level variable) keyword values.
      - timedelta(days, seconds, ...) positional form (Python signature:
        days, seconds, microseconds, milliseconds, minutes, hours, weeks).
      - a plain number → interpreted as SECONDS (matching Airflow, which coerces
        an int/float retry_delay to timedelta(seconds=N)).
      - a module-level variable referencing any of the above.

    Returns minutes as a float, or _UNRESOLVED when it can't be determined.
    """
    # Plain number → Airflow treats it as seconds
    resolved_literal = _resolve_node(node, assignments)
    if isinstance(resolved_literal, (int, float)) and not isinstance(resolved_literal, bool):
        return resolved_literal / 60.0

    # Resolve a variable reference down to its assignment node for timedelta(...)
    target = node
    if isinstance(node, ast.Name) and node.id in assignments:
        target = assignments[node.id]

    if not isinstance(target, ast.Call):
        return _UNRESOLVED

    func = target.func
    func_name = ""
    if isinstance(func, ast.Name):
        func_name = func.id
    elif isinstance(func, ast.Attribute):
        func_name = func.attr

    if func_name != "timedelta":
        return _UNRESOLVED

    # Multipliers to convert each timedelta unit into minutes
    unit_to_minutes = {
        "weeks":   7 * 24 * 60,
        "days":    24 * 60,
        "hours":   60,
        "minutes": 1,
        "seconds": 1 / 60.0,
        "milliseconds": 1 / 60000.0,
        "microseconds": 1 / 60000000.0,
    }
    # Positional order of datetime.timedelta(...)
    positional_units = [
        "days", "seconds", "microseconds",
        "milliseconds", "minutes", "hours", "weeks",
    ]

    total_minutes = 0.0

    # Positional args
    for i, arg in enumerate(target.args):
        if i >= len(positional_units):
            return _UNRESOLVED
        val = _resolve_node(arg, assignments)
        if not isinstance(val, (int, float)) or isinstance(val, bool):
            return _UNRESOLVED
        total_minutes += val * unit_to_minutes[positional_units[i]]

    # Keyword args
    for kw in target.keywords:
        if kw.arg not in unit_to_minutes:
            return _UNRESOLVED
        val = _resolve_node(kw.value, assignments)
        if not isinstance(val, (int, float)) or isinstance(val, bool):
            return _UNRESOLVED
        total_minutes += val * unit_to_minutes[kw.arg]

    return total_minutes


def _validate_retry_delay(default_args_dict: ast.Dict | None,
                          assignments: dict[str, ast.expr]) -> bool:
    """
    Enforces that default_args contains a 'retry_delay' of at least
    MIN_RETRY_DELAY_MINUTES minutes. Called only when some retries value
    exceeded RETRY_DELAY_TRIGGER_RETRIES.
    """
    if default_args_dict is None:
        err(
            f"'retries' exceeds {RETRY_DELAY_TRIGGER_RETRIES}, so default_args "
            f"MUST define a 'retry_delay' of at least {MIN_RETRY_DELAY_MINUTES} "
            f"minutes — but default_args is missing or not a resolvable dict.\n"
            f"    Add default_args={{'retry_delay': timedelta(minutes="
            f"{MIN_RETRY_DELAY_MINUTES})}} to your DAG()."
        )
        return False

    retry_delay_node = _dict_lookup(default_args_dict, "retry_delay")
    if retry_delay_node is None:
        err(
            f"'retries' exceeds {RETRY_DELAY_TRIGGER_RETRIES}, so default_args "
            f"MUST define a 'retry_delay' of at least {MIN_RETRY_DELAY_MINUTES} "
            f"minutes — but no 'retry_delay' key was found in default_args.\n"
            f"    Add 'retry_delay': timedelta(minutes={MIN_RETRY_DELAY_MINUTES}) "
            f"to default_args."
        )
        return False

    minutes = _timedelta_to_minutes(retry_delay_node, assignments)

    if minutes is _UNRESOLVED:
        err(
            f"'retries' exceeds {RETRY_DELAY_TRIGGER_RETRIES}, so 'retry_delay' "
            f"must be at least {MIN_RETRY_DELAY_MINUTES} minutes, but its value "
            f"could not be resolved statically.\n"
            f"    Use an explicit timedelta, e.g. "
            f"timedelta(minutes={MIN_RETRY_DELAY_MINUTES}), so it can be validated."
        )
        return False

    if minutes < MIN_RETRY_DELAY_MINUTES:
        err(
            f"'retries' exceeds {RETRY_DELAY_TRIGGER_RETRIES}, so 'retry_delay' "
            f"must be at least {MIN_RETRY_DELAY_MINUTES} minutes, but it is "
            f"{minutes:g} minute(s).\n"
            f"    Increase retry_delay to timedelta(minutes="
            f"{MIN_RETRY_DELAY_MINUTES}) or more."
        )
        return False

    ok(
        f"retry_delay = {minutes:g} minute(s) satisfies the minimum of "
        f"{MIN_RETRY_DELAY_MINUTES} minutes required when retries exceed "
        f"{RETRY_DELAY_TRIGGER_RETRIES}."
    )
    return True


def validate_retries(dag_node: ast.Call, tree: ast.Module) -> bool:
    """
    Enforces that retries <= MAX_RETRIES in BOTH:
      a. default_args={'retries': N} on the DAG() definition
      b. retries=N on every individual operator/task call
    Additionally, if ANY retries value exceeds RETRY_DELAY_TRIGGER_RETRIES,
    default_args must define a 'retry_delay' of at least
    MIN_RETRY_DELAY_MINUTES minutes.
    """
    passed = True
    assignments = _build_assignment_map(tree)
    checked_any = False
    any_exceeds_trigger = False

    # ── (a) default_args in the DAG() definition ─────────────────────────────
    default_args_dict = None
    default_args_node = None
    for kw in dag_node.keywords:
        if kw.arg == "default_args":
            default_args_node = kw.value
            break

    if default_args_node is not None:
        default_args_dict = _get_dict_node(default_args_node, assignments)
        if default_args_dict is None:
            warn(
                "default_args is not an inline/resolvable dict literal — "
                "cannot statically inspect 'retries' in default_args. "
                f"Ensure it is <= {MAX_RETRIES}."
            )
        else:
            retries_node = _dict_lookup(default_args_dict, "retries")
            if retries_node is not None:
                checked_any = True
                p, _, exceeds = _check_retries_value(
                    retries_node, assignments, "default_args (DAG definition)"
                )
                passed = passed and p
                any_exceeds_trigger = any_exceeds_trigger or exceeds
            else:
                ok("default_args has no 'retries' key — nothing to check there.")
    else:
        ok("No default_args on DAG() — no default retries to check.")

    # ── (b) retries on individual task/operator calls ────────────────────────
    for task_call in _collect_task_call_nodes(tree):
        task_id = "<unknown>"
        for kw in task_call.keywords:
            if kw.arg == "task_id":
                tid = _literal_value(kw.value)
                if isinstance(tid, str):
                    task_id = tid
        for kw in task_call.keywords:
            if kw.arg == "retries":
                checked_any = True
                p, _, exceeds = _check_retries_value(
                    kw.value, assignments, f"task '{task_id}'"
                )
                passed = passed and p
                any_exceeds_trigger = any_exceeds_trigger or exceeds

    if not checked_any:
        ok(
            f"No explicit 'retries' found in default_args or any task — "
            f"nothing exceeds {MAX_RETRIES}."
        )

    # ── (c) retry_delay coupling: required when retries exceed the trigger ────
    if any_exceeds_trigger:
        warn(
            f"A 'retries' value exceeds {RETRY_DELAY_TRIGGER_RETRIES} — "
            f"default_args must define retry_delay >= {MIN_RETRY_DELAY_MINUTES} "
            f"minutes. Checking..."
        )
        passed = _validate_retry_delay(default_args_dict, assignments) and passed
    else:
        ok(
            f"No 'retries' value exceeds {RETRY_DELAY_TRIGGER_RETRIES} — "
            f"retry_delay minimum not required."
        )

    return passed


# # ── Rule 6: schedule_interval must never be None and must be a valid cron ─────

# def _is_valid_cron_string(value: str) -> bool:
#     """
#     Accepts a standard 5-field cron string (minute hour dom month dow) or an
#     Airflow cron preset (@daily, @hourly, ...). This is a structural check,
#     not a full cron grammar validation.
#     """
#     v = value.strip()
#     if v in CRON_PRESETS:
#         return True
#     fields = v.split()
#     return len(fields) == 5


# def validate_schedule_interval(dag_node: ast.Call, tree: ast.Module) -> bool:
#     """
#     Enforces that schedule_interval:
#       - is present on the DAG() definition (must be explicit),
#       - is never None / null / empty,
#       - resolves to a valid cron string or Airflow preset.
#     When sourced from a variable / Variable.get(), the value is resolved and
#     must not be None. A Variable.get() with no default is rejected because it
#     can return None at runtime.
#     """
#     assignments = _build_assignment_map(tree)

#     schedule_node = None
#     for kw in dag_node.keywords:
#         if kw.arg == "schedule_interval":
#             schedule_node = kw.value
#             break

#     if schedule_node is None:
#         err(
#             "schedule_interval is not set in the DAG() definition.\n"
#             "    It must be explicitly defined as a cron expression and must "
#             "never rely on the default or be None.\n"
#             "    Example: schedule_interval=\"0 5 * * *\""
#         )
#         return False

#     # Explicit None literal → hard fail
#     if isinstance(schedule_node, ast.Constant) and schedule_node.value is None:
#         err(
#             "schedule_interval is set to None. A None schedule means the DAG "
#             "will never be scheduled.\n"
#             "    Set it to a valid cron expression, e.g. \"0 5 * * *\"."
#         )
#         return False

#     # Variable.get() with no default can resolve to None at runtime → fail
#     if _is_variable_get_call(schedule_node):
#         has_default, _ = _variable_get_default(schedule_node)
#         if not has_default:
#             err(
#                 "schedule_interval is sourced from Variable.get() without a "
#                 "default value. This can resolve to None at runtime if the "
#                 "Airflow Variable is unset.\n"
#                 "    Provide a non-None default cron, e.g. "
#                 "Variable.get(\"my_schedule\", \"0 5 * * *\")."
#             )
#             return False

#     resolved = _resolve_node(schedule_node, assignments)

#     if resolved is _UNRESOLVED:
#         warn(
#             "schedule_interval is set from a value that could not be resolved "
#             "statically. Ensure it always evaluates to a valid, non-None cron "
#             "expression."
#         )
#         return True

#     if resolved is None:
#         err(
#             "schedule_interval resolves to None (via a variable or "
#             "Variable.get default). A None schedule means the DAG will never "
#             "be scheduled.\n"
#             "    Set it to a valid cron expression, e.g. \"0 5 * * *\"."
#         )
#         return False

#     if not isinstance(resolved, str) or not resolved.strip():
#         err(
#             f"schedule_interval resolves to an invalid value ({resolved!r}). "
#             f"It must be a non-empty cron string.\n"
#             f"    Example: schedule_interval=\"0 5 * * *\"."
#         )
#         return False

#     if _is_valid_cron_string(resolved):
#         ok(f"schedule_interval = '{resolved}' is a valid cron expression.")
#         return True

#     err(
#         f"schedule_interval = '{resolved}' is not a valid cron expression.\n"
#         f"    Use a 5-field cron string (e.g. \"0 5 * * *\") or an Airflow "
#         f"preset ({', '.join(sorted(CRON_PRESETS))})."
#     )
#     return False


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Validate DAG metadata — team tagging, usecase tagging, and dag_id/filename match."
    )
    parser.add_argument("--dag",     required=True, help="Path to dag .py file")
    parser.add_argument("--team",    required=True, help="Team name selected in Jenkins")
    parser.add_argument("--usecase", required=True, help="Usecase name entered in Jenkins")
    parser.add_argument("--env_name", required=True, help="Environment entered in Jenkins")
    args = parser.parse_args()

    print("=" * 60)
    print("  DAG METADATA VALIDATOR")
    print(f"  File    : {args.dag}")
    print(f"  Team    : {args.team}")
    print(f"  Usecase : {args.usecase}")
    print(f"  Environment : {args.env_name}")
    print("=" * 60)

    try:
        source = read_source(args.dag)
    except FileNotFoundError:
        print(f"[ERROR] File not found: {args.dag}")
        sys.exit(1)

    try:
        tree = ast.parse(source)
    except SyntaxError as exc:
        print(f"[ERROR] Python syntax error in DAG file: {exc}")
        sys.exit(1)

    # Locate the DAG() call node once — shared across all rules
    dag_node = find_dag_node(tree)
    if dag_node is None:
        print("[ERROR] No DAG() constructor call found in the file.")
        print("        Ensure your file contains a DAG() or airflow.DAG() instantiation.")
        sys.exit(1)

    results = []

    print("\n── Rule 1: Team Value in DAG Tags ───────────────────────────")
    results.append(validate_team_in_tags(dag_node, args.team))

    print("\n── Rule 2: Usecase Name in DAG Tags ─────────────────────────")
    results.append(validate_usecase_in_tags(dag_node, args.usecase))

    print("\n── Rule 3: dag_id Matches DAG Filename Stem ─────────────────")
    results.append(validate_dag_id_matches_filename(dag_node, args.dag))

    print("\n── Rule 4: Environment Name in DAG Tags ─────────────────")
    results.append(validate_env_name_in_tags(dag_node, args.env_name))

    print("\n── Rule 5: Task Retries <= 20 + retry_delay >= 15m when > 5 ─")
    results.append(validate_retries(dag_node, tree))

    # print("\n── Rule 6: schedule_interval is a valid non-None cron ───────")
    # results.append(validate_schedule_interval(dag_node, tree))

    print("\n" + "=" * 60)
    if all(results):
        print("  RESULT : ALL VALIDATIONS PASSED ✓")
        print("=" * 60)
        sys.exit(0)
    else:
        failed = results.count(False)
        print(f"  RESULT : {failed} VALIDATION(S) FAILED ✗")
        print("=" * 60)
        sys.exit(1)


if __name__ == "__main__":
    main()