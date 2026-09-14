#!/usr/bin/env python3
"""Inventory tests that have no outcome assertion or only mock-call assertions."""

from __future__ import annotations

import argparse
import ast
import json
from dataclasses import asdict, dataclass
from pathlib import Path


MOCK_CALL_ASSERTIONS = frozenset(
    {
        "assert_any_await",
        "assert_any_call",
        "assert_awaited",
        "assert_awaited_once",
        "assert_awaited_once_with",
        "assert_awaited_with",
        "assert_called",
        "assert_called_once",
        "assert_called_once_with",
        "assert_called_with",
        "assert_has_awaits",
        "assert_has_calls",
        "assert_not_awaited",
        "assert_not_called",
    }
)
PYTEST_OUTCOME_HELPERS = frozenset({"deprecated_call", "raises", "warns"})


@dataclass(frozen=True)
class AuditResult:
    """Stable, serializable assertion audit output."""

    test_functions: int
    assertion_free_count: int
    call_only_count: int
    assertion_free: list[str]
    call_only: list[str]


class _AssertionVisitor(ast.NodeVisitor):
    """Count outcome and mock-call assertions without entering nested scopes."""

    def __init__(self) -> None:
        self.outcome_assertions = 0
        self.mock_call_assertions = 0

    def visit_Assert(self, node: ast.Assert) -> None:
        self.outcome_assertions += 1
        self.generic_visit(node)

    def visit_Call(self, node: ast.Call) -> None:
        if isinstance(node.func, ast.Attribute):
            method = node.func.attr
            if method in MOCK_CALL_ASSERTIONS:
                self.mock_call_assertions += 1
            elif method.startswith("assert") or (
                method in PYTEST_OUTCOME_HELPERS and isinstance(node.func.value, ast.Name) and node.func.value.id == "pytest"
            ):
                self.outcome_assertions += 1
        self.generic_visit(node)

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        """Do not attribute assertions in a nested helper to its parent test."""

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        """Do not attribute assertions in a nested helper to its parent test."""

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        """Do not enter a class defined inside a test."""

    def visit_Lambda(self, node: ast.Lambda) -> None:
        """Do not attribute assertions in a nested lambda to its parent test."""


def _is_pytest_fixture(node: ast.FunctionDef | ast.AsyncFunctionDef) -> bool:
    for decorator in node.decorator_list:
        target = decorator.func if isinstance(decorator, ast.Call) else decorator
        if isinstance(target, ast.Attribute) and isinstance(target.value, ast.Name):
            if target.value.id == "pytest" and target.attr == "fixture":
                return True
        elif isinstance(target, ast.Name) and target.id == "fixture":
            return True
    return False


def audit(root: Path) -> AuditResult:
    """Audit test functions below *root* in deterministic path/line order."""
    tests: list[tuple[str, ast.FunctionDef | ast.AsyncFunctionDef]] = []
    base = root.parent
    for path in sorted(root.rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        relative_path = path.relative_to(base).as_posix()
        for node in ast.walk(tree):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name.startswith("test_") and not _is_pytest_fixture(node):
                tests.append((f"{relative_path}:{node.lineno}:{node.name}", node))

    assertion_free: list[str] = []
    call_only: list[str] = []
    for identifier, node in sorted(tests):
        visitor = _AssertionVisitor()
        for statement in node.body:
            visitor.visit(statement)
        if visitor.outcome_assertions == 0:
            if visitor.mock_call_assertions == 0:
                assertion_free.append(identifier)
            else:
                call_only.append(identifier)

    return AuditResult(
        test_functions=len(tests),
        assertion_free_count=len(assertion_free),
        call_only_count=len(call_only),
        assertion_free=assertion_free,
        call_only=call_only,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path("tests"), help="test tree to scan (default: tests)")
    parser.add_argument("--json", action="store_true", help="emit machine-readable JSON")
    args = parser.parse_args()

    result = audit(args.root)
    if args.json:
        print(json.dumps(asdict(result), indent=2))
        return

    print(f"test functions: {result.test_functions}")
    print(f"assertion-free: {result.assertion_free_count}")
    print(f"call-only: {result.call_only_count}")
    for heading, identifiers in (("assertion-free tests", result.assertion_free), ("call-only tests", result.call_only)):
        print(f"\n{heading}:")
        for identifier in identifiers:
            print(f"  {identifier}")


if __name__ == "__main__":
    main()
