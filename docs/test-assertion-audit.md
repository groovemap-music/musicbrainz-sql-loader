# Test assertion audit

The repository-local `scripts/audit_test_assertions.py` inventory uses the Python AST rather
than text matching. It classifies pytest-style test functions as assertion-free or reliant
only on standard `Mock`/`AsyncMock` call assertions. Python `assert`, unittest-style
`assert*` methods, and `pytest.raises`/`warns`/`deprecated_call` count as outcome assertions.

Run it from the repository root:

```console
uv run python scripts/audit_test_assertions.py --json
```

## Fixture-hardening audit

| Revision | Test functions | Assertion-free | Call-only |
| --- | ---: | ---: | ---: |
| Before (`0c30197`) | 194 | 5 | 28 |
| After (`gm-musicbrainz-sql-loader-55p.1`) | 195 | 5 | 28 |

The added fixture-contract test accounts for the test-function increase. Spec-hardening the
shared PostgreSQL pool boundary exposed no latent product failures. The existing active
release-groups remediation remains owned by `gm-musicbrainz-sql-loader-dg-cev8`; this change
does not alter or duplicate that work.
