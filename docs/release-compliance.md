# Release compliance

The repository gate is credential-free and does not contact a deployment. `just check`
verifies formatting, linting, types, tests and coverage, promoted catalog and schema
contracts, immutable automation, package construction and installation, MIT metadata,
complete Git and worktree secret scans, and version consistency. `just audit` adds the
current network-backed Python vulnerability audit.

`just image` builds the repository-named `musicbrainz-sql-loader:local` image, checks the
installed service import, and proves that the image runs as numeric user and group
`1000:1000`. `just release-dry-run` creates the wheel and source distribution, SHA-256
checksums, a CycloneDX SBOM, third-party notices, and provenance bound to the exact source
revision. Neither command uploads a package, creates a tag, or changes repository settings.

The first-party package is MIT licensed. The release gate verifies tracked license and
package metadata and inventories runtime and development dependency licenses with
`pip-licenses`. Dependency vulnerabilities are evaluated from the Python 3.14 locked graph
with `pip-audit` before a release is approved.

The CI caller pins the organization Automation workflows and `python-libraries` dependency
to immutable commits. Every pull request author, including Dependabot, runs the same required
job graph. Releases remain tag-only, and no Renovate workflow is active.

Publication remains a separate infrastructure decision. Complete reachable-history and
secret scans, a clean reviewed commit, successful hosted CI, and explicit operator approval
are required before any visibility change. Tags, packages, and container publication require
their own approval.
