# PostgreSQL adapter CI on Beep

The quality job and six separate PostgreSQL 13–18 live jobs run in disposable
`beep-vm` guests. Each live job owns a digest-pinned official PostgreSQL service.
GitHub starts and removes that service inside its guest; the guest and its disk
overlay are removed after the job. Matrix `fail-fast: false` preserves every
server result. The existing one-VM Beep limit serializes jobs.

All existing gates remain mandatory: locked dependency restoration; precommit
(force compile with warnings as errors, format, `mix credo --strict` and the
separate `mix credo -C atom_audit --all-priorities --strict` alias,
ExUnit, four bounded protocol proofs and zero compile-connected cycles);
strict documentation; Dialyzer; and metadata-only Hex package assembly. Every
live server runs `mix test --include postgres` and
`mix selecto_db_postgresql.verify_sql`.

At baseline `87251b8`, quality has 104 passing tests and 69 PostgreSQL exclusions;
each server executes all 173 tests without failures, skips or exclusions,
and proves 232 relational differentials. CI records actual test counts and
enforces these executed-coverage floors so new tests can grow. The four bounded
protocol profiles and 232-case differential contract remain exact.

Beep installs exact Elixir 1.20.0 / OTP 29.0.2. Local helper and quality checks
use the installed Elixir 1.19.5 / OTP 28.4.1; they do not replace the forthcoming
seven-job proof on the selected Beep runtime.

## Immutable source and credentials

`ci/core_ref.exs` parses the Core declaration and lock without evaluating their
code. They must agree on one full immutable Git SHA and the Core repository.
At this baseline it is `e24b60d50c1ad1741691d3f79f47a1b6dc1346ef`; the former
hosted workflow instead checked out `a596b342`.

The installed `selecto-ci-checkout` helper clones the default branch. Its
optional ref is a branch/tag, so CI explicitly fetches and checks out the
validated full SHA afterward. A file-only credential helper permits HTTPS
GitHub `get` requests for exactly `seeken/selecto`, only when it is an enrolled
sibling. The token never enters a URL, environment variable, report, or stored
Git remote. Checkout credentials are not persisted; operator credentials are
not used.

Only dependency sources and PLTs are cached. Compiled `_build` files are never
restored; cache keys include the validated Core SHA and exact runtime.
Diagnostics verify the loaded adapter and Core module source directories,
actual Git HEADs and clean status. Unknown Git status fails provenance.

## Bounded evidence

Jobs upload `ci-reports/*.json` as `postgresql-quality-ATTEMPT` or
`postgresql-MAJOR-ATTEMPT`. Stage reports contain actual test/proof counts,
clean Dialyzer results, or a freshly assembled package filename/hash.
`diagnostics.json` records actual Elixir, OTP and Postgrex versions, two source
identities, and the actual server version for live jobs; its major must match
the selected matrix entry. The package report proves metadata assembly,
not an installed Hex consumer. Assembly clears only this project's prior
generated tar files and refuses a missing or ambiguous fresh output.

Raw tool output remains guest-local and is deleted. Failures retain the
original nonzero exit code with a fixed stage/classification; no raw exception,
token or connection URL is uploaded. A stage rerun replaces its owned report,
and job initialization clears prior owned JSON reports. Helper checks cover pin
drift, older-commit fetches, unknown/dirty Git status, credential scope, incomplete
coverage and failure propagation/redaction.

Beep enrollment requires exact owned public-fork identity
`seeken/selecto_db_postgresql` / `1185660776`, actor `seeken`, same-repository
heads and `beep-vm`; external fork heads are refused. Its only sibling is Core.
Root activates the policy after the reviewed VM hook has been refreshed.
