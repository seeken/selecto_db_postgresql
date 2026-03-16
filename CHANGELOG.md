CHANGES
=======

V NEXT
------

- Dropped the standalone `selecto_db_adapter` dependency now that
  `Selecto.DB.Adapter` ships with `selecto`.
- Updated installation guidance to depend directly on `selecto` plus
  `selecto_db_postgresql`.

V 0.4.0
--------

- Introduced the standalone PostgreSQL adapter package for the external
  Selecto adapter architecture.
- Added PostgreSQL-owned hooks for execution, pooling, streaming, diagnostics,
  server version detection, and repo fallback behavior.
- Bump package version to `0.4.0`.
