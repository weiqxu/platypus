# SQL Reader / GPORCA Bridge

This executable (`server`) demonstrates a simple SQL pipeline:

1. Parse SQL with DuckDB parser
2. Build a GPORCA logical expression (manually, for supported shapes)
3. Optimize with GPORCA (`CQueryContext` + `CEngine`)
4. Convert GPORCA physical operator tree into a DuckDB-style physical plan view

## Supported SQL Shape (Current)

The current implementation supports the demo shape:

- `SELECT <constant> AS answer, <constant-string> AS note`

Internally this is modeled as a projection over a constant table-get.

## Supported Operators (Current)

### Logical conversion to GPORCA

- DuckDB logical intent `PROJECTION` -> GPORCA `CLogicalProject`
- DuckDB logical intent `DUMMY_SCAN` -> GPORCA `CLogicalConstTableGet`
- Project list node -> GPORCA `CScalarProjectList`
- Project element node -> GPORCA `CScalarProjectElement`
- Project expression ref -> GPORCA `CScalarIdent`

### GPORCA physical to DuckDB-style physical plan mapping

- GPORCA `CPhysicalComputeScalar` -> DuckDB-style `PROJECTION`
- GPORCA `CPhysicalConstTableGet` -> DuckDB-style `DUMMY_SCAN`

Any unrecognized GPORCA physical operator is currently printed using its original GPORCA operator name.

## Metadata Bootstrap (Non-minidump)

The optimizer uses a metadata provider file loaded through `CMDProviderMemory`:

- Default path: `third_party/gporca/data/dxl/metadata/md.xml`
- Override via env var: `GPORCA_MD_FILE`

## Notes

- This is a targeted bridge demo, not a general SQL-to-GPORCA translator yet.
- Support for joins, filters, scans on real tables, aggregates, and broader physical operator mappings is not implemented yet.
