# Component Catalog

## 1) `MetagraphSource[Trigger]`

Module: `nexus/actors/metagraph_source.py`

Purpose:

- consume a typed trigger and emit a metagraph snapshot for the configured subnet

Required knobs:

- `_id`

Optional knobs:

- `netuid` (defaults from `VALIDATOR_NETUID`)
- `pylon_client_provider`

Endpoints:

- sink: `trigger`
- sources: `metagraph`, `error`
