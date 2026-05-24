# Cat-Images Localnet Scenario Manual (All 4 Services)

This guide starts the full stack together with one Compose file:

1. `pylon`
2. `validator` + `nexus_auth_validator`
3. `miner` + `nginx` (TLS termination) + `nexus_auth_miner`
4. `facilitator` (user-facing UI/API on `http://127.0.0.1:8080`)

Validator→miner traffic is protected by mutual TLS (mTLS): the validator presents its X.509 client certificate
to the miner's nginx, which verifies it via `nexus_auth`. On the validator side, the miner's server certificate
is pinned: before each connection the validator fetches the miner's expected public key from Pylon and verifies
the miner's presented TLS certificate matches.

It assumes a Bittensor localnet/subtensor node is already running on the host and exposing HTTP RPC on
`http://127.0.0.1:9944`. Compose maps `host.docker.internal` to the host gateway so pylon can reach that localnet from
inside its container.

## 1) Prepare `.env` for compose

```bash
cd demos/cat-images
cp .env.compose.example .env
```

Then edit `.env` and fill real values. Facilitator-specific examples are in `.env.facilitator.example`, but compose still reads from `.env`.

Required variables:

- Pylon (open access): `PYLON_BITTENSOR_NETWORK`, `PYLON_OPEN_ACCESS_TOKEN`, `PYLON_RECENT_OBJECTS_NETUIDS`
- Pylon (validator identity for weight writes): `PYLON_IDENTITIES`, `PYLON_ID_VALIDATOR_WALLET_NAME`, `PYLON_ID_VALIDATOR_HOTKEY_NAME`, `PYLON_ID_VALIDATOR_NETUID`, `PYLON_ID_VALIDATOR_TOKEN`
- Pylon (miner identity for mTLS cert management): `PYLON_ID_MINER_WALLET_NAME`, `PYLON_ID_MINER_HOTKEY_NAME`, `PYLON_ID_MINER_NETUID`, `PYLON_ID_MINER_TOKEN`
- Validator: `VALIDATOR_NETUID`, `VALIDATOR_OPENROUTER_API_KEY`, `VALIDATOR_PYLON_OPEN_ACCESS_TOKEN`, `VALIDATOR_PYLON_IDENTITY_NAME`, `VALIDATOR_PYLON_IDENTITY_TOKEN`, `VALIDATOR_S3_BUCKET`
- mTLS cert paths (set in step 3): `VALIDATOR_MTLS_CERT_PATH`, `VALIDATOR_MTLS_KEY_PATH`
- Miner: `MINER_OPENROUTER_API_KEY`, `MINER_WALLET_NAME`, `MINER_HOTKEY_NAME`, `MINER_NETUID`, `MINER_UPDATE_AXON`
- AWS/S3: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_DEFAULT_REGION`
- Facilitator: `FACI_S3_BUCKET`, `FACI_S3_ACCESS_KEY`, `FACI_S3_SECRET_KEY` (optional `FACI_S3_REGION`, `FACI_S3_ENDPOINT_URL`)

Localnet defaults:

- `PYLON_BITTENSOR_NETWORK=http://host.docker.internal:9944`
- `PYLON_RECENT_OBJECTS_NETUIDS=[2]`
- `PYLON_ID_VALIDATOR_NETUID=2`
- `PYLON_ID_MINER_NETUID=2`
- `VALIDATOR_NETUID=2`
- `MINER_NETUID=2`
- `MINER_SUBTENSOR_NETWORK=http://host.docker.internal:9944`
- `MINER_UPDATE_AXON=false`
- This checkout's localnet subnet `2` wallet matches are `cat-images-validator-kuba-2/default` for the validator and
  `cat-images-miner-kuba-2/default` for the miner.

Notes:

- For testnet subnet `278` with `cat-images-validator-kuba` + `default` hotkey, override the localnet defaults:
  - `PYLON_BITTENSOR_NETWORK=test`
  - `PYLON_RECENT_OBJECTS_NETUIDS=[278]`
  - `PYLON_IDENTITIES=["validator","miner"]`
  - `PYLON_ID_VALIDATOR_WALLET_NAME=cat-images-validator-kuba`
  - `PYLON_ID_VALIDATOR_HOTKEY_NAME=default`
  - `PYLON_ID_VALIDATOR_NETUID=278`
  - `PYLON_ID_MINER_NETUID=278`
  - `VALIDATOR_NETUID=278`
  - `VALIDATOR_PYLON_IDENTITY_NAME=validator`
  - `VALIDATOR_PYLON_IDENTITY_TOKEN` must match `PYLON_ID_VALIDATOR_TOKEN`
- Keep `MINER_UPDATE_AXON=false` for local smoke tests unless the miner wallet should write axon metadata to the chain.
- `FACI_VALIDATORS` is already wired in `compose.yaml` to the validator container.

## 2) Start pylon and build images

mTLS certificate generation requires pylon to be running. Start it first:

```bash
docker compose -f compose.yaml --env-file .env up --build -d pylon
docker compose -f compose.yaml --env-file .env ps
```

## 3) Generate mTLS certificates

Generate a certificate for the validator (published to Pylon under the validator identity):

```bash
docker compose -f compose.yaml --env-file .env run --rm nexus_auth_validator generate \
    -ss58-address <VALIDATOR_HOTKEY_SS58>
```

Generate a certificate for the miner (published to Pylon under the miner identity):

```bash
docker compose -f compose.yaml --env-file .env run --rm nexus_auth_miner generate \
    -ss58-address <MINER_HOTKEY_SS58>
```

Certificates are stored in `docker/certs/validator/` and `docker/certs/miner/` (gitignored).
The cert paths are hardcoded in `compose.yaml` and map to the volume-mounted `/certs/` directory — no `.env` changes needed.

## 4) Start all services

```bash
docker compose -f compose.yaml --env-file .env up --build -d
docker compose -f compose.yaml --env-file .env ps
```

Expected: all services are `Up`. The facilitator is exposed on `0.0.0.0:8080`. The miner's nginx listens
internally on port `8443` within the `catnet` network (not exposed to the host).

## 5) Manual smoke test through facilitator

Submit an image (example uses `mouse.jpg` from this directory):

```bash
curl -sS -X POST -F "image=@mouse.jpg;type=image/jpeg" http://127.0.0.1:8080/catify
```

You can also use the Web UI for that by navigating to `http://127.0.0.1:8080/`.

Expected response is an HTML fragment like:

```html
<div data-job-id="..."></div>
```

Extract job id:

```bash
JOB_ID="$(curl -sS -X POST -F "image=@mouse.jpg;type=image/jpeg" http://127.0.0.1:8080/catify \
  | grep -oE 'data-job-id="[^"]+"' | sed -E 's/data-job-id="([^"]+)"/\1/')"
echo "$JOB_ID"
```

Check job status page:

```bash
curl -sS "http://127.0.0.1:8080/jobs/${JOB_ID}"
```

When done, fetch generated image via facilitator proxy:

```bash
curl -fSs -o /tmp/cat-result-"${JOB_ID}".png "http://127.0.0.1:8080/images/${JOB_ID}/result"
```

## 6) Observe logs

```bash
docker compose -f compose.yaml --env-file .env logs -f facilitator validator miner pylon nginx
```

## 7) Stop services

```bash
docker compose -f compose.yaml --env-file .env down
```
