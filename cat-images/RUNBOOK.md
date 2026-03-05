# Cat-Images Local Scenario Manual (All 4 Services)

This guide starts the full stack together with one Compose file:

1. `pylon`
2. `validator`
3. `miner`
4. `facilitator` (user-facing UI/API on `http://127.0.0.1:8080`)

## 1) Prepare `.env` for compose

```bash
cd /home/kuba/repos/nexus-poc/cat-images
cp .env.compose.example .env
```

Then edit `.env` and fill real values. Facilitator-specific examples are in `.env.facilitator.example`, but compose still reads from `.env`.

Required variables:

- Shared: `WALLET_NAME`
- Pylon: `PYLON_BITTENSOR_NETWORK`, `PYLON_HOTKEY_NAME`, `PYLON_OPEN_ACCESS_TOKEN`, `PYLON_RECENT_OBJECTS_NETUIDS`
- Validator: `VALIDATOR_NETUID`, `VALIDATOR_OPENROUTER_API_KEY`, `VALIDATOR_PYLON_OPEN_ACCESS_TOKEN`, `VALIDATOR_S3_BUCKET`
- Miner: `MINER_OPENROUTER_API_KEY`, `MINER_HOTKEY_NAME`, `MINER_UPDATE_AXON`
- AWS/S3: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_DEFAULT_REGION`
- Facilitator: `FACI_S3_BUCKET`, `FACI_S3_ACCESS_KEY`, `FACI_S3_SECRET_KEY` (optional `FACI_S3_REGION`, `FACI_S3_ENDPOINT_URL`)

Notes:

- For local smoke tests, `MINER_UPDATE_AXON=false` is usually the easiest option.
- `FACI_VALIDATORS` is already wired in `compose.yaml` to the validator container.

## 2) Start all services

```bash
docker compose -f compose.yaml --env-file .env up --build -d
docker compose -f compose.yaml --env-file .env ps
```

Expected: all four services are `Up` and facilitator is exposed on `0.0.0.0:8080`.

## 3) Manual smoke test through facilitator

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

## 4) Observe logs

```bash
docker compose -f compose.yaml --env-file .env logs -f facilitator validator miner pylon
```

## 5) Stop services

```bash
docker compose -f compose.yaml --env-file .env down
```
