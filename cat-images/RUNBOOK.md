# Cat-Images Local Scenario Manual

This guide runs the full local flow:

1. validator + pylon (compose)
2. miner (compose, same Docker network)
3. request to `POST /cat-images`
4. validator returns `image_hash`

All secrets are passed as command-prefixed environment variables, not stored in env files.

Important: Docker Compose interpolates required variables on every command (`up`, `ps`, `logs`, `down`), so keep the same secret prefixes for all `run_validator.sh` and `run_miner.sh` commands.

## 1) Prepare non-secret env files

Create runtime files from examples:

```bash
cd /home/kuba/repos/nexus-poc/cat-images

cp docker/.env.validator-compose.example docker/.env.validator
cp docker/.env.miner-compose.example docker/.env.miner
```

Append non-secret overrides for this scenario:

```bash
cat <<'EOF' >> docker/.env.validator
VALIDATOR_NETUID=278
VALIDATOR_S3_BUCKET=kuba-cat-images-bucket
PYLON_BITTENSOR_NETWORK=test
PYLON_RECENT_OBJECTS_NETUIDS=[278]
EOF

cat <<'EOF' >> docker/.env.miner
MINER_UPDATE_AXON=true
MINER_SUBTENSOR_NETWORK=test
MINER_NETUID=278
MINER_WALLET_NAME=cat-images-miner-kuba
MINER_HOTKEY_NAME=default
MINER_EXTERNAL_IP=172.30.0.30
MINER_EXTERNAL_PORT=9090
EOF
```

## 2) Build images

```bash
./build_validator.sh cat-validator:compose
./build_miner.sh cat-miner:latest
```

## 3) Start validator + pylon

Pass validator/OpenRouter and AWS credentials inline:

```bash
VALIDATOR_OPENROUTER_API_KEY='<YOUR_OPENROUTER_KEY>' \
AWS_ACCESS_KEY_ID='<YOUR_AWS_ACCESS_KEY_ID>' \
AWS_SECRET_ACCESS_KEY='<YOUR_AWS_SECRET_ACCESS_KEY>' \
AWS_DEFAULT_REGION='eu-north-1' \
./run_validator.sh up
```

If you use temporary AWS credentials, include:

```bash
AWS_SESSION_TOKEN='<YOUR_AWS_SESSION_TOKEN>'
```

Check status:

```bash
VALIDATOR_OPENROUTER_API_KEY='<YOUR_OPENROUTER_KEY>' \
AWS_ACCESS_KEY_ID='<YOUR_AWS_ACCESS_KEY_ID>' \
AWS_SECRET_ACCESS_KEY='<YOUR_AWS_SECRET_ACCESS_KEY>' \
AWS_DEFAULT_REGION='eu-north-1' \
./run_validator.sh ps
```

## 4) Start miner

Pass miner OpenRouter key inline:

```bash
MINER_OPENROUTER_API_KEY='<YOUR_OPENROUTER_KEY>' \
./run_miner.sh up
```

Check status:

```bash
MINER_OPENROUTER_API_KEY='<YOUR_OPENROUTER_KEY>' \
./run_miner.sh ps
```

## 5) Upload input image to S3

Example with `chopin-monument.jpg`:

```bash
AWS_ACCESS_KEY_ID='<YOUR_AWS_ACCESS_KEY_ID>' \
AWS_SECRET_ACCESS_KEY='<YOUR_AWS_SECRET_ACCESS_KEY>' \
AWS_DEFAULT_REGION='eu-north-1' \
aws s3api put-object \
  --bucket kuba-cat-images-bucket \
  --key chopin-monument.jpg \
  --body chopin-monument.jpg
```

## 6) Issue request to validator

Generate a presigned HTTP GET URL for the uploaded source image:

```bash
IMAGE_HTTP_URL="$(
  AWS_ACCESS_KEY_ID='<YOUR_AWS_ACCESS_KEY_ID>' \
  AWS_SECRET_ACCESS_KEY='<YOUR_AWS_SECRET_ACCESS_KEY>' \
  AWS_DEFAULT_REGION='eu-north-1' \
  aws s3 presign "s3://kuba-cat-images-bucket/chopin-monument.jpg" \
    --expires-in 1800
)"
```

Send the request:

```bash
curl -sS -X POST "http://127.0.0.1:8081/cat-images" \
  -H "Content-Type: application/json" \
  --data-binary "{\"image_s3_url\":\"${IMAGE_HTTP_URL}\",\"image_name\":\"manual-$(date +%s).jpg\"}"
```

Note: the request field is still named `image_s3_url` for backward compatibility, but it should carry an `http(s)` URL.

Expected response shape:

```json
{"image_hash":"<sha256-like-hash>"}
```

## 7) Observe logs

```bash
VALIDATOR_OPENROUTER_API_KEY='<YOUR_OPENROUTER_KEY>' \
AWS_ACCESS_KEY_ID='<YOUR_AWS_ACCESS_KEY_ID>' \
AWS_SECRET_ACCESS_KEY='<YOUR_AWS_SECRET_ACCESS_KEY>' \
AWS_DEFAULT_REGION='eu-north-1' \
./run_validator.sh logs validator

MINER_OPENROUTER_API_KEY='<YOUR_OPENROUTER_KEY>' \
./run_miner.sh logs miner
```

## 8) Stop services

```bash
MINER_OPENROUTER_API_KEY='<YOUR_OPENROUTER_KEY>' \
./run_miner.sh down

VALIDATOR_OPENROUTER_API_KEY='<YOUR_OPENROUTER_KEY>' \
AWS_ACCESS_KEY_ID='<YOUR_AWS_ACCESS_KEY_ID>' \
AWS_SECRET_ACCESS_KEY='<YOUR_AWS_SECRET_ACCESS_KEY>' \
AWS_DEFAULT_REGION='eu-north-1' \
./run_validator.sh down
```
