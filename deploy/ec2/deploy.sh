#!/usr/bin/env bash
# Build the app image for the VM, ship it over SSH, and (re)start the stack.
#
#   deploy/ec2/deploy.sh ubuntu@<ip> [site-address]
#
# The image is built here, not on the 1 GiB VM (where compiling Go can run out of memory):
# cross-compiled for linux/amd64 and streamed with `docker save | ssh docker load`, so no
# registry is involved. Secrets are generated on the VM on first deploy and never leave it,
# except ADMIN_KEY, which is copied to .env.live (gitignored) so the burst can create shows.
set -euo pipefail
cd "$(dirname "$0")/../.."

HOST=${1:?usage: deploy/ec2/deploy.sh ubuntu@<ip> [site-address]}
IP=${HOST#*@}
SITE=${2:-$(echo "$IP" | tr . -).sslip.io}
KEY=${SSH_KEY:-$HOME/.ssh/seat-reservation-ec2}
PLATFORM=${PLATFORM:-linux/amd64}
VERSION=$(git describe --always --dirty)
ssh_() { ssh -i "$KEY" -o StrictHostKeyChecking=accept-new "$HOST" "$@"; }

echo "==> building seat-reservation:$VERSION for $PLATFORM"
docker buildx build --platform "$PLATFORM" --build-arg VERSION="$VERSION" \
  -t "seat-reservation:$VERSION" --load .

echo "==> shipping image to $HOST"
docker save "seat-reservation:$VERSION" | gzip | ssh_ 'gunzip | docker load'

echo "==> uploading stack definition"
ssh_ 'mkdir -p ~/seat-reservation'
scp -q -i "$KEY" deploy/ec2/docker-compose.yml deploy/ec2/Caddyfile "$HOST:seat-reservation/"

echo "==> starting $VERSION (https://$SITE)"
ssh_ bash -s -- "$VERSION" "$SITE" <<'REMOTE'
set -euo pipefail
cd ~/seat-reservation
if [ ! -f .env ]; then # first deploy: secrets are generated on the VM
  umask 077
  printf 'POSTGRES_PASSWORD=%s\nJWT_SECRET=%s\nADMIN_KEY=%s\n' \
    "$(openssl rand -hex 24)" "$(openssl rand -hex 32)" "$(openssl rand -hex 16)" > .env
fi
sed -i '/^APP_VERSION=/d;/^SITE_ADDRESS=/d' .env
printf 'APP_VERSION=%s\nSITE_ADDRESS=%s\n' "$1" "$2" >> .env
docker compose up -d --remove-orphans
# the Caddyfile is a bind mount: reload it gracefully (no dropped connections) in case it changed
docker compose exec -T caddy caddy reload --config /etc/caddy/Caddyfile --adapter caddyfile 2>/dev/null || true
# keep only the running app version
docker images seat-reservation --format '{{.Tag}}' | grep -vxF "$1" |
  xargs -r -I{} docker rmi -f "seat-reservation:{}" >/dev/null 2>&1 || true
REMOTE

echo "==> waiting for readiness"
ready=""
for _ in $(seq 1 60); do
  if out=$(curl -fsS --max-time 3 "http://$IP/readyz" 2>/dev/null); then
    ready=1
    break
  fi
  sleep 2
done
if [ -z "$ready" ]; then
  echo "==> NOT READY after 2 minutes. Last answer:" >&2
  curl -sS --max-time 5 "http://$IP/readyz" >&2 || true
  echo "    look at: ssh -i $KEY $HOST 'cd ~/seat-reservation && docker compose logs --tail=50 app'" >&2
  exit 1
fi
echo "$out"

ssh_ 'grep "^ADMIN_KEY=" ~/seat-reservation/.env' > .env.live
printf 'BASE_URL=https://%s\n' "$SITE" >> .env.live
chmod 600 .env.live
echo "==> deployed $VERSION. ADMIN_KEY and BASE_URL are in .env.live (gitignored)."
echo "    burst:  set -a; . ./.env.live; set +a; ./burst.sh \"\$BASE_URL\""
