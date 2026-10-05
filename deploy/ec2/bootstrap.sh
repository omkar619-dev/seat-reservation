#!/usr/bin/env bash
# One-time host setup for a small Ubuntu 24.04 VM (tested on a t3.micro: 2 vCPU, 1 GiB RAM).
# Idempotent: safe to re-run. Run on the VM:  ssh ubuntu@<host> 'bash -s' < deploy/ec2/bootstrap.sh
set -euo pipefail

# The box previously ran a TURN relay; stop it so its ports and memory are free.
sudo systemctl disable --now coturn 2>/dev/null || true
# No IAM instance profile is attached, so the SSM agent can never register: reclaim its memory.
sudo snap stop --disable amazon-ssm-agent 2>/dev/null || true

if ! command -v docker >/dev/null 2>&1; then
  # A VM that was stopped for a while runs unattended-upgrades on boot and holds the dpkg
  # lock; wait for it instead of failing.
  APT="sudo DEBIAN_FRONTEND=noninteractive apt-get -o DPkg::Lock::Timeout=900 -q"
  $APT update
  $APT install -y docker.io docker-compose-v2
  sudo usermod -aG docker "$USER"
fi

# Rotate container logs so a long-running stack can never fill the 8 GB root disk.
sudo mkdir -p /etc/docker
echo '{"log-driver":"json-file","log-opts":{"max-size":"20m","max-file":"5"}}' | sudo tee /etc/docker/daemon.json >/dev/null
sudo systemctl enable docker >/dev/null 2>&1
sudo systemctl restart docker

# 1 GiB of RAM is tight for Postgres + app + proxy: swap is the safety net against OOM kills,
# and a low swappiness keeps it a last resort rather than a performance tax.
SWAP_SIZE=2G
if [ "$(sudo swapon --show=NAME,SIZE --noheadings | awk '$1=="/swapfile"{print $2}')" != "$SWAP_SIZE" ]; then
  sudo swapoff /swapfile 2>/dev/null || true
  sudo rm -f /swapfile
  sudo fallocate -l "$SWAP_SIZE" /swapfile
  sudo chmod 600 /swapfile
  sudo mkswap /swapfile >/dev/null
  sudo swapon /swapfile
  grep -q '^/swapfile ' /etc/fstab || echo '/swapfile none swap sw 0 0' | sudo tee -a /etc/fstab >/dev/null
fi

# Deeper accept queues for on-sale bursts (thousands of connections arriving at once).
printf 'vm.swappiness=10\nnet.core.somaxconn=8192\nnet.ipv4.tcp_max_syn_backlog=8192\n' |
  sudo tee /etc/sysctl.d/90-seat-reservation.conf >/dev/null
sudo sysctl -q --system

mkdir -p ~/seat-reservation
echo "bootstrap done: $(docker --version 2>/dev/null || sudo docker --version)"
free -m | head -2
swapon --show
