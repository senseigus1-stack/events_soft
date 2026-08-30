#!/usr/bin/env bash
set -Eeuo pipefail

if [[ ${EUID} -ne 0 ]]; then
  echo "Run as root: sudo ./deploy/kubernetes/install-k3s.sh"
  exit 1
fi

if [[ ! -r /etc/os-release ]]; then
  echo "A modern systemd-based Linux distribution is required."
  exit 1
fi

. /etc/os-release
case "${ID}" in
  ubuntu|debian) ;;
  *) echo "This bootstrap is tested on Ubuntu and Debian; detected ${ID}." ;;
esac

cpu_count="$(nproc)"
memory_mb="$(awk '/MemTotal/ {print int($2 / 1024)}' /proc/meminfo)"
if (( cpu_count < 2 || memory_mb < 3800 )); then
  echo "Warning: production requires at least 2 vCPU/4 GB RAM; 4 vCPU/8 GB is recommended."
fi

apt-get update
apt-get install -y ca-certificates curl git openssl tar

tmp_dir="$(mktemp -d)"
trap 'rm -rf -- "${tmp_dir}"' EXIT

k3s_channel="${K3S_CHANNEL:-v1.36}"
curl -fsSL https://get.k3s.io -o "${tmp_dir}/install-k3s.sh"
chmod 0700 "${tmp_dir}/install-k3s.sh"
INSTALL_K3S_CHANNEL="${k3s_channel}" \
  "${tmp_dir}/install-k3s.sh" server \
  --write-kubeconfig-mode=600

helm_version="${HELM_VERSION:-v4.2.4}"
case "$(uname -m)" in
  x86_64) helm_arch=amd64 ;;
  aarch64|arm64) helm_arch=arm64 ;;
  *) echo "Unsupported Helm architecture: $(uname -m)"; exit 1 ;;
esac
helm_archive="helm-${helm_version}-linux-${helm_arch}.tar.gz"
curl -fsSL "https://get.helm.sh/${helm_archive}" -o "${tmp_dir}/${helm_archive}"
curl -fsSL "https://get.helm.sh/${helm_archive}.sha256sum" -o "${tmp_dir}/${helm_archive}.sha256sum"
helm_checksum="$(awk '{print $1}' "${tmp_dir}/${helm_archive}.sha256sum")"
printf '%s  %s\n' "${helm_checksum}" "${tmp_dir}/${helm_archive}" | sha256sum -c -
tar -xzf "${tmp_dir}/${helm_archive}" -C "${tmp_dir}"
install -m 0755 "${tmp_dir}/linux-${helm_arch}/helm" /usr/local/bin/helm

deploy_user="${SUDO_USER:-root}"
deploy_home="$(getent passwd "${deploy_user}" | cut -d: -f6)"
if [[ -n "${deploy_home}" ]]; then
  deploy_group="$(id -gn "${deploy_user}")"
  install -d -m 0700 -o "${deploy_user}" -g "${deploy_group}" "${deploy_home}/.kube"
  install -m 0600 -o "${deploy_user}" -g "${deploy_group}" \
    /etc/rancher/k3s/k3s.yaml "${deploy_home}/.kube/config"
fi

systemctl enable --now k3s
k3s kubectl wait --for=condition=Ready node --all --timeout=180s
echo "K3s ${k3s_channel} and Helm ${helm_version} are ready. Re-login, then run deploy/kubernetes/deploy.sh."
