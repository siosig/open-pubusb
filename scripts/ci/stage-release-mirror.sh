#!/usr/bin/env bash
# stage-release-mirror.sh — lay out a local mirror of the GitHub Release the
# `open_pubusb` Ansible role installs from, using the binary of an image that
# was already built from this checkout.
#
# Usage: scripts/ci/stage-release-mirror.sh <image> <version> <dist-dir>
#
# Result (serve <dist-dir> over HTTP and point open_pubusb_release_base_url at it):
#   <dist-dir>/v<version>/open-pubusb-v<version>-<triple>.tar.gz
#   <dist-dir>/v<version>/SHA256SUMS
#
# <triple> is derived from the machine running this script, which is also the
# machine the molecule instance runs on, so it matches what the role's
# ansible_architecture mapping will ask for.
set -euo pipefail

if [[ $# -ne 3 ]]; then
  echo "usage: ${0##*/} <image> <version> <dist-dir>" >&2
  exit 2
fi
image=$1
version=$2
dist=$3

case "$(uname -m)" in
  x86_64) triple=x86_64-unknown-linux-musl ;;
  aarch64|arm64) triple=aarch64-unknown-linux-musl ;;
  *) echo "unsupported architecture: $(uname -m)" >&2; exit 1 ;;
esac

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
work=$(mktemp -d)
trap 'rm -rf "${work}"' EXIT

container=$(docker create "${image}")
docker cp "${container}:/usr/local/bin/open-pubusb" "${work}/open-pubusb"
docker rm -f "${container}" >/dev/null

release_dir="${dist}/v${version}"
"${script_dir}/../release/package.sh" "${work}/open-pubusb" "${version}" "${triple}" "${release_dir}"
(cd "${release_dir}" && cat ./*.tar.gz.sha256 | sort -k2 > SHA256SUMS && rm -f ./*.tar.gz.sha256)
echo "release mirror staged under ${release_dir}:"
ls -l "${release_dir}"
