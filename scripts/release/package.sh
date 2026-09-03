#!/usr/bin/env bash
# package.sh — build the release tarball for one target triple.
#
# Usage: scripts/release/package.sh <binary> <version> <target-triple> <dist-dir>
#
# Produces, in <dist-dir>:
#   open-pubusb-v<version>-<target-triple>.tar.gz          (flat: open-pubusb, README.md)
#   open-pubusb-v<version>-<target-triple>.tar.gz.sha256   ("<hash>  <archive name>")
#
# The archive name and its flat layout are the contract the `open_pubusb`
# Ansible role installs from (ansible/roles/open_pubusb/vars/main.yml,
# tasks/install_systemd.yml): the role downloads
# <release>/v<version>/open-pubusb-v<version>-<triple>.tar.gz, verifies it
# against <release>/v<version>/SHA256SUMS, and expects the `open-pubusb`
# binary at the top level of the extracted tree. Both the GitHub Release
# workflow and the CI molecule mirror go through this script so the two can
# never drift apart.
set -euo pipefail

if [[ $# -ne 4 ]]; then
  echo "usage: ${0##*/} <binary> <version> <target-triple> <dist-dir>" >&2
  exit 2
fi
binary=$1
version=$2
triple=$3
dist=$4

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
name="open-pubusb-v${version}-${triple}"

stage=$(mktemp -d)
trap 'rm -rf "${stage}"' EXIT
install -m 0755 "${binary}" "${stage}/open-pubusb"
install -m 0644 "${repo_root}/README.md" "${stage}/README.md"

mkdir -p "${dist}"
tar -C "${stage}" -czf "${dist}/${name}.tar.gz" open-pubusb README.md
(cd "${dist}" && sha256sum "${name}.tar.gz" > "${name}.tar.gz.sha256")
echo "${dist}/${name}.tar.gz"
