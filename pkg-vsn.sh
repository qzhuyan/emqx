#!/usr/bin/env bash
set -euo pipefail

# This script prints the release version for emqx

# ensure dir
cd -P -- "$(dirname -- "$0")"

RELEASE="$(grep -E 'define.+EMQX_RELEASE,' include/emqx_release.hrl | cut -d '"' -f2)"

if [ -d .git ] && ! git describe --tags --match "${RELEASE}" --exact >/dev/null 2>&1; then
    SUFFIX="-$(git rev-parse HEAD | cut -b1-8)"
fi

echo "${RELEASE}${SUFFIX:-}"
