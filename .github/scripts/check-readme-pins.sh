#!/usr/bin/env bash

set -euo pipefail

repo_url="https://github.com/BotResources/br-drive"
readme="README.md"

workspace_version=$(grep -m1 '^version = ' Cargo.toml | sed -E 's/^version = "([^"]+)".*/\1/')
if [ -z "$workspace_version" ]; then
  echo "::error file=Cargo.toml::could not extract [workspace.package] version" >&2
  exit 1
fi
expected_tag="v${workspace_version}"

if [ ! -f "$readme" ]; then
  echo "::error::${readme} is missing" >&2
  exit 1
fi

fail=0
found=0

while IFS= read -r line; do
  [ -n "$line" ] || continue
  found=1
  if ! grep -qE "tag = \"${expected_tag}\"" <<<"$line"; then
    echo "::error file=${readme}::self-pin does not carry tag = \"${expected_tag}\": ${line}" >&2
    fail=1
    continue
  fi
  if ! grep -qE "version = \"${workspace_version}\"" <<<"$line"; then
    echo "::error file=${readme}::self-pin does not carry version = \"${workspace_version}\" beside the tag (a tag-only git dep is a wildcard and cargo-deny denies it): ${line}" >&2
    fail=1
    continue
  fi
  echo "✓ ${readme}: self-pin on ${expected_tag} / ${workspace_version}"
done < <(grep -F "${repo_url}\"" "$readme" | grep -F 'git = ' || true)

if [ "$found" -eq 0 ]; then
  echo "::error file=${readme}::no self-pin line found (expected a git = \"${repo_url}\" dependency snippet)" >&2
  fail=1
fi

exit $fail
