#!/usr/bin/env bash
set -euo pipefail

mode="${1:-lenient}"

version=$(grep -m1 '^version = ' Cargo.toml | sed -E 's/^version = "([^"]+)".*/\1/')
if [ -z "$version" ]; then
  echo "::error file=Cargo.toml::could not extract the workspace version" >&2
  exit 1
fi

changelog="CHANGELOG.md"
if [ ! -f "$changelog" ]; then
  echo "::error::workspace version ${version} but ${changelog} is missing" >&2
  exit 1
fi

if grep -qE "^## ${version}( |\$)" "$changelog"; then
  echo "✓ v${version} has a released entry"
  exit 0
fi

if [ "$mode" = "release" ]; then
  echo "::notice file=${changelog}::v${version} has no '## ${version}' entry yet; nothing to tag" >&2
  exit 2
fi

if grep -qE "^## Unreleased( |\$)" "$changelog"; then
  echo "✓ v${version} is still under '## Unreleased'"
  exit 0
fi

echo "::error file=${changelog}::v${version} has neither a '## ${version}' nor an '## Unreleased' section" >&2
exit 1
