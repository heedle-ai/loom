#!/usr/bin/env bash
# Checks docs/wiki/Invariants.md against the code, in both directions:
#   docs -> code: every test a row names exists (asked of the Go toolchain
#                 with `go test -list`), and every `ci:NAME` guard is a step
#                 named NAME in .github/workflows/ci.yml;
#   code -> docs: every guard test (TestLaw_*, TestInv_*, Fuzz*) is listed.
# It also checks rows have unique IDs and a guard, and that every image the
# wiki references exists and every SVG is well-formed XML.
# Passing is checked by the normal `go test -race ./...` step.
set -euo pipefail
cd "$(dirname "$0")/.."
inv=docs/wiki/Invariants.md
fail=0
err() { echo "invariants: $*" >&2; fail=1; }

[ -f "$inv" ] || { err "$inv is missing"; exit 1; }

have=""
if [ -f go.mod ]; then
  have=$(go test -list '.*' ./... | grep -E '^(Test|Fuzz|Example)' | sort -u || true)
fi
listed=$(grep -oE '`(Test|Fuzz|Example)[A-Za-z0-9_]*`' "$inv" | tr -d '`' | sort -u || true)

for t in $listed; do
  grep -qx "$t" <<<"$have" || err "Invariants.md names $t, but no such test exists"
done
for t in $(grep -E '^(TestLaw_|TestInv_|Fuzz)' <<<"$have" || true); do
  grep -qx "$t" <<<"$listed" || err "$t guards an invariant but isn't listed in Invariants.md"
done
for g in $(grep -oE '`ci:[A-Za-z0-9_-]+`' "$inv" | tr -d '`' | sed 's/^ci://' | sort -u || true); do
  grep -qE "name: $g\$" .github/workflows/ci.yml || err "Invariants.md names ci:$g, but ci.yml has no step named $g"
done

rows=$(grep -E '^\| [A-Z]+-[0-9]+ \|' "$inv" || true)
[ -n "$rows" ] || err "no invariant rows (| ID-n | ... |) in $inv"
dups=$(cut -d'|' -f2 <<<"$rows" | tr -d ' ' | sort | uniq -d)
[ -z "$dups" ] || err "duplicate invariant IDs: $dups"
while IFS= read -r row; do
  [ -z "$row" ] && continue
  grep -qE '`(Test|Fuzz|Example)[A-Za-z0-9_]*`|`ci:[A-Za-z0-9_-]+`' <<<"$row" ||
    err "row has no guard: $(cut -d'|' -f2 <<<"$row")"
done <<<"$rows"

for img in $(grep -ohE '\[\[/images/[^]|]+' docs/wiki/*.md | sed 's#^\[\[/##' | sort -u || true); do
  [ -f "docs/wiki/$img" ] || err "wiki references missing image $img"
done
for svg in docs/wiki/images/*.svg; do
  [ -e "$svg" ] || continue
  python3 -c 'import sys, xml.dom.minidom; xml.dom.minidom.parse(sys.argv[1])' "$svg" || err "invalid SVG $svg"
done

[ "$fail" = 0 ] && echo "invariants: $(wc -l <<<"$rows" | tr -d ' ') rows checked, docs and tests agree"
exit "$fail"
