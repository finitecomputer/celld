#!/usr/bin/env bash
# Proves build's header gate (rusty-v8.sh check-headers, docs/v8.md) on a
# scratch git repository of headers, as V8's include/ is laid out:
#   - a changed header whose exact (old blob, new blob) pair the allowlist
#     lists passes, named; so does v8-version.h's V8_PATCH_LEVEL alone;
#   - refused, naming what changed: a change the allowlist does not list
#     (a comment's too), a listed header with another new blob, an added
#     header, any other v8-version.h define, a malformed allowlist line, and
#     a list of changed headers that can't be read (a bad revision).
#
#   tools/v8/headers-test.sh
set -euo pipefail

SCRIPT="$(dirname "$0")/rusty-v8.sh"
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
repo=$work/v8
git init -q "$repo"
git -C "$repo" config user.name headers-test
git -C "$repo" config user.email headers-test@localhost
mkdir "$repo/include"
cat > "$repo/include/v8-function.h" <<'EOF'
#ifndef INCLUDE_V8_FUNCTION_H_
#define INCLUDE_V8_FUNCTION_H_
#define V8_FUNCTION_FLAG 1
namespace v8 {
class Function {
 public:
  /**
   * Calls the function.
   */
  int Call(int argc);  // the count
};
}  // namespace v8
#endif
EOF
cat > "$repo/include/v8-version.h" <<'EOF'
#define V8_MAJOR_VERSION 15
#define V8_MINOR_VERSION 8
#define V8_BUILD_NUMBER 62
#define V8_PATCH_LEVEL 31
EOF
git -C "$repo" add -A && git -C "$repo" commit -qm base
base=$(git -C "$repo" rev-parse HEAD)

comment() { sed -i 's|   \* Calls the function.|   * Calls the function.\n   *\n   * Passing no arguments is allowed.|; s|// the count|// the argument count|' include/v8-function.h; }
patch_level() { sed -i 's/V8_PATCH_LEVEL 31/V8_PATCH_LEVEL 68/' include/v8-version.h; }
declaration() { sed -i 's/int Call(int argc);/int Call(int argc, int flags);/' include/v8-function.h; }
macro() { sed -i 's/V8_FUNCTION_FLAG 1/V8_FUNCTION_FLAG 2/' include/v8-function.h; }
added() { echo 'int Added();' > include/v8-added.h; }
version() { sed -i 's/V8_BUILD_NUMBER 62/V8_BUILD_NUMBER 63/' include/v8-version.h; }
both() { comment; patch_level; }
nothing() { :; }

# A revision of the base with one edit, committed; its SHA on stdout.
revision() {
  git -C "$repo" checkout -q --detach "$base"
  (cd "$repo" && "$1")
  git -C "$repo" add -A && git -C "$repo" commit -q --allow-empty -m "$1"
  git -C "$repo" rev-parse HEAD
}

# The allowlists: the comment change's pair listed, nothing listed, a bad line.
blob() { git -C "$repo" rev-parse "$1:include/v8-function.h"; }
reviewed=$(revision comment)
listed=$work/listed
empty=$work/empty
malformed=$work/malformed
echo "include/v8-function.h $(blob "$base") $(blob "$reviewed") a comment, reviewed" > "$listed"
printf '# nothing reviewed yet\n' > "$empty"
{ cat "$listed"; echo "include/v8-function.h not-a-blob"; } > "$malformed"

failures=0
# $1: what is checked; $2: pass or fail; $3: a string the output must hold;
# $4: the allowlist; $5: the edit making the revision checked; $6: the
# revision checked against (default the base).
case_() {
  local what=$1 want=$2 needle=$3 list=$4 to from=${6:-$base} out status got=fail
  to=$(revision "$5")
  status=0
  out=$("$SCRIPT" check-headers "$repo" "$from" "$to" "$list" 2>&1) || status=$?
  (( status == 0 )) && got=pass
  if [[ $got != "$want" || $out != *"$needle"* ]]; then
    echo "FAIL $what (wanted $want with '$needle', got exit $status):"
    printf '%s\n' "$out" | sed 's/^/  /'
    failures=$((failures + 1))
  else
    echo "ok   $what"
  fi
}

case_ "the listed pair" pass "pairs for include/v8-function.h" "$listed" comment
case_ "the listed pair and the patch level" pass "pairs for include/v8-function.h" "$listed" both
case_ "the patch level alone" pass "but for V8_PATCH_LEVEL" "$empty" patch_level
case_ "nothing changed" pass "but for V8_PATCH_LEVEL" "$empty" nothing
case_ "an unlisted change, a comment's" fail "include/v8-function.h changed" "$empty" comment
case_ "an unlisted change, shown stripped of comments for review" fail "(no difference outside comments)" "$empty" comment
case_ "a listed header with another new blob (a declaration)" fail "include/v8-function.h changed" "$listed" declaration
case_ "a listed header with another new blob (a macro)" fail "include/v8-function.h changed" "$listed" macro
case_ "an added header" fail "include/v8-added.h was added or removed" "$listed" added
case_ "another v8-version.h define" fail "v8-version.h changed beyond the patch level" "$listed" version
case_ "a malformed allowlist line" fail "a line is '<header> <old blob> <new blob> <reason>'" "$malformed" comment
case_ "a list of changed headers that can't be read" fail "can't list the public headers" "$listed" comment 0000000000000000000000000000000000000bad

if (( failures > 0 )); then
  echo "$failures case(s) failed"
  exit 1
fi
echo "all header cases passed"
