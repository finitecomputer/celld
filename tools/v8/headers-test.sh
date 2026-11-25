#!/usr/bin/env bash
# Proves build's header gate (rusty-v8.sh check-headers, docs/v8.md) on a
# scratch git repository of headers, as V8's include/ is laid out:
#   - a header changed in its comments alone passes, named;
#   - v8-version.h's V8_PATCH_LEVEL alone passes;
#   - a changed declaration, a changed macro, an added header, and any other
#     v8-version.h define each fail, naming what changed.
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

failures=0
# $1: what is checked; $2: pass or fail; $3: a string the output must hold;
# then the edit that makes the revision to check, run in the repository.
case_() {
  local what=$1 want=$2 needle=$3 out status
  shift 3
  git -C "$repo" checkout -q --detach "$base"
  (cd "$repo" && "$@")
  git -C "$repo" add -A && git -C "$repo" commit -qm "$what"
  status=0
  out=$("$SCRIPT" check-headers "$repo" "$base" HEAD 2>&1) || status=$?
  local got=fail
  (( status == 0 )) && got=pass
  if [[ $got != "$want" || $out != *"$needle"* ]]; then
    echo "FAIL $what (wanted $want with '$needle', got exit $status):"
    printf '%s\n' "$out" | sed 's/^/  /'
    failures=$((failures + 1))
  else
    echo "ok   $what"
  fi
}

comment() { sed -i 's|   \* Calls the function.|   * Calls the function.\n   *\n   * Passing no arguments is allowed.|; s|// the count|// the argument count|' include/v8-function.h; }
patch_level() { sed -i 's/V8_PATCH_LEVEL 31/V8_PATCH_LEVEL 68/' include/v8-version.h; }
declaration() { sed -i 's/int Call(int argc);/int Call(int argc, int flags);/' include/v8-function.h; }
macro() { sed -i 's/V8_FUNCTION_FLAG 1/V8_FUNCTION_FLAG 2/' include/v8-function.h; }
added() { echo 'int Added();' > include/v8-added.h; }
version() { sed -i 's/V8_BUILD_NUMBER 62/V8_BUILD_NUMBER 63/' include/v8-version.h; }
both() { comment; patch_level; }

case_ "a header changed in its comments alone" pass "comments in include/v8-function.h" comment
case_ "the patch level alone" pass "but for V8_PATCH_LEVEL" patch_level
case_ "comments and the patch level" pass "comments in include/v8-function.h" both
case_ "a changed declaration" fail "include/v8-function.h changed beyond its comments" declaration
case_ "a changed macro" fail "include/v8-function.h changed beyond its comments" macro
case_ "an added header" fail "include/v8-added.h was added or removed" added
case_ "another v8-version.h define" fail "v8-version.h changed beyond the patch level" version

if (( failures > 0 )); then
  echo "$failures case(s) failed"
  exit 1
fi
echo "all header cases passed"
