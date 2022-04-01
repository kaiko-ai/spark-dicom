#!/usr/bin/env sh
#
# Runs shellcheck on all *.sh files. This script uses sh because
# the shellcheck-alpine image doesn't have bash (in the CI).

set -e
command -v shellcheck || { echo "shellcheck not found. Please install it: https://github.com/koalaman/shellcheck"; exit 1; }

scripts=$(find . -name "*.sh") # We look for all scripts, to ensure
# newly written scripts get automatically checked.
set +e  # so that the loop below doesn't stop at the first error and all errors
# are reported at once

rc="0"

for script in $scripts # In a loop, for more readable output
do
  echo "shellcheck $script"
  shellcheck "$script" || rc="$?"
done

exit "$rc"
