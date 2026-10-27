#!/usr/bin/env bash
# Bash remains Make's shell so .ONESHELL strips internal @ recipe prefixes.
exec python3 "${BASH_SOURCE[0]%/*}/make-recipe.py" "$@"
