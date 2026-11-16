#!/usr/bin/env bash
set -Eeuo pipefail
cd "$(dirname "$0")/../.."
# Inspect STAGED filenames; refuse accidental data and secrets; never modify index.
files="$(git diff --cached --name-only --diff-filter=ACMR)"
if [[ -z "$files" ]]; then echo 'No staged files'; exit 0; fi
printf '%s\n' "$files"
if printf '%s\n' "$files" | grep -Ei '(^|/)(\.env[^/]*|secrets?[^/]*|credentials[^/]*|id_rsa)(/|$)|(^|/)(volumes|checkpoints|__pycache__)/|\.(parquet|csv|jar|zip|db|sqlite|pem|key|p12|pfx|bak|mdf|ldf)$'; then
  echo 'BLOCK: staged filenames contain potential data/secrets' >&2; exit 1
fi
if git diff --cached --check; then echo 'Whitespace check PASS'; else exit 1; fi
# Pattern scan of STAGED text; findings need review, not automatic publication.
if git diff --cached --unified=0 | grep -Ei '^\+[^+].*(password|secret|token|private_key)[[:space:]]*[:=][[:space:]]*["\x27]?[A-Za-z0-9!@#_\-]{6,}' ; then
 echo 'BLOCK: possible hard-coded credential in staged diff' >&2; exit 1
fi
echo 'Precommit heuristic PASS (not a comprehensive secret scan)'
