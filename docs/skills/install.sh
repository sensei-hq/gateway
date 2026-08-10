#!/usr/bin/env sh
# Install the gateway "using-gateway" agent skill into the current repo.
#
#   curl -fsSL https://gateway.sensei-hq.com/skills/install.sh | sh
#
# Downloads SKILL.md into .claude/skills/using-gateway/ (a Claude Code project skill,
# also readable by other coding agents). Re-run any time to update to the latest
# published version. Override the target with GATEWAY_SKILL_DEST.
set -eu

BASE="${GATEWAY_SKILLS_BASE:-https://gateway.sensei-hq.com/skills}"
DEST="${GATEWAY_SKILL_DEST:-.claude/skills/using-gateway}"

mkdir -p "$DEST"
curl -fsSL "$BASE/using-gateway/SKILL.md" -o "$DEST/SKILL.md"

echo "✓ Installed the using-gateway skill → $DEST/SKILL.md"
echo "  Re-run this script to update. Docs: https://gateway.sensei-hq.com/docs"
