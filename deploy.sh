#!/usr/bin/env bash
set -uo pipefail

REPO_DIR="$(cd "$(dirname "$0")" && pwd)" || { echo "ERROR: cannot resolve script directory"; exit 1; }
SRC_INCLUDE="$REPO_DIR/Include"

# All .mq5 files in repo root are scripts/EAs → deploy to Experts/
EXPERT_FILES=()
for f in "$REPO_DIR"/*.mq5; do
    [ -f "$f" ] || continue
    EXPERT_FILES+=("$(basename "$f")")
done

# Include files with relative paths preserved
INCLUDE_FILES=()
if [ -d "$SRC_INCLUDE" ]; then
    while IFS= read -r -d '' f; do
        INCLUDE_FILES+=("${f#"$SRC_INCLUDE/"}")
    done < <(find "$SRC_INCLUDE" -type f \( -name '*.mqh' -o -name '*.mq5' \) -print0)
fi

if [ ${#EXPERT_FILES[@]} -eq 0 ] && [ ${#INCLUDE_FILES[@]} -eq 0 ]; then
    echo "ERROR: No .mq5 or .mqh files found in $REPO_DIR"
    exit 1
fi

echo "Found ${#EXPERT_FILES[@]} expert files, ${#INCLUDE_FILES[@]} include files"

copied=0
skipped=0
failed=0
total_targets=0

for mt5_dir in /home/typhoon/.mt5_*/; do
    [ -d "$mt5_dir" ] || continue
    MQL5_DIR="$mt5_dir/drive_c/Program Files/Darwinex MetaTrader 5/MQL5"
    if [ ! -d "$MQL5_DIR" ]; then
        echo "SKIP  $(basename "$mt5_dir"): MQL5 directory not found"
        skipped=$((skipped + 1))
        continue
    fi
    total_targets=$((total_targets + 1))
    DST_EXPERTS="$MQL5_DIR/Experts"
    DST_INCLUDE="$MQL5_DIR/Include"
    inst="$(basename "$mt5_dir")"
    inst_failed=0

    for f in "${EXPERT_FILES[@]}"; do
        src="$REPO_DIR/$f"
        dst="$DST_EXPERTS/$f"
        if cmp -s "$src" "$dst" 2>/dev/null; then
            continue
        fi
        if cp "$src" "$dst"; then
            copied=$((copied + 1))
        else
            echo "FAIL  $inst: could not copy $f"
            failed=$((failed + 1))
            inst_failed=$((inst_failed + 1))
        fi
    done

    for f in "${INCLUDE_FILES[@]}"; do
        src="$SRC_INCLUDE/$f"
        dst="$DST_INCLUDE/$f"
        dst_dir="$(dirname "$dst")"
        [ -d "$dst_dir" ] || mkdir -p "$dst_dir"
        if cmp -s "$src" "$dst" 2>/dev/null; then
            continue
        fi
        if cp "$src" "$dst"; then
            copied=$((copied + 1))
        else
            echo "FAIL  $inst: could not copy Include/$f"
            failed=$((failed + 1))
            inst_failed=$((inst_failed + 1))
        fi
    done

    if [ "$inst_failed" -gt 0 ]; then
        echo " WARN  $inst ($inst_failed failures)"
    else
        echo "  OK  $inst"
    fi
done

echo ""
echo "Deploy complete: $total_targets installations, $copied files copied, $skipped skipped, $failed failed"
