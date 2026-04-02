#!/bin/bash
# deploy_ramdisk.sh — Move BarCacheWriter SQLite DBs to /dev/shm (tmpfs ramdisk)
#
# Deletes old on-disk databases and creates fresh symlinks to /dev/shm.
# Each MT5 instance gets its own ramdisk DB to avoid write conflicts.
# BarCacheWriter re-exports all history on startup (~5-10 min for 851 symbols).
#
# Usage:
#   chmod +x deploy_ramdisk.sh
#   ./deploy_ramdisk.sh          # Set up (stop MT5 first)
#   ./deploy_ramdisk.sh --undo   # Revert to local files
#
# Prerequisites:
#   - Stop all MT5 instances before running
#   - /dev/shm must have enough space (check: df -h /dev/shm)

set -euo pipefail

RAMDISK="/dev/shm"
MT5_BASE="$HOME"
DB_NAME="typhoon_mt5_cache.db"

# Only process active instances (user specifies or auto-detect)
ACTIVE_INSTANCES="${MT5_INSTANCES:-.mt5_7 .mt5_10 .mt5_11}"

echo "Ramdisk: $RAMDISK ($(df -h $RAMDISK | tail -1 | awk '{print $2}') total, $(df -h $RAMDISK | tail -1 | awk '{print $4}') available)"
echo ""

if [ "${1:-}" = "--undo" ]; then
    echo "=== UNDOING ramdisk symlinks ==="
    for name in $ACTIVE_INSTANCES; do
        inst="$MT5_BASE/$name"
        files_dir="$inst/drive_c/Program Files/Darwinex MetaTrader 5/MQL5/Files"
        db_path="$files_dir/$DB_NAME"
        ramdisk_db="$RAMDISK/${DB_NAME%.db}_${name}.db"

        if [ -L "$db_path" ]; then
            echo "  $name: removing symlink"
            rm "$db_path"
            echo "  $name: BarCacheWriter will create fresh DB on next start"
        else
            echo "  $name: not a symlink, skipping"
        fi
        # Clean up ramdisk DB
        if [ -f "$ramdisk_db" ]; then
            size=$(du -h "$ramdisk_db" | cut -f1)
            echo "  $name: removing ramdisk DB ($size)"
            rm "$ramdisk_db"
        fi
    done
    echo "Done. Restart MT5 instances."
    exit 0
fi

echo "=== Setting up ramdisk for: $ACTIVE_INSTANCES ==="
echo ""

for name in $ACTIVE_INSTANCES; do
    inst="$MT5_BASE/$name"
    files_dir="$inst/drive_c/Program Files/Darwinex MetaTrader 5/MQL5/Files"
    db_path="$files_dir/$DB_NAME"
    ramdisk_db="$RAMDISK/${DB_NAME%.db}_${name}.db"

    if [ ! -d "$files_dir" ]; then
        echo "  $name: MQL5/Files/ not found at $files_dir, skipping"
        continue
    fi

    if [ -L "$db_path" ]; then
        target=$(readlink "$db_path")
        echo "  $name: already symlinked → $target"
        continue
    fi

    # Delete old on-disk DB (data will be re-exported by BarCacheWriter)
    if [ -f "$db_path" ]; then
        size=$(du -h "$db_path" | cut -f1)
        echo "  $name: deleting old DB ($size) — will re-export to ramdisk"
        rm "$db_path"
    fi

    # Clean any stale ramdisk DB from previous run
    if [ -f "$ramdisk_db" ]; then
        rm "$ramdisk_db"
    fi

    # Create symlink: MQL5/Files/typhoon_mt5_cache.db → /dev/shm/typhoon_mt5_cache_.mt5_X.db
    ln -sf "$ramdisk_db" "$db_path"
    echo "  $name: $db_path → $ramdisk_db"
done

echo ""
echo "Done! Ramdisk state:"
ls -lh $RAMDISK/typhoon_mt5_cache_*.db 2>/dev/null || echo "  (empty — DBs will be created on first BarCacheWriter run)"
echo ""
echo "Next steps:"
echo "  1. Start MT5 instances (BarCacheWriter will re-export all history)"
echo "  2. In TyphooN-Terminal Settings → MT5 BarCacheWriter Sources, set:"
for name in $ACTIVE_INSTANCES; do
    echo "     $RAMDISK/${DB_NAME%.db}_${name}.db"
done
echo ""
echo "NOTE: /dev/shm does NOT survive reboot. Run this script after each reboot."
echo "      BarCacheWriter re-exports everything on startup (~5-10 min)."
