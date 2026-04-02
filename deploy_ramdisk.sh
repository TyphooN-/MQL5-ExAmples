#!/bin/bash
# deploy_ramdisk.sh — Move BarCacheWriter SQLite DBs to /dev/shm (tmpfs ramdisk)
#
# Creates symlinks from each MT5 instance's MQL5/Files/typhoon_mt5_cache.db
# to /dev/shm/typhoon_mt5_cache_N.db (one per instance to avoid write conflicts).
#
# Effect: ALL BarCacheWriter I/O stays in RAM. Zero Wine disk overhead.
# /dev/shm is tmpfs — data does NOT survive reboot. BarCacheWriter re-exports
# all history on startup with an empty DB (~5-10 min for 851 symbols).
#
# Usage:
#   chmod +x deploy_ramdisk.sh
#   ./deploy_ramdisk.sh          # Set up ramdisk symlinks
#   ./deploy_ramdisk.sh --undo   # Revert to local files
#
# Prerequisites:
#   - Stop all MT5 instances before running
#   - /dev/shm must have enough space (each DB ~1-2GB, check with: df -h /dev/shm)

set -euo pipefail

RAMDISK="/dev/shm"
MT5_BASE="$HOME"
DB_NAME="typhoon_mt5_cache.db"

# Find all MT5 instances
INSTANCES=$(find "$MT5_BASE" -maxdepth 1 -name ".mt5_*" -type d 2>/dev/null | sort)

if [ -z "$INSTANCES" ]; then
    echo "No MT5 instances found in $MT5_BASE/.mt5_*"
    exit 1
fi

echo "Found $(echo "$INSTANCES" | wc -l) MT5 instances"
echo "Ramdisk: $RAMDISK ($(df -h $RAMDISK | tail -1 | awk '{print $4}') available)"
echo ""

if [ "${1:-}" = "--undo" ]; then
    echo "=== UNDOING ramdisk symlinks ==="
    for inst in $INSTANCES; do
        name=$(basename "$inst")
        files_dir="$inst/drive_c/Program Files/Darwinex MetaTrader 5/MQL5/Files"
        db_path="$files_dir/$DB_NAME"
        ramdisk_db="$RAMDISK/${DB_NAME%.db}_${name}.db"

        if [ -L "$db_path" ]; then
            echo "  $name: removing symlink"
            rm "$db_path"
            # Copy ramdisk DB back if it exists
            if [ -f "$ramdisk_db" ]; then
                echo "  $name: copying ramdisk DB back to local"
                cp "$ramdisk_db" "$db_path"
                rm "$ramdisk_db"
            fi
        else
            echo "  $name: not a symlink, skipping"
        fi
    done
    echo "Done. Restart MT5 instances."
    exit 0
fi

echo "=== Setting up ramdisk symlinks ==="
for inst in $INSTANCES; do
    name=$(basename "$inst")
    files_dir="$inst/drive_c/Program Files/Darwinex MetaTrader 5/MQL5/Files"
    db_path="$files_dir/$DB_NAME"
    ramdisk_db="$RAMDISK/${DB_NAME%.db}_${name}.db"

    if [ ! -d "$files_dir" ]; then
        echo "  $name: MQL5/Files/ not found, skipping"
        continue
    fi

    if [ -L "$db_path" ]; then
        echo "  $name: already a symlink → $(readlink "$db_path")"
        continue
    fi

    # Move existing DB to ramdisk (preserves data for this session)
    if [ -f "$db_path" ]; then
        size=$(du -h "$db_path" | cut -f1)
        echo "  $name: moving existing DB ($size) to ramdisk..."
        mv "$db_path" "$ramdisk_db"
    else
        echo "  $name: no existing DB, creating empty ramdisk target"
        touch "$ramdisk_db"
    fi

    # Create symlink
    ln -sf "$ramdisk_db" "$db_path"
    echo "  $name: $db_path → $ramdisk_db"
done

echo ""
echo "Done! Ramdisk DBs:"
ls -lh $RAMDISK/typhoon_mt5_cache_*.db 2>/dev/null || echo "  (none yet — will be created on first BarCacheWriter run)"
echo ""
echo "Next steps:"
echo "  1. Start MT5 instances (BarCacheWriter will re-export if DB was empty)"
echo "  2. In TyphooN-Terminal Settings, add these Mt5Sync sources:"
for inst in $INSTANCES; do
    name=$(basename "$inst")
    files_dir="$inst/drive_c/Program Files/Darwinex MetaTrader 5/MQL5/Files"
    if [ -d "$files_dir" ]; then
        echo "     $RAMDISK/${DB_NAME%.db}_${name}.db"
    fi
done
echo ""
echo "NOTE: /dev/shm data does NOT survive reboot."
echo "      BarCacheWriter re-exports everything on startup (~5-10 min)."
echo "      Run this script again after each reboot."
