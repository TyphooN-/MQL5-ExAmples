#!/bin/bash
# deploy_ramdisk.sh — Move BarCacheWriter SQLite DBs + demand.txt to /dev/shm
#
# For each active MT5 instance this script creates two ramdisk symlinks:
#   1. MQL5/Files/typhoon_mt5_cache.db → /dev/shm/typhoon_mt5_cache_<inst>.db
#      (bar cache — heavy writes, journal I/O)
#   2. Common/Files/demand.txt → /dev/shm/typhoon_demand_<inst>.txt
#      (TyphooN-Terminal → EA gap-fill request file — tiny, but co-locating it
#       in /dev/shm lets the user cat/tail it directly without walking the
#       Wine prefix tree)
#
# BarCacheWriter re-exports all history on startup (~5-10 min for 851 symbols)
# and the terminal rewrites demand.txt on its next heartbeat cycle (~5s).
#
# Usage:
#   chmod +x deploy_ramdisk.sh
#   ./deploy_ramdisk.sh          # Set up (stop MT5 first)
#   ./deploy_ramdisk.sh --undo   # Revert to local files
#
# Prerequisites:
#   - Stop all MT5 instances before running
#   - /dev/shm must have enough space (check: df -h /dev/shm)
#   - MT5 has been started at least once per instance so Common/Files exists

set -euo pipefail

# Auto-detect ramdisk: prefer /dev/shm, fall back to /run/user/$UID, then /tmp
if [ -d /dev/shm ] && df /dev/shm 2>/dev/null | grep -q tmpfs; then
    RAMDISK="/dev/shm"
elif [ -d "/run/user/$(id -u)" ] && df "/run/user/$(id -u)" 2>/dev/null | grep -q tmpfs; then
    RAMDISK="/run/user/$(id -u)"
elif [ -d /tmp ] && df /tmp 2>/dev/null | grep -q tmpfs; then
    RAMDISK="/tmp"
else
    echo "ERROR: No tmpfs ramdisk found (/dev/shm, /run/user/$UID, /tmp)"
    echo "BarCacheWriter will write directly to disk (slower, more SSD wear)"
    exit 1
fi
MT5_BASE="$HOME"
DB_NAME="typhoon_mt5_cache.db"
DEMAND_NAME="demand.txt"   # BarCacheWriter reads ${accountTag}_demand.txt first, falls back to demand.txt (FILE_COMMON).

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
        ramdisk_demand="$RAMDISK/typhoon_demand_${name}.txt"

        if [ -L "$db_path" ]; then
            echo "  $name: removing DB symlink"
            rm "$db_path"
            echo "  $name: BarCacheWriter will create fresh DB on next start"
        else
            echo "  $name: DB not a symlink, skipping"
        fi
        # Clean up ramdisk DB
        if [ -f "$ramdisk_db" ]; then
            size=$(du -h "$ramdisk_db" | cut -f1)
            echo "  $name: removing ramdisk DB ($size)"
            rm "$ramdisk_db"
        fi

        # Remove demand.txt symlinks (iterate all Common/Files candidates)
        while IFS= read -r common_dir; do
            demand_link="$common_dir/$DEMAND_NAME"
            if [ -L "$demand_link" ]; then
                echo "  $name: removing demand.txt symlink ($demand_link)"
                rm "$demand_link"
            fi
        done < <(find "$inst/drive_c/users" -maxdepth 6 -type d -path "*/AppData/Roaming/MetaQuotes/Terminal/Common/Files" 2>/dev/null)

        if [ -f "$ramdisk_demand" ]; then
            echo "  $name: removing ramdisk demand.txt"
            rm "$ramdisk_demand"
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
    ramdisk_demand="$RAMDISK/typhoon_demand_${name}.txt"

    if [ ! -d "$files_dir" ]; then
        echo "  $name: MQL5/Files/ not found at $files_dir, skipping"
        continue
    fi

    # --- DB symlink -------------------------------------------------------
    if [ -L "$db_path" ]; then
        target=$(readlink "$db_path")
        echo "  $name: DB already symlinked → $target"
    else
        # Copy existing DB to ramdisk (preserves all historical data!)
        # Previously this deleted the DB, causing data loss on first run.
        if [ -f "$db_path" ]; then
            size=$(du -h "$db_path" | cut -f1)
            echo "  $name: copying existing DB ($size) to ramdisk — zero data loss"
            if ! cp "$db_path" "$ramdisk_db"; then
                echo "  ERROR: failed to copy DB to ramdisk — check disk space on $RAMDISK"
                echo "  Available: $(df -h "$RAMDISK" | tail -1 | awk '{print $4}')"
                continue
            fi
            rm "$db_path"
        elif [ -f "$ramdisk_db" ]; then
            echo "  $name: ramdisk DB already exists ($(du -h "$ramdisk_db" | cut -f1))"
        fi

        # Create symlink: MQL5/Files/typhoon_mt5_cache.db → /dev/shm/typhoon_mt5_cache_.mt5_X.db
        if ! ln -sf "$ramdisk_db" "$db_path"; then
            echo "  ERROR: failed to create symlink $db_path → $ramdisk_db"
            continue
        fi
        echo "  $name: $db_path → $ramdisk_db"
    fi

    # --- demand.txt symlink ----------------------------------------------
    # Find every Common/Files dir under this Wine prefix (glob over user names).
    # Typically exactly one match; multiple happen only if the prefix has been
    # reused across Wine user names.
    demand_any=0
    while IFS= read -r common_dir; do
        demand_any=1
        demand_link="$common_dir/$DEMAND_NAME"
        if [ -L "$demand_link" ]; then
            echo "  $name: demand.txt already symlinked → $(readlink "$demand_link")"
            continue
        fi
        if [ -f "$demand_link" ]; then
            echo "  $name: moving existing demand.txt to ramdisk"
            mv "$demand_link" "$ramdisk_demand" 2>/dev/null || rm -f "$demand_link"
        fi
        if ! ln -sf "$ramdisk_demand" "$demand_link"; then
            echo "  ERROR: failed to symlink $demand_link → $ramdisk_demand"
            continue
        fi
        echo "  $name: $demand_link → $ramdisk_demand"
    done < <(find "$inst/drive_c/users" -maxdepth 6 -type d -path "*/AppData/Roaming/MetaQuotes/Terminal/Common/Files" 2>/dev/null)

    if [ "$demand_any" = 0 ]; then
        echo "  $name: Common/Files/ not found — start MT5 once to create it, then re-run"
    fi
done

echo ""
echo "Done! Ramdisk state:"
ls -lh $RAMDISK/typhoon_mt5_cache_*.db 2>/dev/null || echo "  (no DBs — will be created on first BarCacheWriter run)"
ls -lh $RAMDISK/typhoon_demand_*.txt 2>/dev/null || echo "  (no demand.txt yet — will be created on first terminal heartbeat)"
echo ""
echo "Next steps:"
echo "  1. Start MT5 instances (BarCacheWriter will re-export all history)"
echo "  2. In TyphooN-Terminal Settings → MT5 BarCacheWriter Sources, set:"
for name in $ACTIVE_INSTANCES; do
    echo "     $RAMDISK/${DB_NAME%.db}_${name}.db"
done
echo ""
echo "  Inspect demand.txt directly (no Wine prefix traversal):"
for name in $ACTIVE_INSTANCES; do
    echo "     cat $RAMDISK/typhoon_demand_${name}.txt"
done
echo ""
echo "NOTE: /dev/shm does NOT survive reboot. Run this script after each reboot."
echo "      BarCacheWriter re-exports everything on startup (~5-10 min);"
echo "      demand.txt regenerates on first terminal heartbeat (~5s)."
echo "      Without this script, BCW + terminal keep using on-disk Common/Files"
echo "      paths — the script is purely an observability + I/O optimisation."
