/**=             BarCacheWriter.mq5  (TyphooN's Direct SQLite Bar Cache Writer)
 *               Copyright 2026, TyphooN (https://www.marketwizardry.org/)
 *
 * Disclaimer and Licence
 *
 * This file is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * All trading involves risk. You should have received the risk warnings
 * and terms of use in the README.MD file distributed with this software.
 * See the README.MD file for more information and before using this software.
 *
 **/
#property copyright "Copyright 2026 TyphooN (MarketWizardry.org)"
#property link      "https://www.marketwizardry.org/"
#property version   "1.472"
#property description "TTBR binary bar cache + specs + bid/ask to SQLite."
#property description "v1.472: Bound the gap-fill cold-start blob. When a gap-fill request lands on a key with existingCount==0 (cache empty for that SYM:TF), the v1.462 shallow check gated on `existingCount > 0` fell into IncrementalExportSymbolTF, which for a hasExisting=false row recursed into ExportSymbolTF(maxBars=0) — CopyRates from 1970.01.01 → now, unbounded. For a liquid equity on 1Min this yields ~1.37M bars → ~65MB TTBR blob. MT5's DatabaseRead on that single INSERT pushes the SQLite journal past its commit threshold and the transaction is auto-rolled back before the next SafeCommit reaches it — observable as 'cannot commit - no transaction is active' immediately after 'gap-fill OLN:1Min merged to 1368961 bars (req 2929, had 0)', followed by the entire symbol batch rolling back. Fix: shallow = (gapBars > existingCount) so the 0-cached case routes to ExportSymbolTF(symbol, tf, gapBars, ...) — bounded by the terminal's actual request — instead of the incremental path's unbounded fallback. Defense-in-depth: the hasExisting=false / bad-magic / bad-count returns inside IncrementalExportSymbolTF are now capped at MAX_BARS_PER_KEY (100K) so any other path that still lands there can't revive the 65MB write."
#property description "v1.471: Stop the gap-fill→re-flag infinite loop. When IncrementalExportSymbolTF is called for a symbol whose broker side has no new bars since the last export (weekend/holiday stocks, quiet sessions), the function returned existingCount without writing anything — leaving bar_cache.timestamp frozen at its prior value. Mt5Sync's delta reader (get_cache_meta_since) saw the unchanged ts and skipped the row, the terminal's gap detector re-walked the bar_count mismatch, re-flagged the key, wrote a fresh demand.txt request, and the cycle repeated forever (observed: 1031 persistent requests, Mt5Sync reporting 0/0/999). Fix: advance bar_cache.timestamp via a pre-prepared row-level UPDATE (g_stmtBarTouchTs) on every appendCount<=0 branch, so 'freshly checked, nothing to add' propagates through Mt5Sync's ts filter and satisfies the terminal's freshness check. No blob rewrite, no metadata churn — just a single UPDATE bar_cache SET timestamp=?1 WHERE key=?2."
#property description "v1.470: Fix 'cannot commit transaction - SQL statements in progress' on gap-fill + incremental paths. DatabaseRead on the shared g_stmtBarRead SELECT statement leaves the cursor in SQLITE_ROW state — SQLite then refuses any COMMIT on the connection until the statement is stepped to SQLITE_DONE or reset. GetCachedBarCount (v1.462+) and IncrementalExportSymbolTF both read one row and returned, so the batch COMMIT at the end of BatchSize symbols would fail whenever either path fired within the batch window (common: any gap-fill request, any incremental pull with an existing cache row). Explicitly DatabaseReset the cursor after extracting columns at all three g_stmtBarRead call sites. The data path was correct — the work completed — but the transaction rolled back, so the writes were silently discarded until the next cycle's full re-export."
#property description "v1.469: Eliminate redundant metadata upserts. __SYMBOLS__ (7KB JSON of broker symbol names) and __SERVER__ (~60B server+company blob) were being rewritten with a fresh timestamp every 30s cycle. Nothing in the terminal reads __SYMBOLS__ — cached_mt5_symbols is derived from bar_cache keys — and __SERVER__ content is fixed after MT5 login. Mt5Sync's ts-based incremental merge treated the fresh ts as a change signal and re-merged the unchanged payload on every pass, costing ~7KB × 120 cycles/hour = ~840KB/hour of wasted target-side work per MT5 source. __SYMBOLS__ now uses two-tier dedup — fast skip on unchanged symCount within a 300s window (O(1), no SymbolName scan); slow path rebuilds the JSON and string-compares to the cached copy before upserting. __SERVER__ moves to a single OnInit write."
#property description "v1.468: Demand-scoped live bid/ask loop. The terminal's only consumer of bid_ask rows is the chart-refresh loop (app.rs:92134), which discards every row whose symbol doesn't match an open chart — and chart symbols are always in the demand set. Writing quotes for all ~851 broker symbols every 60s was pure waste: ~800 SymbolInfoDouble pairs + ~800 DB upserts per cycle for rows nobody reads. Scope to g_demandSymbols[] (typically 20-50 entries) and fall back to full-sweep only when demand is empty (standalone EA with no terminal attached, or first cycle before demand.txt loads). Cuts bid_ask transaction cost from ~800 upserts/60s to ~25 upserts/60s, freeing Wine syscall budget for the bar rotation that follows."
#property description "v1.467: ShouldEnterInitialBurst switches bar_cache → bar_track count. The terminal's Mt5Sync now deletes merged bar_cache rows from the source /dev/shm DB on every sync pass (post-merge cleanup bounds tmpfs growth). Under the old bar_cache-based cold-start check, every EA re-attach on a warm/synced account would false-positive into burst mode and re-export everything. bar_track rows persist across post-sync deletes — terminal only touches bar_cache — so counting bar_track.last_bar_time > 0 (excluding FAIL_SENTINEL) cleanly distinguishes 'EA has never exported here' from 'terminal already merged + reclaimed'. Same 20% demand-floor heuristic, same 450-row absolute floor, just a different source table."
#property description "v1.466: O(1) demand.txt reload + O(1) metadata gate. Replaces the v1.448 2-cycle (~60s) demand reload gate with a per-cycle mtime stat — FileGetInteger(FILE_MODIFY_DATE) is ~1 µs and returns a stable datetime, so LoadDemandFile now fires on the same tick the terminal rewrites demand.txt instead of up to 60s later. Worst-case gap-fill awareness latency drops from 60s → 0s when mtime changes between ticks (typical case ≤30s, bounded by OnTimer cadence). Also drops the v1.457-era 5-minute lastMetaWrite gate — the Rust terminal's O(1) Mt5Sync uses get_cache_meta_since(ts) to read only changed rows, so throttling metadata writes to reduce mtime churn is no longer needed and only delays fresh-symbol discovery by up to 300s."
#property description "v1.465: Per-(sym,TF) TF gate + O(1) hot-loop lookups. Replaces per-TF-global g_tfLastExportTime[9] with g_trackLastExport[] parallel to g_trackTimes[], so the 80% TF-period gate now fires per (symbol,TF) pair instead of globally per TF — previously the first symbol exported in a TF updated the global timer, gating ALL other symbols on that TF for the next period and starving the rotation queue. Adds g_gapFillMaxBarsByMWIdx[symCount*tfCount] + g_trackIdxByMWIdx[symCount*tfCount] flat-array caches populated by RebuildSlotCaches (runs alongside RebuildDemandIndexBitmap on demand.txt reload / symCount change). Hot loop goes from O(log N) on every (symbol,TF) to O(1). Tiered MaxBarsForTF: 1Min/5Min=50K, 15Min/30Min=30K, 1Hour=30K, 4Hour=20K, 1Day+=10K — gives backtest-useful low-TF history without blowing up H4/D1+ cache size."
#property description "v1.464: RamdiskMode input (default true) — when the DB lives on /dev/shm (deploy_ramdisk.sh), switch to journal_mode=MEMORY + synchronous=OFF. Eliminates SQLite's DELETE-mode journal file create/truncate on every BEGIN/COMMIT, which under Wine translates to a stream of NtCreateFile/NtWriteFile/NtDeleteFile syscalls that dominate ExportAll throughput. Safe on tmpfs (reboot wipes the DB anyway). IntegrityCheck catches any blob corrupted by mid-COMMIT crash on next startup. Non-ramdisk users set RamdiskMode=false to keep DELETE journal + sync=NORMAL for durability."
#property description "v1.463: Startup integrity exports broker's full available history (min(mt5Count, MaxBarsForTF)) instead of the old InitialBarCap=1000 fast-restart limit. Matches the terminal's integrity target; avoids the ~30 s window where v1.462's shallow-redirect fills the gap between a 1000-bar startup stub and the 100K target."
#property description "v1.462: Shallow-cache redirect on gap-fill — when the terminal's gap-fill request asks for more bars than currently cached (integrity target: 100K for M1-H4, full history for D1/W1/MN1), route to ExportSymbolTF(maxBars=gapBars) instead of IncrementalExportSymbolTF. The incremental merge only APPENDS newer bars, so a fresh-but-shallow cache would otherwise never grow. New GetCachedBarCount helper reads the TTBR header for the decision."
#property description "v1.461: Gap-fill bypasses TF-period gating — previously the 80% gate at the top of the per-TF loop skipped rows before the gap-fill check, leaving D1/W1/MN1 gap requests unserviced (terminal looped on the same ~256 stale pairs). Now checks GetGapFillMaxBars first and short-circuits the gating when a request is pending."
#property description "v1.460: DeleteStaleBarCacheRows — DELETE bar rows not refreshed in 14 days (metadata preserved) on same cycle as VACUUM so freed pages reclaim in one pass. Bounds /dev/shm growth when symbols leave the demand set or broker list."
#property description "v1.459: Gap-fill uses IncrementalExportSymbolTF (merge) instead of capped ExportSymbolTF (replace). Preserves cached history; long gaps heal via v1.453 hole-detection full re-export — no data loss."
#property description "v1.458: IntegrityCheck magic-byte check now covers all 4 TTBR bytes — previous partial 'TT' match could let a corrupted blob through and read garbage as the bar count. Matches the full-magic check already used in IncrementalExportSymbolTF."
#property description "v1.457: Drop dead g_demandFileMtime + g_demandFilePath globals — mtime-change reload scheme was replaced by pure cycle cadence (every 2 cycles ~1 min) in v1.448, left orphaned state. Path now logged directly from the local in LoadDemandFile."
#property description "v1.456: IntegrityCheck adds staleness detection — compares DB newest-bar ts_ms against MT5's latest bar and re-exports symbols where the cache is >2 TF periods behind. Catches outage scenarios (EA downtime, broker disconnect, account loss, /dev/shm stale persistence) that previously slipped through the dbCount<100 filter."
#property description "v1.455: LoadDemandFile validates parts[0] is a symbol and parts[1] is a known TF — rejects rows whose symbol slot is accidentally a TF string (e.g. '1Hour:1Hour:0:1500') before they pollute g_demandSymbols and waste rotation cycles on non-existent broker symbols. Belt+suspenders for the terminal-side canonicalisation fix."
#property description "v1.454: IncrementalExportSymbolTF reverse-order guard — if the newest fetched bar is strictly older than the cached last bar, force full re-export. Previously the merge loop would silently append older bars after the existing ones, producing a non-monotonic blob. Covers clock-skew, broker data switch, and bit-rot corruption."
#property description "v1.453: IncrementalExportSymbolTF hole detection — if the fetch window (capped at 200 bars) can't reach back to the last existing bar AND the broker has bars in the gap, full re-export instead of partial merge. Previously a Wine crash / MT5 crash / internet outage / laptop hibernate longer than 200 × tfPeriod silently left a hole in the blob."
#property description "v1.452: ExportAll gate-first — rotation/demand check runs before SymbolName/StringLen/IsForexSymbol, skipping ~650 of 851 symbols/cycle without paying string overhead. IntegrityCheck demand-strict (no fallback to check-all on empty demand list). Fix stale NORMAL-vs-EXCLUSIVE locking comment in inter-batch sleep."
#property description "v1.451: CanExitInitialBurst binary search — replaces 3.15M string comparisons/cycle during burst with ~5800 (log2). Removes dead g_lastHeartbeatWrite variable."
#property description "v1.450: Gap-fill early-bail via g_gapFillActive counter — skips the binary search in the 900-call/cycle hot path whenever no gap-fill request is outstanding (the steady-state case). Preserves v1.449 sort for when requests do arrive."
#property description "v1.449: Gap-fill lookup binary search — replaces 256-entry linear scan in GetGapFillMaxBars/ClearGapFill (hot path, ~230K string comparisons/cycle → ~7K). LoadDemandFile sorts g_gapFill* arrays after populate."
#property description "v1.448: demand.txt v3-only (SYMBOL:TF:LAST_TS:MAX_BARS) — drops dead v1 bare / v2 3-part branches in LoadDemandFile, simplifies parser, removes unused g_demandV2 arrays."
#property description "v1.447: Heartbeat row (mt5:__HEARTBEAT__:{accountId}) + initial-burst mode — on cold start (empty /dev/shm) process demand symbols sequentially with no rotation / TF gating until cache is warm. Terminal can detect staleness via heartbeat."
#property description "v1.446: Thread cached tfStr/tfPeriod into ExportSymbolTF/IncrementalExportSymbolTF — eliminates last TFToStr()/PeriodSeconds() calls from hot path."
#property description "v1.445: Cache TF periods (7659 PeriodSeconds calls/cycle → 0), cache g_tfCount, consolidate TimeCurrent() calls in ExportAll."
#property description "v1.444: Fix specs CSV buffer overflow on non-ASCII descriptions (e.g. USDMXN's 'México' expands in UTF-8) — measure byte length, not char count."
#property description "v1.443: Deeper Wine-overhead pass — O(1) demand bitmap, track-time dirty flag bulk flush (10× fewer bar_track writes), skip redundant specs DB writes."
#property description "v1.442: Startup O(N²)→O(N) via direct-append in LoadTrackingFromDB; hoist TimeCurrent() out of inner TF loop (7659→1 syscall/cycle)."
#property description "v1.441: SSD write reduction — bid/ask every 2.5min (was 60s), specs cached 1hr."
#property description "v1.439: Perf — sorted demand lookup, specs caching, pre-prepared metadata stmts."
#property strict


input int    UpdateIntervalSec = 30;     // Update interval (seconds)
input bool   MarketWatchOnly   = false;  // false = ALL broker symbols
input int    BatchSize         = 10;     // Symbols per SQLite transaction
input bool   ForceReExport     = false;  // true = clear tracking, re-export all history once
input bool   IntegrityCheck    = true;   // Verify bar counts on startup, re-export short keys
input int    InitialBarCap     = 1000;   // Max bars per key on startup integrity (1000 = fast restart)
input int    SpecsCacheMin     = 60;     // Minutes between full symbol spec refreshes (default 1h)
input bool   RamdiskMode       = true;   // DB on /dev/shm (deploy_ramdisk.sh) — use MEMORY journal + sync=OFF to eliminate journal file I/O under Wine. Set false when DB is on regular disk.

int g_db = INVALID_HANDLE;
string g_accountTag = "";
int g_stmtBarInsert = INVALID_HANDLE;       // Pre-prepared INSERT OR REPLACE for bar_cache
int g_stmtBarRead = INVALID_HANDLE;         // Pre-prepared SELECT data FROM bar_cache WHERE key=?1
int g_stmtTrackInsert = INVALID_HANDLE;     // Pre-prepared INSERT OR REPLACE for bar_track
int g_stmtQuoteInsert = INVALID_HANDLE;     // Pre-prepared INSERT OR REPLACE for bid_ask
int g_stmtMetaInsert  = INVALID_HANDLE;     // Pre-prepared INSERT OR REPLACE for metadata (specs/symbols/server)
int g_stmtBarTouchTs  = INVALID_HANDLE;     // Pre-prepared UPDATE bar_cache SET timestamp=?1 WHERE key=?2 — v1.471 heartbeat-only
string g_cachedSpecsCsv = "";                // Cached specs CSV (avoid rebuilding every 5min)
datetime g_specsLastBuild = 0;               // When specs CSV was last rebuilt
datetime g_specsLastDbWrite = 0;             // v1.443: When specs CSV was last persisted to DB (skip redundant writes)
// v1.469: Symbol list content dedup. __SYMBOLS__ is a ~7KB JSON array of
// broker symbol names; nothing in the terminal consumes it (cached_mt5_symbols
// is derived from bar_cache keys, not this row), so writing it every 30s just
// burns a BCW upsert + re-triggers Mt5Sync to merge the unchanged payload
// on every pass. Two-tier dedup:
//   - Fast skip: symCount unchanged AND <300 s since last successful write →
//     return immediately (no SymbolName scan, no JSON rebuild).
//   - Slow path (every 300 s, or on symCount change): rebuild the JSON and
//     compare to the cached copy. Only upsert when the bytes actually differ.
// 300 s bounds the "symbol swap at equal count" worst-case detection window
// — broker adds/removes typically bump symCount and trigger immediate rebuild.
string g_cachedSymbolsJson = "";
datetime g_symbolsLastDbWrite = 0;
int    g_symbolsLastCount = -1;
// v1.469: __SERVER__ row is written once in OnInit; its content (server +
// company) is fixed after MT5 login. Previously rewritten every cycle
// alongside __SYMBOLS__ with a fresh timestamp, which Mt5Sync saw as a
// change and re-merged unchanged bytes every pass.

// Demand symbols — loaded from demand.txt in OnInit, used in ExportAll.
// v3 format only: every line is SYMBOL:TF:LAST_TS_MS:MAX_BARS. MAX_BARS=0
// is passive demand (normal rotation export); MAX_BARS>0 is a gap-fill
// request — force-export that many recent bars overriding the incremental
// path. Gap entries are consumed once: on successful export the
// corresponding MAX_BARS is zeroed so the request is not re-served.
string g_demandSymbols[];     // flat symbol list (sorted for binary search)
int g_demandCount = 0;
string   g_gapFillKeys[];     // "SYMBOL:TF" e.g. "EURUSD:1Hour"
long     g_gapFillLastTs[];   // ms
int      g_gapFillMaxBars[];
int      g_gapFillCount = 0;
// v1.450: active-count tracking for early-bail. Mirrors how many entries
// in g_gapFillMaxBars[] are still > 0 (i.e. still have pending work).
// LoadDemandFile sets this to the number of v3 lines with MAX_BARS > 0;
// ClearGapFill decrements when it zeros an active slot. When this is 0,
// GetGapFillMaxBars can short-circuit without running a binary search —
// which is the steady-state case for the 900 calls per ExportAll cycle.
int      g_gapFillActive = 0;

// v1.443 perf: O(1) demand-symbol membership check. Parallel array indexed by
// the MT5 symbol index (SymbolName(i, MarketWatchOnly)). Rebuilt once at OnInit
// after demand.txt is loaded, and on symCount change if detected. Replaces
// O(log m) BinarySearchKey call inside the hot 851-symbol outer loop.
bool g_isDemandIdx[];
int  g_demandIdxSymCount = 0;  // symCount at time of last bitmap rebuild

// Track last bar time per symbol:TF to skip unchanged data
// Uses sorted arrays + binary search for O(log n) lookup instead of O(n) linear scan
// With 851 symbols × 9 TFs = 7,659 keys, this matters every tick
string g_trackKeys[];
datetime g_trackTimes[];
int g_trackFails[];  // Consecutive export failures per key — give up after MAX_CONSEC_FAILS
int g_trackCount = 0;
bool g_trackSorted = true; // false when new keys appended, triggers re-sort

// v1.443: Track-time dirty flags — lets us defer DB persistence of bar_track
// updates from the hot export loop to a bulk end-of-cycle flush. In-memory
// state is authoritative; DB is a recovery checkpoint written periodically.
// At worst a crash loses ≤TRACK_FLUSH_CYCLES * UpdateIntervalSec seconds of
// tracking state, after which the next ExportSymbolTF call re-detects and
// re-exports any delta. Net effect: ~10× fewer bar_track DB writes per cycle.
bool g_trackDirty[];
int  g_trackFlushCounter = 0;
#define TRACK_FLUSH_CYCLES 10   // Flush dirty track updates every N export cycles
#define MAX_CONSEC_FAILS 10  // Give up on a symbol/TF after this many consecutive failures
#define FAIL_SENTINEL  1     // Sentinel timestamp: marks "permanently failed, stop retrying"
#define MAX_BARS_PER_KEY 100000  // Hard cap: trim oldest bars during incremental merge if exceeded
int g_cycleCount = 0;           // Counts ExportAll() calls for periodic maintenance

// v1.465: Per-(sym,TF) last export time — replaces old per-TF-global
// g_tfLastExportTime[9]. The global timer was a serialisation bug: the
// first symbol exported in a TF updated g_tfLastExportTime[tf], gating
// every other symbol on that same TF for the next 80% of the TF period.
// On a 38-demand-symbol load, only ONE symbol per TF per gate window
// could export without gap-fill bypass, starving the rotation queue
// (symptom: terminal saw 42→44% cache coverage overnight and flatlined).
// Now indexed in lockstep with g_trackKeys/g_trackTimes/g_trackFails/
// g_trackDirty via trackIdx. Sorted alongside g_trackKeys in SortTrackArrays.
datetime g_trackLastExport[];

// v1.465: Flat-array O(1) caches indexed by (mwIdx * g_tfCount + tf).
// g_gapFillMaxBarsByMWIdx replaces the binary-search GetGapFillMaxBars
// call inside the ~900 symbol×TF inner-loop iterations per cycle.
// g_trackIdxByMWIdx is lazy-populated: -1 means "not yet looked up",
// positive means "resolved to this trackIdx". Populated on first hit
// via GetTrackIndex and cached for the rest of the session (stable
// because broker symbol ordering is stable within a session).
// Rebuilt by RebuildSlotCaches — runs alongside RebuildDemandIndexBitmap
// whenever demand.txt reloads or symCount changes.
int g_gapFillMaxBarsByMWIdx[];
int g_trackIdxByMWIdx[];
int g_slotCacheSymCount = 0;     // symCount at last RebuildSlotCaches call

// v1.447: Initial-burst mode — after /dev/shm clear or fresh install, rotation
// would leave most symbols unpopulated for ~4 minutes. When OnInit detects an
// empty or near-empty DB, we flip this flag to process demand symbols
// sequentially at max speed (bypassing rotation + TF gating) until the cache
// is warm. Exit when all demand symbols have at least one populated TF.
bool     g_initBurstActive     = false;
int      g_initBurstCycles     = 0;    // cycles spent in burst (log context)
int      g_initBurstExitThresh = 100;  // min bars per demand:TF to count "populated"

// v1.447: Heartbeat — written at end of every ExportAll cycle so the terminal
// can see writer liveness without parsing log files. Key is
// `mt5:__HEARTBEAT__:{accountId}` stored as JSON text in bar_cache.data.


// Safe transaction wrappers — handle dangling transactions from prior lock failures
bool SafeBegin()
{
   if(!DatabaseTransactionBegin(g_db))
   {
      // Likely already in a transaction from a prior failed commit — rollback and retry
      DatabaseExecute(g_db, "ROLLBACK");
      return DatabaseTransactionBegin(g_db);
   }
   return true;
}

bool SafeCommit()
{
   if(!DatabaseTransactionCommit(g_db))
   {
      // Commit failed (lock timeout) — rollback to clear transaction state
      DatabaseExecute(g_db, "ROLLBACK");
      return false;
   }
   return true;
}

// Forex filtering: only export forex symbols on the CFD server (detected by USDMXN availability).
// Crypto and Futures servers don't need EURGBP/EURUSD/GBPUSD cluttering the cache.
bool g_isCFDServer = false;

// Pre-sorted currency codes — O(log n) binary search instead of O(n) linear scan per call.
// Sorted alphabetically: 19 codes → max 5 comparisons per lookup.
string g_forexCurrencies[] = {"AUD","CAD","CHF","CZK","DKK","EUR","GBP","HKD","HUF","JPY","MXN","NOK","NZD","PLN","SEK","SGD","TRY","USD","ZAR"};
#define FOREX_CCY_COUNT 19

bool IsCurrencyCode(string code)
{
   int lo = 0, hi = FOREX_CCY_COUNT - 1;
   while(lo <= hi)
   {
      int mid = (lo + hi) / 2;
      int cmp = StringCompare(g_forexCurrencies[mid], code);
      if(cmp == 0) return true;
      if(cmp < 0) lo = mid + 1;
      else hi = mid - 1;
   }
   return false;
}

bool IsForexSymbol(string symbol)
{
   if(StringLen(symbol) != 6) return false;
   return IsCurrencyCode(StringSubstr(symbol, 0, 3))
       && IsCurrencyCode(StringSubstr(symbol, 3, 3));
}

// Timeframes: MN1 first (higher TFs are smaller, export fast)
ENUM_TIMEFRAMES g_timeframes[] = {
   PERIOD_MN1, PERIOD_W1, PERIOD_D1, PERIOD_H4, PERIOD_H1,
   PERIOD_M30, PERIOD_M15, PERIOD_M5, PERIOD_M1
};
// Pre-cached TF strings — avoids 7,659 switch evaluations per tick
string g_tfStrings[9];
// Pre-cached TF periods (seconds) — avoids 7,659 PeriodSeconds() calls per tick
int g_tfPeriods[9];
// Cached ArraySize(g_timeframes) — avoids function call in the hot TF loop
int g_tfCount = 9;

// Max bars per timeframe. v1.465: tiered targets — low-TF gets deeper
// history for algo backtesting, high-TF stays shallow (low-TF is where
// the history matters). Must stay in lockstep with mt5_tf_spec in
// native/src/app.rs so gap-detect's coverage target and BCW's fetch
// ceiling agree.
//   1Min   50K ≈ 35 days     — intraday backtests (scalping, short-term)
//   5Min   50K ≈ 6 months    — swing backtests
//   15Min  30K ≈ 10 months
//   30Min  30K ≈ 20 months
//   1Hour  30K ≈ 3.4 years   — positional backtests
//   4Hour  20K ≈ 9 years     — long-horizon models
//   1Day+  10K               — broker-served history ceiling
int MaxBarsForTF(ENUM_TIMEFRAMES tf)
{
   switch(tf)
   {
      case PERIOD_M1:  return 50000;
      case PERIOD_M5:  return 50000;
      case PERIOD_M15: return 30000;
      case PERIOD_M30: return 30000;
      case PERIOD_H1:  return 30000;
      case PERIOD_H4:  return 20000;
      case PERIOD_D1:  return 10000;
      case PERIOD_W1:  return 10000;
      case PERIOD_MN1: return 10000;
   }
   return 10000;
}

string TFToStr(ENUM_TIMEFRAMES tf)
{
   switch(tf)
   {
      case PERIOD_M1:  return "1Min";
      case PERIOD_M5:  return "5Min";
      case PERIOD_M15: return "15Min";
      case PERIOD_M30: return "30Min";
      case PERIOD_H1:  return "1Hour";
      case PERIOD_H4:  return "4Hour";
      case PERIOD_D1:  return "1Day";
      case PERIOD_W1:  return "1Week";
      case PERIOD_MN1: return "1Month";
   }
   return "?";
}

ENUM_TIMEFRAMES StrToTF(string s)
{
   if(s == "1Min")   return PERIOD_M1;
   if(s == "5Min")   return PERIOD_M5;
   if(s == "15Min")  return PERIOD_M15;
   if(s == "30Min")  return PERIOD_M30;
   if(s == "1Hour")  return PERIOD_H1;
   if(s == "4Hour")  return PERIOD_H4;
   if(s == "1Day")   return PERIOD_D1;
   if(s == "1Week")  return PERIOD_W1;
   if(s == "1Month") return PERIOD_MN1;
   return (ENUM_TIMEFRAMES)0;
}

// Binary search for key in sorted g_trackKeys. Returns index or -1.
int BinarySearchKey(const string &keys[], int count, string key)
{
   int lo = 0, hi = count - 1;
   while(lo <= hi)
   {
      int mid = (lo + hi) / 2;
      int cmp = StringCompare(keys[mid], key);
      if(cmp == 0) return mid;
      if(cmp < 0) lo = mid + 1;
      else hi = mid - 1;
   }
   return -1;
}

// v1.443 perf: Rebuild the O(1) demand bitmap for the current symbol count.
// Called after demand.txt load in OnInit and lazily on symCount change in ExportAll.
// Cost: O(N log M) once, then O(1) per-symbol membership check in hot loops
// (replacing the O(log M) BinarySearchKey call that ran twice per symbol per cycle).
void RebuildDemandIndexBitmap(int symCount)
{
   if(symCount <= 0) return;
   ArrayResize(g_isDemandIdx, symCount);
   ArrayInitialize(g_isDemandIdx, false);
   if(g_demandCount <= 0) { g_demandIdxSymCount = symCount; return; }
   for(int i = 0; i < symCount; i++)
   {
      string s = SymbolName(i, MarketWatchOnly);
      if(StringLen(s) == 0) continue;
      if(BinarySearchKey(g_demandSymbols, g_demandCount, s) >= 0)
         g_isDemandIdx[i] = true;
   }
   g_demandIdxSymCount = symCount;
}

// Sort tracking arrays by key (insertion sort — only runs when new keys added).
// v1.465: sorts g_trackDirty and g_trackLastExport in lockstep — previously
// g_trackDirty was untouched, which was latent corruption (a dirty flag for
// key A would end up attached to key B after sort). g_trackIdxByMWIdx cache
// is also invalidated on sort since previously-cached trackIdx values no
// longer point at the same key.
void SortTrackArrays()
{
   for(int i = 1; i < g_trackCount; i++)
   {
      string   tmpKey     = g_trackKeys[i];
      datetime tmpTime    = g_trackTimes[i];
      int      tmpFails   = g_trackFails[i];
      bool     tmpDirty   = g_trackDirty[i];
      datetime tmpLastExp = g_trackLastExport[i];
      int j = i - 1;
      while(j >= 0 && StringCompare(g_trackKeys[j], tmpKey) > 0)
      {
         g_trackKeys[j + 1]        = g_trackKeys[j];
         g_trackTimes[j + 1]       = g_trackTimes[j];
         g_trackFails[j + 1]       = g_trackFails[j];
         g_trackDirty[j + 1]       = g_trackDirty[j];
         g_trackLastExport[j + 1]  = g_trackLastExport[j];
         j--;
      }
      g_trackKeys[j + 1]       = tmpKey;
      g_trackTimes[j + 1]      = tmpTime;
      g_trackFails[j + 1]      = tmpFails;
      g_trackDirty[j + 1]      = tmpDirty;
      g_trackLastExport[j + 1] = tmpLastExp;
   }
   g_trackSorted = true;

   // Cached trackIdx values are stale after a re-sort. Reset the slot
   // cache to -1 so the next hot-loop lookup re-resolves via GetTrackIndex.
   // Cheap: g_trackIdxByMWIdx is symCount*tfCount ints (~7.6K for 851 syms).
   int slots = ArraySize(g_trackIdxByMWIdx);
   for(int k = 0; k < slots; k++) g_trackIdxByMWIdx[k] = -1;
}

// ── TTBR Binary Bar Format ───────────────────────────────────────────────
// Identical to TyphooN-Terminal's internal format — zero conversion on read.
// [4 bytes: "TTBR"] [4 bytes: bar count LE u32] [48 bytes/bar: i64 ts_ms, f64 O,H,L,C,V]
// 48 bytes/bar vs ~60 chars/bar CSV. No StringFormat, no string concat, no GC pressure.
union ByteConv { long l; double d; uchar b[8]; };

void WriteRaw8(uchar &buf[], int off, ByteConv &bc)
{
   buf[off]   = bc.b[0]; buf[off+1] = bc.b[1]; buf[off+2] = bc.b[2]; buf[off+3] = bc.b[3];
   buf[off+4] = bc.b[4]; buf[off+5] = bc.b[5]; buf[off+6] = bc.b[6]; buf[off+7] = bc.b[7];
}

void PackBarsBinary(const MqlRates &rates[], int startIdx, int count, uchar &buffer[])
{
   int totalBytes = 8 + count * 48;
   ArrayResize(buffer, totalBytes);

   // Magic: TTBR
   buffer[0] = 'T'; buffer[1] = 'T'; buffer[2] = 'B'; buffer[3] = 'R';

   // Count (LE u32)
   buffer[4] = (uchar)(count & 0xFF);
   buffer[5] = (uchar)((count >> 8) & 0xFF);
   buffer[6] = (uchar)((count >> 16) & 0xFF);
   buffer[7] = (uchar)((count >> 24) & 0xFF);

   ByteConv bc;
   for(int i = 0; i < count; i++)
   {
      int off = 8 + i * 48;
      int ri = startIdx + i;

      bc.l = (long)rates[ri].time * 1000;  // timestamp as epoch milliseconds
      WriteRaw8(buffer, off, bc);

      bc.d = rates[ri].open;
      WriteRaw8(buffer, off + 8, bc);

      bc.d = rates[ri].high;
      WriteRaw8(buffer, off + 16, bc);

      bc.d = rates[ri].low;
      WriteRaw8(buffer, off + 24, bc);

      bc.d = rates[ri].close;
      WriteRaw8(buffer, off + 32, bc);

      bc.d = (double)rates[ri].tick_volume;
      WriteRaw8(buffer, off + 40, bc);
   }
}

int GetTrackIndex(string key)
{
   // Binary search if sorted
   if(g_trackSorted && g_trackCount > 0)
   {
      int idx = BinarySearchKey(g_trackKeys, g_trackCount, key);
      if(idx >= 0) return idx;
   }
   else if(!g_trackSorted)
   {
      // Linear fallback during population phase (before first sort)
      for(int i = 0; i < g_trackCount; i++)
         if(g_trackKeys[i] == key) return i;
   }

   // Not found — append and mark unsorted
   g_trackCount++;
   ArrayResize(g_trackKeys, g_trackCount, 1024); // reserve 1024 extra to reduce reallocs
   ArrayResize(g_trackTimes, g_trackCount, 1024);
   ArrayResize(g_trackFails, g_trackCount, 1024);
   ArrayResize(g_trackDirty, g_trackCount, 1024);
   ArrayResize(g_trackLastExport, g_trackCount, 1024);
   g_trackKeys[g_trackCount - 1] = key;
   g_trackTimes[g_trackCount - 1] = 0;
   g_trackFails[g_trackCount - 1] = 0;
   g_trackDirty[g_trackCount - 1] = false;
   g_trackLastExport[g_trackCount - 1] = 0;
   g_trackSorted = false;
   return g_trackCount - 1;
}

// Persist last bar time to DB so we survive restarts.
// v1.443: Not called from the hot export loop any more — callers mark
// g_trackDirty[idx] = true instead and FlushDirtyTrackTimes() persists them
// in bulk at cycle end (every TRACK_FLUSH_CYCLES cycles). Retained for the
// FAIL_SENTINEL path and OnDeinit where immediate persistence matters.
void SaveTrackTime(string trackKey, datetime barTime)
{
   if(g_stmtTrackInsert == INVALID_HANDLE) return;
   DatabaseReset(g_stmtTrackInsert);
   DatabaseBind(g_stmtTrackInsert, 0, trackKey);
   DatabaseBind(g_stmtTrackInsert, 1, (long)barTime);
   DatabaseRead(g_stmtTrackInsert);
}

// v1.443: Flush all dirty bar_track entries to DB in one transaction.
// Called periodically from ExportAll (every TRACK_FLUSH_CYCLES cycles) and
// on OnDeinit. Returns count of rows flushed.
int FlushDirtyTrackTimes()
{
   if(g_stmtTrackInsert == INVALID_HANDLE || g_trackCount == 0) return 0;
   int dirtyN = 0;
   for(int i = 0; i < g_trackCount; i++)
      if(g_trackDirty[i]) dirtyN++;
   if(dirtyN == 0) return 0;

   if(!SafeBegin()) return 0;
   int flushed = 0;
   for(int i = 0; i < g_trackCount; i++)
   {
      if(!g_trackDirty[i]) continue;
      DatabaseReset(g_stmtTrackInsert);
      DatabaseBind(g_stmtTrackInsert, 0, g_trackKeys[i]);
      DatabaseBind(g_stmtTrackInsert, 1, (long)g_trackTimes[i]);
      if(DatabaseRead(g_stmtTrackInsert))
      {
         g_trackDirty[i] = false;
         flushed++;
      }
   }
   SafeCommit();
   return flushed;
}

// Load existing key→timestamp mapping from DB so we don't re-export after restart.
// Keys from the DB are guaranteed unique (bar_track primary key), so we bypass
// GetTrackIndex entirely and direct-append. Previously each row called GetTrackIndex
// which, with g_trackSorted=false during population, did a linear scan through the
// already-loaded entries — giving O(N²) total startup cost at 7,659 keys
// (≈58M comparisons). Direct-append is O(N) and sorts once at the end.
void LoadTrackingFromDB()
{
   if(g_db == INVALID_HANDLE) return;

   // Count first so we can ArrayResize once instead of growing incrementally.
   int total = 0;
   int cntReq = DatabasePrepare(g_db, "SELECT COUNT(*) FROM bar_track");
   if(cntReq != INVALID_HANDLE)
   {
      if(DatabaseRead(cntReq))
      {
         long n = 0;
         DatabaseColumnLong(cntReq, 0, n);
         total = (int)n;
      }
      DatabaseFinalize(cntReq);
   }

   int req = DatabasePrepare(g_db,
      "SELECT key, last_bar_time FROM bar_track");
   if(req == INVALID_HANDLE) return;

   // Single up-front allocation with a small safety margin for new keys added later.
   if(total > 0)
   {
      int resv = total + 256;
      ArrayResize(g_trackKeys,       resv);
      ArrayResize(g_trackTimes,      resv);
      ArrayResize(g_trackFails,      resv);
      ArrayResize(g_trackDirty,      resv);
      ArrayResize(g_trackLastExport, resv);
   }

   int loaded = 0;
   while(DatabaseRead(req))
   {
      string key = "";
      long ts = 0;
      DatabaseColumnText(req, 0, key);
      DatabaseColumnLong(req, 1, ts);

      if(ts <= 0) continue;

      // Direct append — no GetTrackIndex lookup. Keys are unique by DB constraint.
      int idx = g_trackCount;
      g_trackCount++;
      // Defensive: if COUNT(*) was stale (shouldn't happen under the prepared tx,
      // but be safe) — grow in chunks to preserve amortized O(1).
      if(idx >= ArraySize(g_trackKeys))
      {
         int grow = g_trackCount + 1024;
         ArrayResize(g_trackKeys,       grow);
         ArrayResize(g_trackTimes,      grow);
         ArrayResize(g_trackFails,      grow);
         ArrayResize(g_trackDirty,      grow);
         ArrayResize(g_trackLastExport, grow);
      }
      g_trackKeys[idx]       = key;
      g_trackTimes[idx]      = (datetime)ts;
      g_trackFails[idx]      = 0;
      g_trackDirty[idx]      = false;
      g_trackLastExport[idx] = 0;
      loaded++;
   }
   DatabaseFinalize(req);

   // Mark unsorted and sort once for O(log n) lookups on subsequent ticks.
   if(loaded > 0)
   {
      g_trackSorted = false;
      SortTrackArrays();
      PrintFormat("BarCacheWriter: restored %d cached keys from DB (skip re-export on restart)", loaded);
   }
}

// v1.449: Binary-search gap-fill lookup. Previously linear — with a
// self-heal cap of 256 entries and ~900 symbol×TF exports per cycle, the
// linear scan burned ~230K string comparisons per cycle on Wine. Binary
// search drops that to log2(256)=8 comparisons per lookup, ~32× fewer
// string ops in the hot path. LoadDemandFile sorts the arrays after
// population so the search is valid from OnInit onward.
int BinarySearchGap(string symTf)
{
   int lo = 0, hi = g_gapFillCount - 1;
   while(lo <= hi)
   {
      int mid = (lo + hi) / 2;
      int cmp = StringCompare(g_gapFillKeys[mid], symTf);
      if(cmp == 0) return mid;
      if(cmp < 0) lo = mid + 1;
      else hi = mid - 1;
   }
   return -1;
}

// v1.462: Read the cached bar count for a key without decoding the whole
// blob. Reads the 8-byte TTBR header (4-byte magic + 4-byte LE count) via
// the pre-prepared read statement. Returns 0 if the key is absent, the
// blob header is malformed, or anything else goes wrong — callers treat
// 0 as "empty/unknown cache" and fall through to the full-export path.
// Hot path: called once per gap-fill hit inside ExportAll to decide
// whether to merge or re-seed.
int GetCachedBarCount(string trackKey)
{
   if(g_stmtBarRead == INVALID_HANDLE) return 0;
   string cacheKey = "mt5:" + trackKey;
   DatabaseReset(g_stmtBarRead);
   DatabaseBind(g_stmtBarRead, 0, cacheKey);
   if(!DatabaseRead(g_stmtBarRead)) return 0;
   uchar hdr[];
   DatabaseColumnBlob(g_stmtBarRead, 0, hdr);
   // v1.470: close the cursor before returning. DatabaseRead on a SELECT
   // leaves the statement in SQLITE_ROW state; the outer batch COMMIT then
   // fails with "cannot commit transaction - SQL statements in progress"
   // because a prepared statement is still stepping.
   int count = 0;
   if(ArraySize(hdr) >= 8 && hdr[0] == 'T' && hdr[1] == 'T' && hdr[2] == 'B' && hdr[3] == 'R')
      count = (int)hdr[4] | ((int)hdr[5] << 8) | ((int)hdr[6] << 16) | ((int)hdr[7] << 24);
   DatabaseReset(g_stmtBarRead);
   return count;
}

// Mark a gap-fill request as served so it isn't re-processed next cycle.
// Called after a successful ExportSymbolTF on a gap-fill key. v1.450:
// decrement the active counter on the 1→0 transition so the next lookup
// can short-circuit.
void ClearGapFill(string symTf)
{
   int idx = BinarySearchGap(symTf);
   if(idx >= 0 && g_gapFillMaxBars[idx] > 0)
   {
      g_gapFillMaxBars[idx] = 0;
      if(g_gapFillActive > 0) g_gapFillActive--;
   }
}

// v1.465: O(1) variant — clears the slot cache alongside the sorted array.
// Called from the hot ExportAll loop where we already have (mwIdx, tf) in
// hand. Avoids re-building "SYMBOL:TF" strings + binary-searching for
// every successful gap-fill. The sorted-array clear still happens so
// subsequent BinarySearchGap calls (e.g. startup integrity) see a clean
// state; g_gapFillActive decrement flows from the sorted clear.
void ClearGapFillSlot(int mwIdx, int tf, string symTf)
{
   int flat = mwIdx * g_tfCount + tf;
   if(flat >= 0 && flat < ArraySize(g_gapFillMaxBarsByMWIdx))
      g_gapFillMaxBarsByMWIdx[flat] = 0;
   ClearGapFill(symTf);
}

// v1.465: Rebuild the O(1) slot caches — paired with RebuildDemandIndexBitmap.
// g_gapFillMaxBarsByMWIdx: flat symCount*tfCount int array. Each slot
//   carries the pending gap-fill bar count for that (mwIdx, tf), 0 when
//   no request is outstanding. Populated by walking g_gapFillKeys once
//   and projecting each entry onto the two matching indices (symbol
//   index in the MW table, TF index in g_timeframes).
// g_trackIdxByMWIdx: flat symCount*tfCount int array, all -1 at rebuild.
//   Lazily populated on first hot-loop hit via GetTrackIndex; cached
//   for the rest of the session. Session-stable because MT5 symbol
//   ordering (SymbolName index) doesn't churn mid-session.
// Cost: O(symCount * tfCount + gapCount) — runs once at OnInit and again
// whenever demand.txt reloads (every 2 cycles) or symCount changes.
void RebuildSlotCaches(int symCount)
{
   if(symCount <= 0) return;
   int slots = symCount * g_tfCount;
   ArrayResize(g_gapFillMaxBarsByMWIdx, slots);
   ArrayResize(g_trackIdxByMWIdx, slots);
   ArrayInitialize(g_gapFillMaxBarsByMWIdx, 0);
   for(int k = 0; k < slots; k++) g_trackIdxByMWIdx[k] = -1;
   g_slotCacheSymCount = symCount;

   if(g_gapFillCount <= 0 || g_demandCount <= 0) return;

   // Build a symbol→mwIdx map by scanning MW symbols once. For each MW
   // symbol that is in the demand list (O(log m) binary search), check
   // if any gap-fill key starts with that symbol and match the TF half.
   for(int i = 0; i < symCount; i++)
   {
      if(i < g_demandIdxSymCount && !g_isDemandIdx[i]) continue;
      string sym = SymbolName(i, MarketWatchOnly);
      if(StringLen(sym) == 0) continue;

      // Probe each TF — gap-fill count is ≤ 1024 (SELF_HEAL_CAP) so a
      // per-(sym,TF) binary search over g_gapFillKeys is cheap.
      for(int tf = 0; tf < g_tfCount; tf++)
      {
         string symTf = sym + ":" + g_tfStrings[tf];
         int gi = BinarySearchGap(symTf);
         if(gi >= 0 && g_gapFillMaxBars[gi] > 0)
            g_gapFillMaxBarsByMWIdx[i * g_tfCount + tf] = g_gapFillMaxBars[gi];
      }
   }
}

// Shared demand.txt loader — v3 format only: every line is
// SYMBOL:TF:LAST_TS_MS:MAX_BARS. MAX_BARS=0 is passive demand;
// MAX_BARS>0 is a gap-fill request. Called from OnInit once and from
// ExportAll whenever the demand file's mtime changes. Rebuilds
// g_demandSymbols + g_gapFill* globals from scratch.
void LoadDemandFile(int symCount)
{
   g_demandCount = 0;
   g_gapFillCount = 0;
   g_gapFillActive = 0;
   ArrayResize(g_demandSymbols, 100);
   ArrayResize(g_gapFillKeys, 16);
   ArrayResize(g_gapFillLastTs, 16);
   ArrayResize(g_gapFillMaxBars, 16);

   string path = g_accountTag + "_demand.txt";
   int h = FileOpen(path, FILE_READ | FILE_ANSI | FILE_COMMON);
   if(h == INVALID_HANDLE)
   {
      path = "demand.txt";
      h = FileOpen(path, FILE_READ | FILE_ANSI | FILE_COMMON);
   }
   if(h == INVALID_HANDLE)
      return;

   while(!FileIsEnding(h))
   {
      string line = FileReadString(h);
      StringTrimRight(line);
      StringTrimLeft(line);
      if(StringLen(line) == 0 || StringGetCharacter(line, 0) == '#') continue;

      string parts[];
      int nParts = StringSplit(line, ':', parts);
      if(nParts != 4 || StringLen(parts[0]) == 0 || StringLen(parts[1]) == 0) continue;

      // v1.455: reject rows where the symbol slot is itself a known TF
      // string (e.g. the terminal's old bug wrote "1Hour:1Hour:0:1500"
      // when a Screener/watchlist chart had a full cache key in
      // chart.symbol). Also require the TF slot to parse — a garbage TF
      // would produce PERIOD_CURRENT (0) and silently export the wrong
      // series, so catch it here instead.
      if(StrToTF(parts[0]) != (ENUM_TIMEFRAMES)0) continue;
      if(StrToTF(parts[1]) == (ENUM_TIMEFRAMES)0) continue;

      int maxBars = (int)StringToInteger(parts[3]);
      if(maxBars > 0)
      {
         if(g_gapFillCount >= ArraySize(g_gapFillKeys))
         {
            int ng = g_gapFillCount * 2 + 1;
            ArrayResize(g_gapFillKeys, ng);
            ArrayResize(g_gapFillLastTs, ng);
            ArrayResize(g_gapFillMaxBars, ng);
         }
         g_gapFillKeys[g_gapFillCount]    = parts[0] + ":" + parts[1];
         g_gapFillLastTs[g_gapFillCount]  = StringToInteger(parts[2]);
         g_gapFillMaxBars[g_gapFillCount] = maxBars;
         g_gapFillCount++;
         g_gapFillActive++;
      }
      if(g_demandCount >= ArraySize(g_demandSymbols))
         ArrayResize(g_demandSymbols, g_demandCount * 2 + 1);
      g_demandSymbols[g_demandCount] = parts[0];
      g_demandCount++;
   }
   FileClose(h);

   // Sort + dedup symbols for O(log n) lookup.
   if(g_demandCount > 1)
   {
      for(int si = 1; si < g_demandCount; si++)
      {
         string tmp = g_demandSymbols[si];
         int sj = si - 1;
         while(sj >= 0 && StringCompare(g_demandSymbols[sj], tmp) > 0)
         {
            g_demandSymbols[sj + 1] = g_demandSymbols[sj];
            sj--;
         }
         g_demandSymbols[sj + 1] = tmp;
      }
      int unique = 1;
      for(int si = 1; si < g_demandCount; si++)
      {
         if(g_demandSymbols[si] != g_demandSymbols[unique - 1])
            g_demandSymbols[unique++] = g_demandSymbols[si];
      }
      g_demandCount = unique;
   }

   // v1.449: Sort gap-fill arrays so GetGapFillMaxBars / ClearGapFill can
   // use binary search instead of a 256-entry linear scan. Insertion sort
   // is fine here — n is small (≤ 256) and this only runs when demand.txt
   // mtime changes (every minute at most).
   for(int gi = 1; gi < g_gapFillCount; gi++)
   {
      string tmpKey = g_gapFillKeys[gi];
      long tmpTs = g_gapFillLastTs[gi];
      int tmpMax = g_gapFillMaxBars[gi];
      int gj = gi - 1;
      while(gj >= 0 && StringCompare(g_gapFillKeys[gj], tmpKey) > 0)
      {
         g_gapFillKeys[gj + 1]    = g_gapFillKeys[gj];
         g_gapFillLastTs[gj + 1]  = g_gapFillLastTs[gj];
         g_gapFillMaxBars[gj + 1] = g_gapFillMaxBars[gj];
         gj--;
      }
      g_gapFillKeys[gj + 1]    = tmpKey;
      g_gapFillLastTs[gj + 1]  = tmpTs;
      g_gapFillMaxBars[gj + 1] = tmpMax;
   }

   RebuildDemandIndexBitmap(symCount);
   RebuildSlotCaches(symCount);

   PrintFormat("BarCacheWriter: demand.txt loaded from %s — %d symbols, %d gap-fills",
      path, g_demandCount, g_gapFillCount);
}

// v1.447: Decide whether to enter initial-burst mode.
// Returns true if the cache appears empty/near-empty for this account —
// the rotation logic would otherwise leave most demand symbols unpopulated
// for minutes after /dev/shm clear. Burst mode bypasses rotation and
// TF gating so demand symbols get filled sequentially at max speed.
//
// v1.467: Use bar_track (in-EA tracking state) instead of bar_cache to
// detect cold-start. The terminal's Mt5Sync now deletes bar_cache rows
// immediately after merging them into its target DB — under the old
// bar_cache-based check, every EA re-attach on a well-synced account
// would false-positive into burst mode and rewrite everything. bar_track
// rows persist across post-sync deletes (terminal only touches bar_cache),
// so counting bar_track.last_bar_time > 0 cleanly distinguishes "EA has
// never exported here" (cold start) from "terminal has already merged +
// cleaned" (warm state with empty bar_cache). FAIL_SENTINEL is excluded
// so symbols the EA gave up on don't count as populated.
bool ShouldEnterInitialBurst()
{
   if(g_db == INVALID_HANDLE) return false;

   int req = DatabasePrepare(g_db,
      "SELECT COUNT(*) FROM bar_track WHERE last_bar_time > 0 AND last_bar_time != ?1");
   if(req == INVALID_HANDLE) return false;
   DatabaseBind(req, 0, (long)FAIL_SENTINEL);
   long populated = 0;
   if(DatabaseRead(req))
      DatabaseColumnLong(req, 0, populated);
   DatabaseFinalize(req);

   if(g_demandCount > 0)
   {
      int expected = g_demandCount * g_tfCount;
      int floor    = (int)(expected * 0.20);
      if(populated < floor)
      {
         PrintFormat("BarCacheWriter: cold start — %I64d/%d tracked keys < 20%% floor (%d); entering initial-burst mode",
            populated, expected, floor);
         return true;
      }
   }
   else
   {
      // No demand list — use absolute floor. 9 TFs × ~50 symbols = 450 keys
      // is a reasonable warm threshold for generic installs.
      if(populated < 450)
      {
         PrintFormat("BarCacheWriter: cold start — %I64d tracked keys < 450; entering initial-burst mode",
            populated);
         return true;
      }
   }
   return false;
}

// v1.447: Exit initial-burst when every demand symbol has at least one TF
// populated with >= g_initBurstExitThresh bars. Called cheaply (once/cycle)
// from ExportAll. Uses in-memory g_trackTimes — no DB query.
bool CanExitInitialBurst()
{
   if(g_demandCount == 0) return true;  // nothing to track → always ok

   // For each demand symbol, confirm at least one of its TFs has progressed
   // past the "never synced" state (g_trackTimes > 0 and != FAIL_SENTINEL).
   // v1.451: Binary search via BinarySearchKey instead of a linear scan of
   // g_trackKeys[]. With ~50 demand symbols × 9 TFs × 7000 tracked keys, the
   // old nested linear scan burned 3.15M string comparisons per burst-cycle
   // call; binary search cuts that to ~5800. Falls back to linear while
   // g_trackSorted=false (only true briefly during OnInit population).
   for(int s = 0; s < g_demandCount; s++)
   {
      bool anyReady = false;
      for(int t = 0; t < g_tfCount; t++)
      {
         string tk = g_demandSymbols[s] + ":" + g_tfStrings[t];
         int idx = -1;
         if(g_trackSorted && g_trackCount > 0)
            idx = BinarySearchKey(g_trackKeys, g_trackCount, tk);
         else
         {
            for(int i = 0; i < g_trackCount; i++)
               if(g_trackKeys[i] == tk) { idx = i; break; }
         }
         if(idx >= 0
            && g_trackTimes[idx] > 0
            && g_trackTimes[idx] != (datetime)FAIL_SENTINEL)
         {
            anyReady = true;
            break;
         }
      }
      if(!anyReady) return false;  // this symbol has no populated TF yet
   }
   return true;
}

// v1.447: Write a heartbeat row so the terminal can detect liveness.
// JSON payload keys: ts, rotation_offset, sym_count, cycle_ms,
// init_burst_active, init_burst_cycles, cycle_count, exported, skipped,
// track_count, demand_count, version.
void WriteHeartbeat(int rotationOffset, int symCount, uint cycleMs, int exportedCount, int skippedCount)
{
   if(g_stmtMetaInsert == INVALID_HANDLE) return;
   datetime now = TimeCurrent();
   string key = "mt5:__HEARTBEAT__:" + g_accountTag;
   string json = StringFormat(
      "{\"ts\":%I64d,\"rotation_offset\":%d,\"sym_count\":%d,\"cycle_ms\":%u,"
      "\"init_burst_active\":%s,\"init_burst_cycles\":%d,\"cycle_count\":%d,"
      "\"exported\":%d,\"skipped\":%d,\"track_count\":%d,\"demand_count\":%d,"
      "\"version\":\"1.465\"}",
      (long)now, rotationOffset, symCount, cycleMs,
      g_initBurstActive ? "true" : "false",
      g_initBurstCycles, g_cycleCount,
      exportedCount, skippedCount, g_trackCount, g_demandCount);

   DatabaseReset(g_stmtMetaInsert);
   DatabaseBind(g_stmtMetaInsert, 0, key);
   DatabaseBind(g_stmtMetaInsert, 1, json);
   DatabaseBind(g_stmtMetaInsert, 2, (long)now);
   DatabaseBind(g_stmtMetaInsert, 3, 0);
   DatabaseRead(g_stmtMetaInsert);
}

int OnInit()
{
   // Opens typhoon_mt5_cache.db in MQL5/Files/ sandbox.
   // For ramdisk: use deploy_ramdisk.sh to symlink MQL5/Files/typhoon_mt5_cache.db → /dev/shm/
   // This is transparent to MQL5 — zero code changes needed.
   g_db = DatabaseOpen("typhoon_mt5_cache.db", DATABASE_OPEN_READWRITE | DATABASE_OPEN_CREATE);
   if(g_db == INVALID_HANDLE)
   {
      PrintFormat("BarCacheWriter: DB open failed (error %d)", GetLastError());
      return INIT_FAILED;
   }

   // Bar data table: stores bars as TTBR binary blobs (48 bytes/bar, zero string overhead)
   // __SYMBOLS__ and __SPECS__ keys store TEXT; bar keys store binary BLOB
   // SQLite dynamic typing handles both — BLOB declaration is self-documenting
   if(!DatabaseExecute(g_db,
      "CREATE TABLE IF NOT EXISTS bar_cache ("
      "  key TEXT PRIMARY KEY,"
      "  data BLOB NOT NULL,"
      "  timestamp INTEGER NOT NULL,"
      "  bar_count INTEGER NOT NULL DEFAULT 0"
      ")"))
   {
      PrintFormat("BarCacheWriter: table create failed (error %d)", GetLastError());
      DatabaseClose(g_db); g_db = INVALID_HANDLE;
      return INIT_FAILED;
   }

   // Track last bar time per key — survives restarts so we skip unchanged data
   if(!DatabaseExecute(g_db,
      "CREATE TABLE IF NOT EXISTS bar_track ("
      "  key TEXT PRIMARY KEY,"
      "  last_bar_time INTEGER NOT NULL DEFAULT 0"
      ")"))
   {
      PrintFormat("BarCacheWriter: bar_track create failed (error %d)", GetLastError());
   }

   // Live bid/ask table — flat, always 1 row per symbol (INSERT OR REPLACE)
   if(!DatabaseExecute(g_db,
      "CREATE TABLE IF NOT EXISTS bid_ask ("
      "  symbol TEXT PRIMARY KEY,"
      "  bid REAL NOT NULL DEFAULT 0,"
      "  ask REAL NOT NULL DEFAULT 0,"
      "  spread REAL NOT NULL DEFAULT 0,"
      "  timestamp INTEGER NOT NULL DEFAULT 0"
      ")"))
   {
      PrintFormat("BarCacheWriter: bid_ask create failed (error %d)", GetLastError());
   }

   // Covering index: lets readers get key/timestamp/bar_count from index alone,
   // without scanning through multi-MB blob rows. Drops metadata queries from ~12s to <100ms.
   DatabaseExecute(g_db, "CREATE INDEX IF NOT EXISTS idx_bar_meta ON bar_cache(key, timestamp, bar_count)");

   // v1.464: Pick journal mode based on DB backing store.
   //   RamdiskMode=true (deploy_ramdisk.sh → DB on /dev/shm):
   //     - journal_mode=MEMORY: rollback journal kept in RAM, no journal file
   //       created/truncated on every BEGIN/COMMIT. Under Wine, DELETE mode's
   //       journal churn is the dominant SQLite-under-Wine cost — every
   //       transaction pays NtCreateFile + NtWriteFile + NtClose + NtDeleteFile
   //       through Wine's file path. MEMORY eliminates all of it.
   //     - synchronous=OFF: skip fsync on commit. Safe on tmpfs (reboot wipes
   //       the DB anyway, so fsync durability is meaningless). IntegrityCheck
   //       at next startup catches any blob corrupted by mid-COMMIT crash.
   //   RamdiskMode=false (DB on regular disk):
   //     - journal_mode=DELETE: rollback journal persisted for crash safety.
   //       WAL unusable because its shared-memory mmap doesn't cross the
   //       Wine/Linux boundary (the terminal-side Mt5Sync reader can't map
   //       the same -shm/-wal files).
   //     - synchronous=NORMAL: fsync on critical moments only.
   // Either mode is safe vs Mt5Sync reads — readers see the last committed
   // page image through SQLite's normal visibility rules regardless of
   // journal location.
   if(RamdiskMode)
   {
      DatabaseExecute(g_db, "PRAGMA journal_mode=MEMORY");
      DatabaseExecute(g_db, "PRAGMA synchronous=OFF");
   }
   else
   {
      DatabaseExecute(g_db, "PRAGMA journal_mode=DELETE");
      DatabaseExecute(g_db, "PRAGMA synchronous=NORMAL");
   }
   // NORMAL locking: acquire/release per transaction. EXCLUSIVE would block
   // Mt5Sync readers permanently. The batch transaction already amortizes
   // lock overhead (10 symbols per BEGIN/COMMIT = ~85 transactions per cycle
   // instead of 7,659 individual locks).
   // 16MB page cache: keeps hot pages (recently written blobs) in memory.
   // Reduces Wine syscalls for repeated reads of the same key (read+merge+write).
   // At 4KB pages, 16MB = 4096 pages — covers ~340 blob headers in cache.
   DatabaseExecute(g_db, "PRAGMA cache_size=-16000");
   // Store temp tables/indices in memory (no Wine disk I/O for temp data).
   DatabaseExecute(g_db, "PRAGMA temp_store=MEMORY");
   // Limit journal file growth: cap at 4MB. Prevents journal from growing
   // unbounded during large batch transactions. SQLite reuses the file.
   DatabaseExecute(g_db, "PRAGMA journal_size_limit=4194304");
   // v1.449: Removed stale `PRAGMA wal_autocheckpoint=2000` — it's a no-op
   // in DELETE journal mode (required because WAL shared memory doesn't
   // cross the Wine/Linux boundary). Had no effect; only confusing.
   // busy_timeout=5000: retry for 5s on lock instead of failing immediately.
   DatabaseExecute(g_db, "PRAGMA busy_timeout=5000");

   // Pre-prepare statements — avoids re-parsing SQL on every write (851 symbols × 9 TFs per tick)
   g_stmtBarInsert = DatabasePrepare(g_db,
      "INSERT OR REPLACE INTO bar_cache (key, data, timestamp, bar_count) VALUES (?1, ?2, ?3, ?4)");
   if(g_stmtBarInsert == INVALID_HANDLE)
      PrintFormat("BarCacheWriter: WARN — failed to prepare bar insert stmt (err %d)", GetLastError());

   g_stmtBarRead = DatabasePrepare(g_db,
      "SELECT data FROM bar_cache WHERE key = ?1");
   if(g_stmtBarRead == INVALID_HANDLE)
      PrintFormat("BarCacheWriter: WARN — failed to prepare bar read stmt (err %d)", GetLastError());

   g_stmtTrackInsert = DatabasePrepare(g_db,
      "INSERT OR REPLACE INTO bar_track (key, last_bar_time) VALUES (?1, ?2)");
   if(g_stmtTrackInsert == INVALID_HANDLE)
      PrintFormat("BarCacheWriter: WARN — failed to prepare track insert stmt (err %d)", GetLastError());

   g_stmtQuoteInsert = DatabasePrepare(g_db,
      "INSERT OR REPLACE INTO bid_ask (symbol, bid, ask, spread, timestamp) VALUES (?1, ?2, ?3, ?4, ?5)");
   if(g_stmtQuoteInsert == INVALID_HANDLE)
      PrintFormat("BarCacheWriter: WARN — failed to prepare quote insert stmt (err %d)", GetLastError());

   g_stmtMetaInsert = DatabasePrepare(g_db,
      "INSERT OR REPLACE INTO bar_cache (key, data, timestamp, bar_count) VALUES (?1, ?2, ?3, ?4)");
   if(g_stmtMetaInsert == INVALID_HANDLE)
      PrintFormat("BarCacheWriter: WARN — failed to prepare meta insert stmt (err %d)", GetLastError());

   // v1.471: ts-only touch for gap-fill requests that yield no new bars.
   // When broker returns existing data (weekend/holiday for stocks), skip
   // the 4.8 MB blob rewrite but advance the timestamp column so the
   // terminal's gap detector sees "freshly checked" and stops re-flagging
   // the key as stale on every cycle.
   g_stmtBarTouchTs = DatabasePrepare(g_db,
      "UPDATE bar_cache SET timestamp = ?1 WHERE key = ?2");
   if(g_stmtBarTouchTs == INVALID_HANDLE)
      PrintFormat("BarCacheWriter: WARN — failed to prepare bar ts touch stmt (err %d)", GetLastError());

   // Cache TF strings + periods once — eliminates 7,659 switch evals and 7,659
   // PeriodSeconds() syscalls per tick inside the hot (symbol × tf) loop.
   g_tfCount = ArraySize(g_timeframes);
   for(int t = 0; t < g_tfCount; t++)
   {
      g_tfStrings[t] = TFToStr(g_timeframes[t]);
      g_tfPeriods[t] = PeriodSeconds(g_timeframes[t]);
   }

   int initSymCount = SymbolsTotal(MarketWatchOnly);

   long acct = AccountInfoInteger(ACCOUNT_LOGIN);
   g_accountTag = IntegerToString(acct);

   // Detect CFD server by checking if USDMXN exists — only CFD servers have exotic forex
   g_isCFDServer = (SymbolInfoInteger("USDMXN", SYMBOL_EXIST) == 1);
   PrintFormat("BarCacheWriter: server type = %s (USDMXN %s)",
      g_isCFDServer ? "CFD (forex enabled)" : "Crypto/Futures (forex SKIPPED)",
      g_isCFDServer ? "found" : "not found");

   // v1.469: Write __SERVER__ metadata row once — content is fixed after
   // login, so the per-cycle rewrite was pure Mt5Sync re-merge churn.
   WriteServerOnce();

   // Restore tracking state from DB — survive restarts without re-exporting everything
   if(ForceReExport)
   {
      // Clear tracking table so all symbol/TFs are treated as "never exported"
      DatabaseExecute(g_db, "DELETE FROM bar_track");
      PrintFormat("BarCacheWriter: ForceReExport=true — cleared all tracking, full re-export queued");
      g_trackCount = 0;
      ArrayResize(g_trackKeys, 0);
      ArrayResize(g_trackTimes, 0);
      ArrayResize(g_trackFails, 0);
      ArrayResize(g_trackDirty, 0);
      ArrayResize(g_trackLastExport, 0);
   }
   else
      LoadTrackingFromDB();

   // Startup integrity check: compare DB bar counts vs MT5 available bars for ALL symbols.
   // Runs once on startup — detects data loss from ramdisk migration or interrupted exports.
   // Re-exports any symbol:TF where DB has <50% of MT5's available bars.
   // v1.437: Batched to prevent OOM on large symbol counts after reboot.
   //         Commits every INTEGRITY_BATCH_SIZE symbols to release memory.
   //         Limits bars per integrity fix to 10K (full history fills in via normal 30s cycle).
   //         Reads demand.txt if present — prioritizes symbols TyphooN-Terminal actually needs.
   if(IntegrityCheck && !ForceReExport)
   {
      int checkedCount = 0, reExportCount = 0, totalReExportedBars = 0;
      uint intStart = GetTickCount();
      int symCount = SymbolsTotal(MarketWatchOnly);
      #define INTEGRITY_BATCH_SIZE 20  // Commit every N symbols to release memory
      // Use configurable InitialBarCap (default 1000) — incremental sync fills the rest

      // Shared demand.txt loader — v3 format only.
      LoadDemandFile(symCount);

      int batchCount = 0;
      SafeBegin();
      for(int si = 0; si < symCount; si++)
      {
         // v1.452: Demand-driven only. Before, an empty demand list fell
         // through to "check every symbol" as a backwards-compat default —
         // removed. If demand.txt is absent (terminal hasn't written yet
         // or user cleared it) there's nothing to integrity-check here;
         // the normal rotation loop in ExportAll() will catch up once
         // the terminal pushes a list. Gate runs index-only so non-demand
         // symbols skip SymbolName/StringLen/IsForexSymbol overhead.
         if(g_demandCount == 0 || si >= g_demandIdxSymCount || !g_isDemandIdx[si])
            continue;

         string sym = SymbolName(si, MarketWatchOnly);
         if(StringLen(sym) == 0) continue;
         if(!g_isCFDServer && IsForexSymbol(sym)) continue;

         SymbolSelect(sym, true);

         for(int ti = 0; ti < g_tfCount; ti++)
         {
            ENUM_TIMEFRAMES enumTf = g_timeframes[ti];
            int mt5Count = Bars(sym, enumTf);
            if(mt5Count < 100) continue; // skip symbols with minimal data

            // Get DB bar count and newest bar timestamp (for staleness detection).
            // TTBR format: [4 magic "TTBR"][4 count LE u32][48B/bar: i64 ts_ms, 5×f64].
            // Bars stored oldest→newest, so newest ts_ms lives at offset 8 + (count-1)*48.
            string cacheKey = "mt5:" + sym + ":" + g_tfStrings[ti];
            int dbCount = 0;
            long dbLastTsMs = 0;
            if(g_stmtBarRead != INVALID_HANDLE)
            {
               DatabaseReset(g_stmtBarRead);
               DatabaseBind(g_stmtBarRead, 0, cacheKey);
               if(DatabaseRead(g_stmtBarRead))
               {
                  uchar tmpBlob[];
                  DatabaseColumnBlob(g_stmtBarRead, 0, tmpBlob);
                  // Full TTBR magic check — partial "TT" match could pass a
                  // corrupted blob (e.g. "TTxy") and read garbage as count.
                  if(ArraySize(tmpBlob) >= 8
                     && tmpBlob[0] == 'T' && tmpBlob[1] == 'T'
                     && tmpBlob[2] == 'B' && tmpBlob[3] == 'R')
                  {
                     dbCount = (int)tmpBlob[4] | ((int)tmpBlob[5] << 8) | ((int)tmpBlob[6] << 16) | ((int)tmpBlob[7] << 24);
                     if(dbCount > 0)
                     {
                        int lastBarOff = 8 + (dbCount - 1) * 48;
                        if(ArraySize(tmpBlob) >= lastBarOff + 8)
                        {
                           ByteConv bc;
                           ReadRaw8(tmpBlob, lastBarOff, bc);
                           dbLastTsMs = bc.l;
                        }
                     }
                  }
               }
               // v1.470: close the cursor after extracting — DatabaseRead
               // leaves SELECTs in SQLITE_ROW state, which would block any
               // subsequent COMMIT. Pre-transaction here, but keep the
               // pattern uniform across all g_stmtBarRead call sites.
               DatabaseReset(g_stmtBarRead);
            }
            checkedCount++;

            // Re-export triggers:
            //  1. Empty/short DB (<100 bars) — bootstrap or corruption.
            //  2. Stale DB — newest cached bar is >2 TF periods behind MT5's latest.
            //     Catches outage scenarios: EA was down, broker disconnected, account
            //     temporarily lost, /dev/shm persisted old data. The normal 30s cycle
            //     would eventually fill the gap but IntegrityCheck resolves it at start.
            bool needsReExport = (dbCount < 100);
            bool isStale = false;
            if(!needsReExport && dbLastTsMs > 0)
            {
               MqlRates latestRate[];
               datetime mt5LastTs = 0;
               if(CopyRates(sym, enumTf, 0, 1, latestRate) == 1)
                  mt5LastTs = latestRate[0].time;
               if(mt5LastTs > 0)
               {
                  long mt5LastMs = (long)mt5LastTs * 1000;
                  long tfMs = (long)g_tfPeriods[ti] * 1000;
                  if(tfMs > 0 && mt5LastMs - dbLastTsMs > 2 * tfMs)
                  {
                     needsReExport = true;
                     isStale = true;
                  }
               }
            }

            if(needsReExport)
            {
               // v1.463: Export the broker's full available history (capped
               // at MaxBarsForTF) instead of the old fast-restart limit of
               // InitialBarCap=1000. Matches the terminal's integrity target
               // (D1/W1/MN1 = all available, M1-H4 = last 100K). Previously
               // v1.462's shallow-redirect healed this on the next cycle
               // anyway, but routing it through startup integrity avoids
               // the ~30 s window where charts show a 1000-bar stub before
               // the live gap-fill kicks in. mt5Count is the Bars() value
               // we already queried at line ~967 for staleness detection,
               // so we reuse it rather than calling Bars() again.
               int maxBars = MathMin(mt5Count, MaxBarsForTF(enumTf));
               int bars = ExportSymbolTF(sym, enumTf, maxBars, g_tfStrings[ti]);
               if(bars > 0)
               {
                  reExportCount++;
                  totalReExportedBars += bars;
                  // Update tracking so normal loop doesn't re-export
                  string trackKey = sym + ":" + g_tfStrings[ti];
                  int idx = GetTrackIndex(trackKey);
                  MqlRates lastRate[];
                  if(CopyRates(sym, enumTf, 0, 1, lastRate) == 1)
                  {
                     g_trackTimes[idx] = lastRate[0].time;
                     SaveTrackTime(trackKey, lastRate[0].time);
                  }
                  if(reExportCount <= 50) // limit log spam
                  {
                     if(isStale)
                     {
                        long ageMin = ((long)TimeCurrent() * 1000 - dbLastTsMs) / 60000;
                        PrintFormat("  Integrity fix (stale): %s — DB last %s (%d min old), refreshed %d bars",
                           cacheKey, TimeToString((datetime)(dbLastTsMs/1000)), (int)ageMin, bars);
                     }
                     else
                     {
                        PrintFormat("  Integrity fix: %s — DB %d bars, MT5 %d, exported %d (cap %d)",
                           cacheKey, dbCount, mt5Count, bars, maxBars);
                     }
                  }
               }
            }
         }

         // Batch commit every INTEGRITY_BATCH_SIZE symbols to release memory
         batchCount++;
         if(batchCount % INTEGRITY_BATCH_SIZE == 0)
         {
            SafeCommit();
            SafeBegin();
         }
      }
      SafeCommit();
      uint intElapsed = GetTickCount() - intStart;
      PrintFormat("BarCacheWriter: startup integrity — %d keys checked, %d re-exported (%d bars) in %d ms, demand=%d",
         checkedCount, reExportCount, totalReExportedBars, intElapsed, g_demandCount);
   }

   // v1.447: Activate initial-burst if the cache looks cold. Must run AFTER
   // demand.txt load (done inside IntegrityCheck block above) so g_demandCount
   // reflects terminal priorities. IntegrityCheck already exports demand
   // symbols up to InitialBarCap bars — burst mode kicks in on subsequent
   // OnTimer calls to keep rotating through at max speed until the rest of
   // watched symbols are warm.
   g_initBurstActive = ShouldEnterInitialBurst();
   g_initBurstCycles = 0;

   PrintFormat("BarCacheWriter v1.470: %s symbols(%d), %ds interval, batch=%d, %d cached keys, 16MB cache, forex=%s, integrity=%s, initCap=%d, ramdisk=%s, burst=%s",
      MarketWatchOnly ? "MW" : "ALL", initSymCount, UpdateIntervalSec, BatchSize, g_trackCount,
      g_isCFDServer ? "ENABLED" : "SKIPPED",
      IntegrityCheck ? "ON" : "OFF",
      InitialBarCap,
      RamdiskMode ? "MEMORY+sync=OFF" : "DELETE+sync=NORMAL",
      g_initBurstActive ? "ACTIVE" : "inactive");

   EventSetTimer(UpdateIntervalSec);
   return INIT_SUCCEEDED;
}

void OnTimer() { ExportAll(); }

void ExportAll()
{
   if(g_db == INVALID_HANDLE) return;

   g_cycleCount++;

   // v1.466: O(1) mtime-gated demand.txt reload. FileGetInteger is ~1 µs
   // per file; we stat both the per-account and shared paths and take
   // the max mtime as the demand-state signature. Reload only fires
   // when the signature changes — the typical Rust-terminal hash-dedup
   // flush means mtime only bumps when content actually changes.
   // Dropped the v1.448 2-cycle gate: it added up to 30s of latency on
   // every demand.txt change for no benefit now that the reload itself
   // is gated by mtime. `lastDemandMtime == 0` bootstrap covers the
   // cold-attach case where demand.txt pre-exists EA launch.
   {
      static long lastDemandMtime = 0;
      long acctMtime = (long)FileGetInteger(g_accountTag + "_demand.txt", FILE_MODIFY_DATE, true);
      long sharedMtime = (long)FileGetInteger("demand.txt", FILE_MODIFY_DATE, true);
      long curMtime = acctMtime > sharedMtime ? acctMtime : sharedMtime;
      if(curMtime != 0 && curMtime != lastDemandMtime)
      {
         LoadDemandFile(SymbolsTotal(MarketWatchOnly));
         lastDemandMtime = curMtime;
      }
   }

   // Periodic maintenance: every 60 cycles (~30 minutes at 30s interval).
   // PRAGMA incremental_vacuum reclaims freed pages from DELETE mode fragmentation.
   // Without this, the DB file grows monotonically as SQLite reuses freed pages
   // only within the same session — Wine memory maps grow proportionally.
   if(g_cycleCount % 60 == 0)
   {
      DatabaseExecute(g_db, "PRAGMA incremental_vacuum(100)"); // reclaim up to 100 pages per run
   }

   uint tickStart = GetTickCount();
   int symCount = SymbolsTotal(MarketWatchOnly);
   // Hoist TimeCurrent() once — reused for log gating, meta gating, quote stamps,
   // and TF gating below. One syscall per cycle instead of 5+.
   datetime cycleNow = TimeCurrent();

   static datetime lastTickLog = 0;
   if(cycleNow - lastTickLog > 300)
   {
      PrintFormat("BarCacheWriter: tick start — %d symbols", symCount);
      lastTickLog = cycleNow;
   }

   int exported = 0, skipped = 0, totalBars = 0;

   // v1.466: Metadata written every cycle. The old 5-min gate existed
   // to suppress mtime bumps that would defeat the Rust sync's mtime
   // fast-path skip. That fast-path is gone — the Rust side now uses
   // get_cache_meta_since(ts), which reads only changed rows regardless
   // of mtime. The gate's only remaining effect was to delay fresh-symbol
   // discovery by up to 300s. Dropping it propagates broker symbol-list
   // changes within 30s. Re-copy cost on the Rust side is ≤5 mt5:__
   // metadata keys per pass — trivial versus the responsiveness win.
   SafeBegin();
   WriteSymbolList(symCount);
   WriteSymbolSpecs(symCount);
   SafeCommit();
   uint afterMeta = GetTickCount();
   if(afterMeta - tickStart > 1000)
      PrintFormat("  metadata took %d ms", afterMeta - tickStart);

   // Live bid/ask sync — every OTHER cycle (60s) to align with 1-min bar writes
   // v1.441: reduced from every cycle to every 2nd — matches M1 bar frequency
   // v1.468: demand-scoped. Terminal's chart-refresh path is the only consumer
   // and it drops every row whose symbol isn't an open chart — chart symbols
   // are always in demand. Full-sweep fallback kept for standalone EA deploys
   // (no terminal attached → demand.txt never arrives).
   static int quoteSkip = 0;
   quoteSkip++;
   if(g_stmtQuoteInsert != INVALID_HANDLE && quoteSkip % 2 == 0)
   {
      SafeBegin();
      long now = (long)cycleNow;
      int quoteCount = 0;
      if(g_demandCount > 0)
      {
         for(int d = 0; d < g_demandCount; d++)
         {
            string qSym = g_demandSymbols[d];
            double bid = SymbolInfoDouble(qSym, SYMBOL_BID);
            double ask = SymbolInfoDouble(qSym, SYMBOL_ASK);
            if(bid <= 0 && ask <= 0) continue;
            double spread = (ask > 0 && bid > 0) ? (ask - bid) : 0;
            DatabaseReset(g_stmtQuoteInsert);
            DatabaseBind(g_stmtQuoteInsert, 0, qSym);
            DatabaseBind(g_stmtQuoteInsert, 1, bid);
            DatabaseBind(g_stmtQuoteInsert, 2, ask);
            DatabaseBind(g_stmtQuoteInsert, 3, spread);
            DatabaseBind(g_stmtQuoteInsert, 4, now);
            if(DatabaseRead(g_stmtQuoteInsert)) quoteCount++;
         }
      }
      else
      {
         for(int q = 0; q < symCount; q++)
         {
            string qSym = SymbolName(q, MarketWatchOnly);
            if(StringLen(qSym) == 0) continue;
            double bid = SymbolInfoDouble(qSym, SYMBOL_BID);
            double ask = SymbolInfoDouble(qSym, SYMBOL_ASK);
            if(bid <= 0 && ask <= 0) continue;
            double spread = (ask > 0 && bid > 0) ? (ask - bid) : 0;
            DatabaseReset(g_stmtQuoteInsert);
            DatabaseBind(g_stmtQuoteInsert, 0, qSym);
            DatabaseBind(g_stmtQuoteInsert, 1, bid);
            DatabaseBind(g_stmtQuoteInsert, 2, ask);
            DatabaseBind(g_stmtQuoteInsert, 3, spread);
            DatabaseBind(g_stmtQuoteInsert, 4, now);
            if(DatabaseRead(g_stmtQuoteInsert)) quoteCount++;
         }
      }
      SafeCommit();
   }

   int batchCount = 0;  // symbols in current transaction batch
   bool inTxn = false;

   // v1.438 CPU optimization: process symbols in rotating batches instead of all 851 every cycle.
   // Each cycle processes BatchSize*10 symbols (default 100). Full rotation in ~8 cycles (~4 min).
   // Demand symbols (from TyphooN-Terminal) are ALWAYS processed every cycle for low latency.
   static int rotationOffset = 0;
   int symbolsPerCycle = BatchSize * 10; // default 100 symbols per 30s cycle

   // v1.443: Rebuild demand bitmap if symCount changed since last rebuild.
   // Cheap safety check — typical cost is zero (no rebuild needed).
   // v1.465: the O(1) slot caches ride along — g_gapFillMaxBarsByMWIdx and
   // g_trackIdxByMWIdx are both indexed by (mwIdx * g_tfCount + tf), so
   // they must rebuild whenever symCount changes too.
   if(g_demandCount > 0 && g_demandIdxSymCount != symCount)
      RebuildDemandIndexBitmap(symCount);
   if(g_slotCacheSymCount != symCount)
      RebuildSlotCaches(symCount);

   for(int i = 0; i < symCount; i++)
   {
      // v1.452: Rotation/demand gate runs FIRST — index-only checks. Skipped
      // symbols avoid SymbolName() + StringLen() + IsForexSymbol() cost.
      // On non-CFD servers with 851 symbols, ~750 per cycle are outside the
      // rotation window; previously each paid the string + forex-check toll
      // (~1700 StringSubstr allocs + ~3800 binary-search compares) just to
      // be discarded. Now they cost a single bool read.
      // v1.443: O(1) bitmap lookup replaces O(log m) BinarySearchKey call.
      bool isDemand = (g_demandCount > 0 && i < g_demandIdxSymCount)
                      ? g_isDemandIdx[i]
                      : false;
      // v1.447: Initial-burst — process demand symbols only, skip everything else
      // so we don't waste cycles on the long tail until watched symbols are warm.
      if(g_initBurstActive)
      {
         if(!isDemand) continue;
      }
      else if(!isDemand && (i < rotationOffset || i >= rotationOffset + symbolsPerCycle))
         continue;

      string symbol = SymbolName(i, MarketWatchOnly);
      if(StringLen(symbol) == 0) continue;

      // Skip forex symbols on non-CFD servers
      if(!g_isCFDServer && IsForexSymbol(symbol)) continue;

      SymbolSelect(symbol, true);

      // Batched transactions — group BatchSize symbols per BEGIN/COMMIT to reduce fsync overhead
      if(!inTxn)
      {
         SafeBegin();
         inTxn = true;
         batchCount = 0;
      }

      for(int tf = 0; tf < g_tfCount; tf++)
      {
         // v1.465: O(1) gap-fill lookup via flat slot cache. Replaces the
         // binary-search GetGapFillMaxBars call that ran once per (sym,TF)
         // — ~900 calls/cycle. Also resolves the trackIdx through a
         // session-lifetime lazy cache so GetTrackIndex's binary search
         // fires at most once per (sym,TF) instead of once per cycle.
         int slot = i * g_tfCount + tf;
         int gapBars = (slot < ArraySize(g_gapFillMaxBarsByMWIdx))
            ? g_gapFillMaxBarsByMWIdx[slot] : 0;

         // Resolve trackIdx — cached for the session after first hit.
         // GetTrackIndex may invalidate the cache (sort after append), so
         // always refetch from the cache after the call to stay coherent.
         string trackKey = symbol + ":" + g_tfStrings[tf];
         int idx = -1;
         if(slot < ArraySize(g_trackIdxByMWIdx))
            idx = g_trackIdxByMWIdx[slot];
         if(idx < 0 || idx >= g_trackCount)
         {
            idx = GetTrackIndex(trackKey);
            if(slot < ArraySize(g_trackIdxByMWIdx))
               g_trackIdxByMWIdx[slot] = idx;
         }

         // v1.465: Per-(sym,TF) TF-period gate. Previously g_tfLastExportTime[tf]
         // was a single datetime shared by all symbols on that TF — the first
         // symbol's export stamped the global timer, gating EVERY other symbol
         // for the next 80% of the TF period. With 38 demand pairs on 1Min, a
         // single export starved 37 other symbols for 48s. g_trackLastExport[idx]
         // is per-pair so every (sym,TF) rides its own gate independently.
         int tfPeriod = g_tfPeriods[tf];
         if(gapBars == 0 && !g_initBurstActive && g_trackLastExport[idx] > 0 && tfPeriod > 0)
         {
            int elapsed = (int)(cycleNow - g_trackLastExport[idx]);
            if(elapsed < (int)(tfPeriod * 0.8))
            {
               skipped++;
               continue;
            }
         }

         // v1.447: Gap-fill request from terminal — force a sync and clear
         // the request on success. Runs AHEAD of the normal track-time
         // comparison so a mid-session gap request always wins.
         // v1.459: merge-instead-of-replace; v1.462: shallow-cache redirect.
         if(gapBars > 0)
         {
            int existingCount = GetCachedBarCount(trackKey);
            int done;
            // v1.472: `gapBars > existingCount` also covers the existingCount==0
            // case, routing a cold-start gap-fill to ExportSymbolTF(gapBars) instead
            // of IncrementalExportSymbolTF — the latter would recurse into
            // ExportSymbolTF(maxBars=0) (unbounded full history) when no row exists,
            // producing multi-MB blobs that blow the SQLite commit threshold.
            bool shallow = (gapBars > existingCount);
            if(shallow)
               done = ExportSymbolTF(symbol, g_timeframes[tf], gapBars, g_tfStrings[tf]);
            else
               done = IncrementalExportSymbolTF(symbol, g_timeframes[tf], g_trackTimes[idx], g_tfStrings[tf], g_tfPeriods[tf]);
            if(done > 0)
            {
               exported++;
               totalBars += done;
               g_trackFails[idx] = 0;
               g_trackLastExport[idx] = cycleNow;
               MqlRates gapLast[];
               if(CopyRates(symbol, g_timeframes[tf], 0, 1, gapLast) == 1)
               {
                  g_trackTimes[idx] = gapLast[0].time;
                  g_trackDirty[idx] = true;
               }
               ClearGapFillSlot(i, tf, trackKey);
               PrintFormat("BarCacheWriter: gap-fill %s — %s to %d bars (req %d, had %d)",
                  trackKey, shallow ? "re-seeded" : "merged", done, gapBars, existingCount);
               continue;
            }
            // Failed — fall through to normal path; ClearGapFill only on success.
         }

         MqlRates lastRate[];
         int gotLast = CopyRates(symbol, g_timeframes[tf], 0, 1, lastRate);

         // Never synced — try to export whatever is locally available (non-blocking)
         if(g_trackTimes[idx] == 0)
         {
            if(g_trackFails[idx] >= MAX_CONSEC_FAILS)
            {
               skipped++;
               continue;
            }
            if(Bars(symbol, g_timeframes[tf]) == 0)
            {
               g_trackFails[idx]++;
               if(g_trackFails[idx] >= MAX_CONSEC_FAILS)
               {
                  g_trackTimes[idx] = (datetime)FAIL_SENTINEL;
                  SaveTrackTime(trackKey, (datetime)FAIL_SENTINEL);
                  PrintFormat("BarCacheWriter: giving up on %s (no bars available after %d checks)", trackKey, MAX_CONSEC_FAILS);
               }
               continue;
            }
            int bars = ExportSymbolTF(symbol, g_timeframes[tf], MaxBarsForTF(g_timeframes[tf]), g_tfStrings[tf]);

            if(bars > 0)
            {
               exported++;
               totalBars += bars;
               g_trackFails[idx] = 0;
               g_trackLastExport[idx] = cycleNow;
               if(gotLast == 1)
               {
                  g_trackTimes[idx] = lastRate[0].time;
                  g_trackDirty[idx] = true;
               }
            }
            else
            {
               g_trackFails[idx]++;
               if(g_trackFails[idx] >= MAX_CONSEC_FAILS)
               {
                  g_trackTimes[idx] = (datetime)FAIL_SENTINEL;
                  SaveTrackTime(trackKey, (datetime)FAIL_SENTINEL);
                  PrintFormat("BarCacheWriter: giving up on %s after %d failures", trackKey, MAX_CONSEC_FAILS);
               }
            }
            continue;
         }

         // Already synced — skip if last bar hasn't changed
         if(gotLast == 1 && lastRate[0].time == g_trackTimes[idx])
         {
            skipped++;
            continue;
         }

         int bars = IncrementalExportSymbolTF(symbol, g_timeframes[tf], g_trackTimes[idx], g_tfStrings[tf], g_tfPeriods[tf]);
         if(bars > 0)
         {
            exported++;
            totalBars += bars;
            g_trackLastExport[idx] = cycleNow;
            if(gotLast == 1)
            {
               g_trackTimes[idx] = lastRate[0].time;
               g_trackDirty[idx] = true;
            }
         }
      }

      batchCount++;
      if(batchCount >= BatchSize)
      {
         SafeCommit();
         inTxn = false;
         // Yield CPU between batches. locking_mode is NORMAL (default), so each
         // SafeCommit() releases the RESERVED/EXCLUSIVE lock — readers (Mt5Sync)
         // can grab a SHARED lock during this 200ms window. Without the sleep
         // the writer would re-acquire the lock immediately on the next batch
         // and readers would hit busy_timeout repeatedly.
         // v1.447: Shorter sleep in burst mode — we want to finish cold-start
         // warming quickly; readers can tolerate briefer contention windows.
         Sleep(g_initBurstActive ? 50 : 200);
      }
   }

   // Commit any remaining symbols in the last partial batch
   if(inTxn) SafeCommit();

   // Sort tracking arrays after population phase for O(log n) lookups on subsequent ticks
   if(!g_trackSorted && g_trackCount > 0)
      SortTrackArrays();

   // v1.443: Periodically flush dirty bar_track entries in one transaction.
   // In-memory state is authoritative between flushes — on crash we lose at
   // most TRACK_FLUSH_CYCLES cycles of tracking state, which the next cycle's
   // ExportSymbolTF re-detects via lastRate[0].time comparison. Net effect:
   // ~10× fewer bar_track DB writes per cycle under steady-state operation.
   g_trackFlushCounter++;
   if(g_trackFlushCounter >= TRACK_FLUSH_CYCLES)
   {
      int flushed = FlushDirtyTrackTimes();
      g_trackFlushCounter = 0;
      if(flushed > 0)
         PrintFormat("BarCacheWriter: flushed %d dirty track rows", flushed);
   }

   // Periodic compact: stale-row DELETE + VACUUM every ~2 hours to reclaim
   // /dev/shm space. DELETE runs first so VACUUM reclaims its freed pages
   // in the same pass (otherwise the pages sit on the free-list until the
   // next VACUUM 2h later). 14-day retention covers long market closures
   // while still evicting rows for symbols that have left the demand set
   // or the broker's symbol list (old chart tabs, removed instruments).
   if(g_cycleCount % 240 == 0 && g_cycleCount > 0)
   {
      DeleteStaleBarCacheRows(14 * 24 * 3600);
      uint vacStart = GetTickCount();
      DatabaseExecute(g_db, "VACUUM");
      PrintFormat("BarCacheWriter: VACUUM completed (%d ms)", GetTickCount() - vacStart);
   }

   int pendingSlots = 0;
   int failedSlots = 0;
   for(int i = 0; i < g_trackCount; i++)
   {
      if(g_trackTimes[i] == 0) pendingSlots++;
      else if(g_trackTimes[i] == (datetime)FAIL_SENTINEL) failedSlots++;
   }

   static datetime lastLog = 0;
   static bool first = true;
   static int failCount = 0;
   if(exported == 0 && skipped == 0) failCount++;
   else failCount = 0;

   // Advance rotation for next cycle
   rotationOffset += symbolsPerCycle;
   if(rotationOffset >= symCount) rotationOffset = 0;

   if(first || TimeCurrent() - lastLog > 300 || failCount <= 3)
   {
      uint elapsed = GetTickCount() - tickStart;
      PrintFormat("BarCacheWriter: %d exported, %d skipped, %d bars | %d pending | %dms (batch %d-%d of %d)%s",
         exported, skipped, totalBars, pendingSlots, elapsed,
         rotationOffset, MathMin(rotationOffset + symbolsPerCycle, symCount), symCount,
         g_initBurstActive ? " [BURST]" : "");

      // Diagnostic: if nothing exported, test first 3 symbols to see why
      if(exported == 0 && skipped == 0)
      {
         int diagCount = MathMin(symCount, 3);
         for(int d = 0; d < diagCount; d++)
         {
            string diagSym = SymbolName(d, MarketWatchOnly);
            MqlRates diagRates[];
            ArraySetAsSeries(diagRates, false);
            int diagBars = CopyRates(diagSym, PERIOD_D1, 0, 10, diagRates);
            PrintFormat("  DIAG: %s D1 CopyRates(0,10)=%d err=%d selected=%s",
               diagSym, diagBars, GetLastError(),
               SymbolInfoInteger(diagSym, SYMBOL_SELECT) ? "YES" : "NO");
         }
      }

      lastLog = TimeCurrent();
      first = false;
   }

   // v1.447: Burst-mode exit check — once every demand symbol has at least
   // one populated TF, drop back to the efficient rotation schedule.
   if(g_initBurstActive)
   {
      g_initBurstCycles++;
      if(CanExitInitialBurst())
      {
         PrintFormat("BarCacheWriter: initial-burst completed after %d cycles (%d tracked keys); reverting to rotation.",
            g_initBurstCycles, g_trackCount);
         g_initBurstActive = false;
      }
      else if(g_initBurstCycles % 10 == 0)
      {
         PrintFormat("BarCacheWriter: still in burst (cycle %d) — %d demand symbols, %d tracked keys",
            g_initBurstCycles, g_demandCount, g_trackCount);
      }
   }

   // v1.447: Heartbeat — written every cycle so readers can detect liveness
   // without parsing logs. Single extra row-write per 30s cycle, negligible.
   {
      uint cycleMs = GetTickCount() - tickStart;
      SafeBegin();
      WriteHeartbeat(rotationOffset, symCount, cycleMs, exported, skipped);
      SafeCommit();
   }
}

void WriteSymbolList(int symCount)
{
   // v1.469: Fast skip — same symCount + recent write → nothing to do.
   datetime tc = TimeCurrent();
   if(g_symbolsLastCount == symCount
      && g_symbolsLastDbWrite > 0
      && tc - g_symbolsLastDbWrite < 300
      && StringLen(g_cachedSymbolsJson) > 0)
   {
      return;
   }

   // Rebuild JSON — O(n) via per-element array, single join.
   string names[];
   ArrayResize(names, symCount);
   int count = 0;
   for(int i = 0; i < symCount; i++)
   {
      string s = SymbolName(i, MarketWatchOnly);
      if(StringLen(s) == 0) continue;
      names[count++] = s;
   }

   int totalLen = 2;
   for(int i = 0; i < count; i++)
      totalLen += StringLen(names[i]) + 3;

   uchar buf[];
   ArrayResize(buf, totalLen + 1);
   int pos = 0;
   buf[pos++] = '[';
   for(int i = 0; i < count; i++)
   {
      if(i > 0) buf[pos++] = ',';
      buf[pos++] = '"';
      uchar nameBytes[];
      int nameLen = StringToCharArray(names[i], nameBytes, 0, -1, CP_UTF8) - 1;
      ArrayCopy(buf, nameBytes, pos, 0, nameLen);
      pos += nameLen;
      buf[pos++] = '"';
   }
   buf[pos++] = ']';
   string csv = CharArrayToString(buf, 0, pos, CP_UTF8);

   // Content dedup — if the rebuilt JSON matches the cache, skip the upsert
   // but refresh the fast-skip deadline so we don't re-rebuild every cycle.
   if(csv == g_cachedSymbolsJson && g_symbolsLastCount == count)
   {
      g_symbolsLastDbWrite = tc;
      return;
   }

   g_cachedSymbolsJson  = csv;
   g_symbolsLastCount   = count;
   g_symbolsLastDbWrite = tc;

   if(g_stmtMetaInsert != INVALID_HANDLE)
   {
      DatabaseReset(g_stmtMetaInsert);
      DatabaseBind(g_stmtMetaInsert, 0, "mt5:__SYMBOLS__:" + g_accountTag);
      DatabaseBind(g_stmtMetaInsert, 1, csv);
      DatabaseBind(g_stmtMetaInsert, 2, (long)tc);
      DatabaseBind(g_stmtMetaInsert, 3, (long)count);
      DatabaseRead(g_stmtMetaInsert);
   }
}

// v1.469: Write the __SERVER__ metadata row once. Server + company strings
// are fixed for the duration of an MT5 login session, so there's no reason
// to re-upsert every cycle — doing so would just keep Mt5Sync re-merging
// identical bytes on every pass. Called from OnInit after g_accountTag is
// populated and the pre-prepared meta insert is ready.
void WriteServerOnce()
{
   if(g_stmtMetaInsert == INVALID_HANDLE) return;
   string server = AccountInfoString(ACCOUNT_SERVER);
   string company = AccountInfoString(ACCOUNT_COMPANY);
   string meta = "{\"server\":\"" + server + "\",\"company\":\"" + company + "\"}";
   DatabaseReset(g_stmtMetaInsert);
   DatabaseBind(g_stmtMetaInsert, 0, "mt5:__SERVER__:" + g_accountTag);
   DatabaseBind(g_stmtMetaInsert, 1, meta);
   DatabaseBind(g_stmtMetaInsert, 2, (long)TimeCurrent());
   DatabaseBind(g_stmtMetaInsert, 3, 0);
   DatabaseRead(g_stmtMetaInsert);
}

void WriteSymbolSpecs(int symCount)
{
   // v1.439: Cache specs and only rebuild when SpecsCacheMin has elapsed.
   // SymbolInfo calls are expensive (~16 per symbol × 851 symbols = 13,616 calls).
   // Specs rarely change (swap rates, margins updated by broker infrequently).
   // v1.443: When the cache is fresh AND we already persisted it recently,
   // skip the DB write entirely. The caller gate writes metadata every 5 min
   // but specs content only changes every SpecsCacheMin (default 60 min), so
   // 11/12 of those write calls are redundant.
   datetime tc = TimeCurrent();
   if(StringLen(g_cachedSpecsCsv) > 0 && g_specsLastBuild > 0
      && tc - g_specsLastBuild < SpecsCacheMin * 60)
   {
      // Cache is fresh. If we already wrote this same CSV to the DB within the
      // cache window, skip the write — the on-disk row is still current.
      if(g_specsLastDbWrite > 0 && tc - g_specsLastDbWrite < SpecsCacheMin * 60)
         return;
      // Otherwise use cached CSV — write it to DB (timestamps updated).
      if(g_stmtMetaInsert != INVALID_HANDLE)
      {
         DatabaseReset(g_stmtMetaInsert);
         DatabaseBind(g_stmtMetaInsert, 0, "mt5:__SPECS__:" + g_accountTag);
         DatabaseBind(g_stmtMetaInsert, 1, g_cachedSpecsCsv);
         DatabaseBind(g_stmtMetaInsert, 2, (long)tc);
         DatabaseBind(g_stmtMetaInsert, 3, (long)symCount);
         DatabaseRead(g_stmtMetaInsert);
         g_specsLastDbWrite = tc;
      }
      return;
   }

   // Rebuild specs CSV — build per-line then join (avoids quadratic string growth)
   // Format: Symbol,SectorName,IndustryName,TradeMode,SwapLong,SwapShort,Spread,
   //         VolumeMin,VolumeMax,VolumeStep,ContractSize,TickSize,TickValue,
   //         Digits,MarginInitial,MarginMaintenance,BaseCurrency,QuoteCurrency,Description
   string lines[];
   int count = 0;
   ArrayResize(lines, symCount); // Pre-allocate (may be slightly over, that's fine)

   for(int i = 0; i < symCount; i++)
   {
      string s = SymbolName(i, MarketWatchOnly);
      if(StringLen(s) == 0) continue;

      string sector = SymbolInfoString(s, SYMBOL_SECTOR_NAME);
      string industry = SymbolInfoString(s, SYMBOL_INDUSTRY_NAME);
      int tradeMode = (int)SymbolInfoInteger(s, SYMBOL_TRADE_MODE);
      double swapLong = SymbolInfoDouble(s, SYMBOL_SWAP_LONG);
      double swapShort = SymbolInfoDouble(s, SYMBOL_SWAP_SHORT);
      int spread = (int)SymbolInfoInteger(s, SYMBOL_SPREAD);
      double volMin = SymbolInfoDouble(s, SYMBOL_VOLUME_MIN);
      double volMax = SymbolInfoDouble(s, SYMBOL_VOLUME_MAX);
      double volStep = SymbolInfoDouble(s, SYMBOL_VOLUME_STEP);
      double contractSize = SymbolInfoDouble(s, SYMBOL_TRADE_CONTRACT_SIZE);
      double tickSize = SymbolInfoDouble(s, SYMBOL_TRADE_TICK_SIZE);
      double tickValue = SymbolInfoDouble(s, SYMBOL_TRADE_TICK_VALUE);
      int digits = (int)SymbolInfoInteger(s, SYMBOL_DIGITS);
      double marginInit = SymbolInfoDouble(s, SYMBOL_MARGIN_INITIAL);
      double marginMaint = SymbolInfoDouble(s, SYMBOL_MARGIN_MAINTENANCE);
      string baseCcy = SymbolInfoString(s, SYMBOL_CURRENCY_BASE);
      string quoteCcy = SymbolInfoString(s, SYMBOL_CURRENCY_PROFIT);
      string desc = SymbolInfoString(s, SYMBOL_DESCRIPTION);

      // Single-pass sanitize (commas/newlines break CSV) — avoids 3× full-string scans
      {
         int dLen = StringLen(desc);
         if(dLen > 0)
         {
            uchar dBuf[];
            int dBytes = StringToCharArray(desc, dBuf, 0, -1, CP_UTF8) - 1;
            int wp = 0;
            for(int c = 0; c < dBytes; c++)
            {
               if(dBuf[c] == ',')       dBuf[wp++] = ';';
               else if(dBuf[c] == '\n') dBuf[wp++] = ' ';
               else if(dBuf[c] == '\r') continue; // skip \r entirely
               else                     dBuf[wp++] = dBuf[c];
            }
            desc = CharArrayToString(dBuf, 0, wp, CP_UTF8);
         }
      }

      // Build line in single StringConcatenate (one allocation per line, not 19)
      lines[count] = s + ","
           + sector + ","
           + industry + ","
           + IntegerToString(tradeMode) + ","
           + DoubleToString(swapLong, 4) + ","
           + DoubleToString(swapShort, 4) + ","
           + IntegerToString(spread) + ","
           + DoubleToString(volMin, 4) + ","
           + DoubleToString(volMax, 2) + ","
           + DoubleToString(volStep, 4) + ","
           + DoubleToString(contractSize, 2) + ","
           + DoubleToString(tickSize, 8) + ","
           + DoubleToString(tickValue, 6) + ","
           + IntegerToString(digits) + ","
           + DoubleToString(marginInit, 2) + ","
           + DoubleToString(marginMaint, 2) + ","
           + baseCcy + ","
           + quoteCcy + ","
           + desc;
      count++;
   }

   // v1.444: Measure UTF-8 bytes (not UTF-16 chars) — non-ASCII descriptions like "México" overflow otherwise
   int totalLen = 0;
   {
      uchar scratch[];
      for(int i = 0; i < count; i++)
      {
         int bLen = StringToCharArray(lines[i], scratch, 0, -1, CP_UTF8) - 1;
         if(bLen < 0) bLen = 0;
         totalLen += bLen + 1; // +1 for newline separator/terminator
      }
   }
   uchar csvBuf[];
   if(ArrayResize(csvBuf, totalLen + 1) < 0) { Print("BarCacheWriter: ArrayResize csvBuf failed (",totalLen+1," bytes)"); return; }
   int csvPos = 0;
   for(int i = 0; i < count; i++)
   {
      if(i > 0) csvBuf[csvPos++] = '\n';
      uchar lineBytes[];
      int lineLen = StringToCharArray(lines[i], lineBytes, 0, -1, CP_UTF8) - 1;
      ArrayCopy(csvBuf, lineBytes, csvPos, 0, lineLen);
      csvPos += lineLen;
   }
   csvBuf[csvPos++] = '\n';
   string csv = CharArrayToString(csvBuf, 0, csvPos, CP_UTF8);

   // Cache the built CSV
   g_cachedSpecsCsv = csv;
   g_specsLastBuild = TimeCurrent();

   // Write to DB using pre-prepared statement
   if(g_stmtMetaInsert != INVALID_HANDLE)
   {
      DatabaseReset(g_stmtMetaInsert);
      DatabaseBind(g_stmtMetaInsert, 0, "mt5:__SPECS__:" + g_accountTag);
      DatabaseBind(g_stmtMetaInsert, 1, csv);
      DatabaseBind(g_stmtMetaInsert, 2, (long)g_specsLastBuild);
      DatabaseBind(g_stmtMetaInsert, 3, (long)count);
      DatabaseRead(g_stmtMetaInsert);
      g_specsLastDbWrite = g_specsLastBuild; // v1.443: track last successful DB write
   }
}

// ── Incremental Export ────────────────────────────────────────────────────
// Instead of re-exporting ALL history (100K+ bars), only fetches bars since
// last sync time, reads the existing blob from DB, merges, and writes back.
// Reduces per-cycle work from 100K bars to ~200 bars (99.8% reduction).

// Read raw 8 bytes from buffer into ByteConv
void ReadRaw8(const uchar &buf[], int off, ByteConv &bc)
{
   bc.b[0] = buf[off];   bc.b[1] = buf[off+1]; bc.b[2] = buf[off+2]; bc.b[3] = buf[off+3];
   bc.b[4] = buf[off+4]; bc.b[5] = buf[off+5]; bc.b[6] = buf[off+6]; bc.b[7] = buf[off+7];
}

// ── v1.427 Incremental Export — SQL BLOB manipulation ────────────────────────
// Previous approach: read full blob (up to 4.8MB) into MQL5, ArrayCopy, write back.
// New approach: only the delta (48 bytes × new bars) crosses the MQL5/SQLite boundary.
// SQLite's SUBSTR slices the existing blob entirely server-side.
//
// Three SQL UPDATE cases, all via pre-prepared statements:
//   g_stmtUpdateLastBar       — forming bar close update only (appendCount == 0)
//   g_stmtReplaceLastAndAppend — matched last bar + N new bars (newStart > 0)
//   g_stmtAppendOnly          — N new bars from a timestamp beyond existing last (newStart == 0)
//
// v1.430: Reliable in-memory merge. Reads existing blob, merges new bars, writes back.
// v1.427-v1.429 used SQL SUBSTR/BLOB UPDATE which corrupted data due to MQL5's
// DatabaseBindArray binding uchar[] as TEXT — SUBSTR on TEXT uses character offsets
// not byte offsets, producing truncated output (110 bytes instead of megabytes).
int IncrementalExportSymbolTF(string symbol, ENUM_TIMEFRAMES tf, datetime lastSyncTime, const string &tfStr, int tfPeriod)
{
   int elapsed = (int)(TimeCurrent() - lastSyncTime);
   int estimatedNewBars = (tfPeriod > 0) ? (elapsed / tfPeriod) + 2 : 10;
   int fetchCount = MathMax(3, MathMin(estimatedNewBars, 200));

   MqlRates newRates[];
   ArraySetAsSeries(newRates, false);
   int newCopied = CopyRates(symbol, tf, 0, fetchCount, newRates);
   if(newCopied <= 0) return 0;

   string key = "mt5:" + symbol + ":" + tfStr;

   // Read existing blob from DB via pre-prepared statement
   if(g_stmtBarRead == INVALID_HANDLE) return ExportSymbolTF(symbol, tf, MAX_BARS_PER_KEY, tfStr);
   DatabaseReset(g_stmtBarRead);
   DatabaseBind(g_stmtBarRead, 0, key);
   uchar existingBlob[];
   bool hasExisting = false;
   if(DatabaseRead(g_stmtBarRead))
   {
      DatabaseColumnBlob(g_stmtBarRead, 0, existingBlob);
      hasExisting = (ArraySize(existingBlob) >= 8);
   }
   // v1.470: close the cursor so the outer batch COMMIT isn't blocked by
   // a statement stuck in SQLITE_ROW state. DatabaseRead only steps once;
   // the explicit reset here releases the cursor before any other DB work.
   DatabaseReset(g_stmtBarRead);

   if(!hasExisting)
      return ExportSymbolTF(symbol, tf, MAX_BARS_PER_KEY, tfStr);

   // Verify TTBR magic
   if(existingBlob[0] != 'T' || existingBlob[1] != 'T' || existingBlob[2] != 'B' || existingBlob[3] != 'R')
      return ExportSymbolTF(symbol, tf, MAX_BARS_PER_KEY, tfStr);

   int existingCount = (int)existingBlob[4]
                     | ((int)existingBlob[5] << 8)
                     | ((int)existingBlob[6] << 16)
                     | ((int)existingBlob[7] << 24);

   if(existingCount <= 0 || ArraySize(existingBlob) < 8 + existingCount * 48)
      return ExportSymbolTF(symbol, tf, MAX_BARS_PER_KEY, tfStr);

   // Find last existing bar timestamp
   ByteConv bc;
   int lastOff = 8 + (existingCount - 1) * 48;
   ReadRaw8(existingBlob, lastOff, bc);
   long lastExistingTsMs = bc.l;

   // v1.453: Hole detection. If the oldest fetched bar is strictly newer
   // than the last existing bar AND the broker has bars in that gap, our
   // fixed-count fetch (capped at 200) couldn't reach back far enough.
   // Partial merge in that case would silently drop the intermediate bars
   // and stamp the row with TimeCurrent() — invisible to the terminal's
   // gap detection downstream. Full re-export from D'1970.01.01' closes
   // the hole in one shot.
   //
   // Covers: Wine crash mid-rotation, MT5 crash and restart, internet
   // outage, laptop hibernate across weekends, any outage longer than
   // the 200-bar fetch window. Weekend / market-closed gaps don't trip
   // this — Bars() returns 0 for a range with no bars on the broker side.
   long firstNewTsMs = (long)newRates[0].time * 1000;
   if(firstNewTsMs > lastExistingTsMs)
   {
      datetime gapStart = (datetime)(lastExistingTsMs / 1000) + 1;
      datetime gapEnd   = newRates[0].time - 1;
      if(gapEnd >= gapStart && Bars(symbol, tf, gapStart, gapEnd) > 0)
         return ExportSymbolTF(symbol, tf, MAX_BARS_PER_KEY, tfStr);
   }

   // v1.454: Reverse-order guard. If the newest fetched bar is strictly
   // older than our last cached bar, the cache is ahead of the broker.
   // The merge loop below would leave newStart=0 (no `==`/`>` match),
   // appending older bars out-of-order and breaking the monotonic
   // invariant the terminal's binary search relies on. Causes: system
   // clock skew rolled back, broker data switch mid-session, bit-rot
   // in the timestamp header. Force a full re-export to resync.
   long lastNewTsMs = (long)newRates[newCopied - 1].time * 1000;
   if(lastNewTsMs < lastExistingTsMs)
      return ExportSymbolTF(symbol, tf, MAX_BARS_PER_KEY, tfStr);

   // Find merge point
   int newStart = 0;
   for(int i = 0; i < newCopied; i++)
   {
      long barTsMs = (long)newRates[i].time * 1000;
      if(barTsMs > lastExistingTsMs)
      {
         newStart = i;
         break;
      }
      if(barTsMs == lastExistingTsMs)
      {
         // Update last bar in-place
         int overOff = 8 + (existingCount - 1) * 48;
         bc.l = barTsMs; WriteRaw8(existingBlob, overOff, bc);
         bc.d = newRates[i].open; WriteRaw8(existingBlob, overOff + 8, bc);
         bc.d = newRates[i].high; WriteRaw8(existingBlob, overOff + 16, bc);
         bc.d = newRates[i].low; WriteRaw8(existingBlob, overOff + 24, bc);
         bc.d = newRates[i].close; WriteRaw8(existingBlob, overOff + 32, bc);
         bc.d = (double)newRates[i].tick_volume; WriteRaw8(existingBlob, overOff + 40, bc);
         newStart = i + 1;
      }
   }

   int appendCount = newCopied - newStart;
   if(appendCount <= 0)
   {
      // No new bars — skip the blob write but touch the timestamp column.
      // The forming bar's close/high/low changes every tick but rewriting a
      // 4.8MB blob for a 48-byte update is the #1 cause of Wine I/O contention.
      // The forming bar will be captured when the NEXT bar opens (appendCount > 0).
      // Bid/ask table already captures live prices for the watchlist.
      //
      // v1.471: advance `timestamp` via a row-level UPDATE so the terminal's
      // gap detector sees the key as "freshly checked" and stops re-flagging
      // it as stale. Without this touch, gap-fill requests from the terminal
      // for weekend/holiday stocks loop forever — BCW returns existingCount,
      // the caller clears the gap-fill slot, but the row's timestamp never
      // advances, so next cycle the terminal re-adds the gap-fill request.
      if(g_stmtBarTouchTs != INVALID_HANDLE)
      {
         DatabaseReset(g_stmtBarTouchTs);
         if(DatabaseBind(g_stmtBarTouchTs, 0, (long)TimeCurrent()) &&
            DatabaseBind(g_stmtBarTouchTs, 1, key))
         {
            DatabaseRead(g_stmtBarTouchTs);
            DatabaseReset(g_stmtBarTouchTs);
         }
      }
      return existingCount;
   }

   int mergedCount = existingCount + appendCount;

   // Bar cap: if exceeded, do full re-export capped at MAX_BARS_PER_KEY
   if(mergedCount > MAX_BARS_PER_KEY)
      return ExportSymbolTF(symbol, tf, MAX_BARS_PER_KEY, tfStr);

   // Build merged blob in memory
   int mergedBytes = 8 + mergedCount * 48;
   uchar mergedBlob[];
   ArrayResize(mergedBlob, mergedBytes);
   int existingBytes = 8 + existingCount * 48;
   ArrayCopy(mergedBlob, existingBlob, 0, 0, existingBytes);

   // Update count in header
   mergedBlob[4] = (uchar)(mergedCount & 0xFF);
   mergedBlob[5] = (uchar)((mergedCount >> 8) & 0xFF);
   mergedBlob[6] = (uchar)((mergedCount >> 16) & 0xFF);
   mergedBlob[7] = (uchar)((mergedCount >> 24) & 0xFF);

   // Append new bars
   for(int i = 0; i < appendCount; i++)
   {
      int off = existingBytes + i * 48;
      int ri = newStart + i;
      bc.l = (long)newRates[ri].time * 1000;
      WriteRaw8(mergedBlob, off, bc);
      bc.d = newRates[ri].open;   WriteRaw8(mergedBlob, off + 8, bc);
      bc.d = newRates[ri].high;   WriteRaw8(mergedBlob, off + 16, bc);
      bc.d = newRates[ri].low;    WriteRaw8(mergedBlob, off + 24, bc);
      bc.d = newRates[ri].close;  WriteRaw8(mergedBlob, off + 32, bc);
      bc.d = (double)newRates[ri].tick_volume;
      WriteRaw8(mergedBlob, off + 40, bc);
   }

   // Write merged blob
   DatabaseReset(g_stmtBarInsert);
   if(!DatabaseBind(g_stmtBarInsert, 0, key) ||
      !DatabaseBindArray(g_stmtBarInsert, 1, mergedBlob) ||
      !DatabaseBind(g_stmtBarInsert, 2, (long)TimeCurrent()) ||
      !DatabaseBind(g_stmtBarInsert, 3, (long)mergedCount))
   {
      return 0;
   }
   DatabaseRead(g_stmtBarInsert);

   static int incrLogCount = 0;
   if(incrLogCount < 10)
   {
      PrintFormat("  INCR OK: %s — +%d bars (total %d)", key, appendCount, mergedCount);
      incrLogCount++;
   }

   return mergedCount;
}

int ExportSymbolTF(string symbol, ENUM_TIMEFRAMES tf, int maxBars, const string &tfStr)
{
   MqlRates rates[];
   ArraySetAsSeries(rates, false);

   // SymbolSelect already called once per symbol in ExportAll() — no need to repeat here

   int copied;
   if(maxBars <= 0)
      copied = CopyRates(symbol, tf, D'1970.01.01 00:00', TimeCurrent(), rates);
   else
      copied = CopyRates(symbol, tf, 0, maxBars, rates);
   if(copied <= 0)
   {
      static int copyFailLog = 0;
      if(copyFailLog < 20)
      {
         PrintFormat("  CopyRates FAIL: %s %s maxBars=%d err=%d",
            symbol, tfStr, maxBars, GetLastError());
         copyFailLog++;
      }
      return 0;
   }

   // Pack bars into TTBR binary format (48 bytes/bar, zero string overhead)
   uchar buffer[];
   PackBarsBinary(rates, 0, copied, buffer);

   string key = "mt5:" + symbol + ":" + tfStr;

   // Use pre-prepared statement (reset+rebind is ~10× faster than prepare+finalize per call)
   int req = g_stmtBarInsert;
   if(req == INVALID_HANDLE)
   {
      PrintFormat("  bar insert stmt not available: %s", key);
      return 0;
   }

   DatabaseReset(req);
   if(!DatabaseBind(req, 0, key) ||
      !DatabaseBindArray(req, 1, buffer) ||
      !DatabaseBind(req, 2, (long)TimeCurrent()) ||
      !DatabaseBind(req, 3, (long)copied))
   {
      PrintFormat("  bind failed: %s (err %d, bytes=%d)", key, GetLastError(), ArraySize(buffer));
      return 0;
   }

   DatabaseRead(req);

   static int writeOkLog = 0;
   if(writeOkLog < 5)
   {
      PrintFormat("  DB WRITE OK: %s — %d bars, %d bytes (binary)", key, copied, ArraySize(buffer));
      writeOkLog++;
   }

   return copied;
}

// v1.460: Purge bar rows not refreshed in `retentionSec` seconds. Metadata
// rows (mt5:__HEARTBEAT__, __SYMBOLS__, __SPECS__, __SERVER__) are preserved
// via `NOT LIKE 'mt5:\_\_%' ESCAPE '\'` — SQLite's LIKE treats '_' as a
// single-char wildcard by default, so the underscore-escape is mandatory.
//
// Same-connection DELETE avoids the cross-process lock cascade that forced
// the terminal-side cleanup revert (d9ca7f5: terminal DELETE+VACUUM on the
// source DB fought BCW's long-lived prepared statements and zeroed exports
// across 797 symbols). Caller runs VACUUM immediately after this so the
// freed pages return to the OS in one pass.
//
// Active symbols refresh timestamp on every new-bar write via
// IncrementalExportSymbolTF. IncrementalExportSymbolTF's appendCount==0
// path skips the DB write, so weekend/holiday gaps leave timestamp stale —
// that's why retention is 14 days (covers long market closures without
// clipping live symbols).
void DeleteStaleBarCacheRows(long retentionSec)
{
   if(g_db == INVALID_HANDLE) return;
   long cutoff = (long)TimeCurrent() - retentionSec;
   if(cutoff <= 0) return;

   long toDelete = 0;
   string countSql = StringFormat(
      "SELECT COUNT(*) FROM bar_cache "
      "WHERE timestamp > 0 AND timestamp < %I64d "
      "  AND key LIKE 'mt5:%%' "
      "  AND key NOT LIKE 'mt5:\\_\\_%%' ESCAPE '\\'",
      cutoff);
   int cq = DatabasePrepare(g_db, countSql);
   if(cq != INVALID_HANDLE)
   {
      if(DatabaseRead(cq))
         DatabaseColumnLong(cq, 0, toDelete);
      DatabaseFinalize(cq);
   }

   if(toDelete <= 0) return;

   string delSql = StringFormat(
      "DELETE FROM bar_cache "
      "WHERE timestamp > 0 AND timestamp < %I64d "
      "  AND key LIKE 'mt5:%%' "
      "  AND key NOT LIKE 'mt5:\\_\\_%%' ESCAPE '\\'",
      cutoff);

   uint t0 = GetTickCount();
   if(!DatabaseExecute(g_db, delSql))
   {
      PrintFormat("BarCacheWriter: stale-row cleanup failed (err %d)", GetLastError());
      return;
   }
   PrintFormat("BarCacheWriter: cleaned %I64d stale rows (cutoff=%s, %d ms)",
      toDelete, TimeToString((datetime)cutoff), GetTickCount() - t0);
}

void OnDeinit(const int reason)
{
   EventKillTimer();
   // v1.443: Final flush of any dirty track entries before shutdown so we
   // don't lose the last batch of updates from in-memory state.
   int finalFlushed = FlushDirtyTrackTimes();
   if(finalFlushed > 0)
      PrintFormat("BarCacheWriter: final flush persisted %d dirty track rows", finalFlushed);
   if(g_stmtBarInsert != INVALID_HANDLE)   { DatabaseFinalize(g_stmtBarInsert);   g_stmtBarInsert = INVALID_HANDLE; }
   if(g_stmtBarRead != INVALID_HANDLE)     { DatabaseFinalize(g_stmtBarRead);     g_stmtBarRead = INVALID_HANDLE; }
   if(g_stmtTrackInsert != INVALID_HANDLE) { DatabaseFinalize(g_stmtTrackInsert); g_stmtTrackInsert = INVALID_HANDLE; }
   if(g_stmtQuoteInsert != INVALID_HANDLE) { DatabaseFinalize(g_stmtQuoteInsert); g_stmtQuoteInsert = INVALID_HANDLE; }
   if(g_stmtMetaInsert != INVALID_HANDLE)  { DatabaseFinalize(g_stmtMetaInsert);  g_stmtMetaInsert = INVALID_HANDLE; }
   if(g_stmtBarTouchTs != INVALID_HANDLE)  { DatabaseFinalize(g_stmtBarTouchTs);  g_stmtBarTouchTs = INVALID_HANDLE; }
   if(g_db != INVALID_HANDLE)
   {
      DatabaseClose(g_db);
      g_db = INVALID_HANDLE;
   }
   Print("BarCacheWriter: stopped");
}
