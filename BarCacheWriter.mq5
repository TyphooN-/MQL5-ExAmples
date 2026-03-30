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
#property version   "1.426"
#property description "Writes bar data (TTBR binary) + symbol specs + live bid/ask to SQLite."
#property description "v1.426: Incremental sync — reads last 10 bars, appends only new (10000x faster)."
#property description "v1.424: Cap all timeframes at 100K bars. Forex filtering by server type."
#property description "v1.422: Forex filtering — only export forex on CFD server (detected by USDMXN)."
#property description "v1.418: Live bid/ask sync for all symbols every tick (INSERT OR REPLACE, flat table)."
#property description "v1.414: Full history export (MaxBarsForTF) on every sync — no truncation."
#property strict


input int    UpdateIntervalSec = 30;     // Update interval (seconds)
input bool   MarketWatchOnly   = false;  // false = ALL broker symbols
input int    BatchSize         = 20;     // Symbols per SQLite transaction (batching reduces fsync overhead)
input bool   ForceReExport     = false;  // true = clear tracking, re-export all history once

int g_db = INVALID_HANDLE;
string g_accountTag = "";
int g_stmtBarInsert = INVALID_HANDLE;   // Pre-prepared INSERT OR REPLACE for bar_cache
int g_stmtTrackInsert = INVALID_HANDLE; // Pre-prepared INSERT OR REPLACE for bar_track
int g_stmtQuoteInsert = INVALID_HANDLE; // Pre-prepared INSERT OR REPLACE for bid_ask

// Track last bar time per symbol:TF to skip unchanged data
// Uses sorted arrays + binary search for O(log n) lookup instead of O(n) linear scan
// With 851 symbols × 9 TFs = 7,659 keys, this matters every tick
string g_trackKeys[];
datetime g_trackTimes[];
int g_trackFails[];  // Consecutive export failures per key — give up after MAX_CONSEC_FAILS
int g_trackCount = 0;
bool g_trackSorted = true; // false when new keys appended, triggers re-sort
#define MAX_CONSEC_FAILS 10  // Give up on a symbol/TF after this many consecutive failures
#define FAIL_SENTINEL  1     // Sentinel timestamp: marks "permanently failed, stop retrying"


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

bool IsForexSymbol(string symbol)
{
   // Common forex pairs — 6-char symbols like EURUSD, GBPJPY, etc.
   // Also catches exotics like USDMXN, USDZAR, etc.
   int len = StringLen(symbol);
   if(len != 6) return false;
   string majors[] = {"USD","EUR","GBP","JPY","CHF","AUD","NZD","CAD","SEK","NOK","MXN","ZAR","TRY","PLN","HUF","CZK","SGD","HKD","DKK"};
   string base = StringSubstr(symbol, 0, 3);
   string quote = StringSubstr(symbol, 3, 3);
   bool baseIsCcy = false, quoteIsCcy = false;
   for(int i = 0; i < ArraySize(majors); i++)
   {
      if(base == majors[i]) baseIsCcy = true;
      if(quote == majors[i]) quoteIsCcy = true;
   }
   return (baseIsCcy && quoteIsCcy);
}

// Timeframes: MN1 first (higher TFs are smaller, export fast)
ENUM_TIMEFRAMES g_timeframes[] = {
   PERIOD_MN1, PERIOD_W1, PERIOD_D1, PERIOD_H4, PERIOD_H1,
   PERIOD_M30, PERIOD_M15, PERIOD_M5, PERIOD_M1
};
// Pre-cached TF strings — avoids 7,659 switch evaluations per tick
string g_tfStrings[9];

// Max bars per timeframe — 0 = ALL available history from server (no limit)
// Uses CopyRates(symbol, tf, D'1970.01.01', TimeCurrent(), rates) for full history
int MaxBarsForTF(ENUM_TIMEFRAMES tf)
{
   // Cap all timeframes at 100,000 bars to prevent OOM and export hangs.
   // 100K bars covers: M1=~69 days, M5=~347 days, M15=~1041 days,
   // M30=~2083 days, H1=~11.4 years, H4=~45 years, D1/W1/MN1=all history
   return 100000;
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

// Sort tracking arrays by key (insertion sort — only runs when new keys added)
void SortTrackArrays()
{
   // Simple insertion sort — runs once after initial population, then never again
   for(int i = 1; i < g_trackCount; i++)
   {
      string tmpKey = g_trackKeys[i];
      datetime tmpTime = g_trackTimes[i];
      int tmpFails = g_trackFails[i];
      int j = i - 1;
      while(j >= 0 && StringCompare(g_trackKeys[j], tmpKey) > 0)
      {
         g_trackKeys[j + 1] = g_trackKeys[j];
         g_trackTimes[j + 1] = g_trackTimes[j];
         g_trackFails[j + 1] = g_trackFails[j];
         j--;
      }
      g_trackKeys[j + 1] = tmpKey;
      g_trackTimes[j + 1] = tmpTime;
      g_trackFails[j + 1] = tmpFails;
   }
   g_trackSorted = true;
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
   g_trackKeys[g_trackCount - 1] = key;
   g_trackTimes[g_trackCount - 1] = 0;
   g_trackFails[g_trackCount - 1] = 0;
   g_trackSorted = false;
   return g_trackCount - 1;
}

// Persist last bar time to DB so we survive restarts
void SaveTrackTime(string trackKey, datetime barTime)
{
   if(g_stmtTrackInsert == INVALID_HANDLE) return;
   DatabaseReset(g_stmtTrackInsert);
   DatabaseBind(g_stmtTrackInsert, 0, trackKey);
   DatabaseBind(g_stmtTrackInsert, 1, (long)barTime);
   DatabaseRead(g_stmtTrackInsert);
}

// Load existing key→timestamp mapping from DB so we don't re-export after restart
void LoadTrackingFromDB()
{
   if(g_db == INVALID_HANDLE) return;

   int req = DatabasePrepare(g_db,
      "SELECT key, last_bar_time FROM bar_track");
   if(req == INVALID_HANDLE) return;

   int loaded = 0;
   while(DatabaseRead(req))
   {
      string key = "";
      long ts = 0;
      DatabaseColumnText(req, 0, key);
      DatabaseColumnLong(req, 1, ts);

      if(ts <= 0) continue;
      int idx = GetTrackIndex(key);
      g_trackTimes[idx] = (datetime)ts;
      loaded++;
   }
   DatabaseFinalize(req);

   if(loaded > 0)
   {
      SortTrackArrays();
      PrintFormat("BarCacheWriter: restored %d cached keys from DB (skip re-export on restart)", loaded);
   }
}

int OnInit()
{
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

   // Use DELETE journal mode — WAL shared memory doesn't work across Wine/Linux boundary
   DatabaseExecute(g_db, "PRAGMA journal_mode=DELETE");
   DatabaseExecute(g_db, "PRAGMA synchronous=NORMAL");
   DatabaseExecute(g_db, "PRAGMA cache_size=-8000"); // 8MB page cache
   DatabaseExecute(g_db, "PRAGMA busy_timeout=5000"); // Wait up to 5s for lock instead of failing

   // Pre-prepare statements — avoids re-parsing SQL on every write (851 symbols × 9 TFs per tick)
   g_stmtBarInsert = DatabasePrepare(g_db,
      "INSERT OR REPLACE INTO bar_cache (key, data, timestamp, bar_count) VALUES (?1, ?2, ?3, ?4)");
   if(g_stmtBarInsert == INVALID_HANDLE)
      PrintFormat("BarCacheWriter: WARN — failed to prepare bar insert stmt (err %d)", GetLastError());

   g_stmtTrackInsert = DatabasePrepare(g_db,
      "INSERT OR REPLACE INTO bar_track (key, last_bar_time) VALUES (?1, ?2)");
   if(g_stmtTrackInsert == INVALID_HANDLE)
      PrintFormat("BarCacheWriter: WARN — failed to prepare track insert stmt (err %d)", GetLastError());

   g_stmtQuoteInsert = DatabasePrepare(g_db,
      "INSERT OR REPLACE INTO bid_ask (symbol, bid, ask, spread, timestamp) VALUES (?1, ?2, ?3, ?4, ?5)");
   if(g_stmtQuoteInsert == INVALID_HANDLE)
      PrintFormat("BarCacheWriter: WARN — failed to prepare quote insert stmt (err %d)", GetLastError());

   // Cache TF strings once — eliminates switch evaluation per symbol×TF per tick
   for(int t = 0; t < ArraySize(g_timeframes); t++)
      g_tfStrings[t] = TFToStr(g_timeframes[t]);

   int initSymCount = SymbolsTotal(MarketWatchOnly);

   long acct = AccountInfoInteger(ACCOUNT_LOGIN);
   g_accountTag = IntegerToString(acct);

   // Detect CFD server by checking if USDMXN exists — only CFD servers have exotic forex
   g_isCFDServer = (SymbolInfoInteger("USDMXN", SYMBOL_EXIST) == 1);
   PrintFormat("BarCacheWriter: server type = %s (USDMXN %s)",
      g_isCFDServer ? "CFD (forex enabled)" : "Crypto/Futures (forex SKIPPED)",
      g_isCFDServer ? "found" : "not found");

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
   }
   else
      LoadTrackingFromDB();

   PrintFormat("BarCacheWriter v1.426: %s symbols(%d), %ds interval, batch=%d, %d cached keys, incremental sync, forex=%s",
      MarketWatchOnly ? "MW" : "ALL", initSymCount, UpdateIntervalSec, BatchSize, g_trackCount,
      g_isCFDServer ? "ENABLED" : "SKIPPED");

   EventSetTimer(UpdateIntervalSec);
   return INIT_SUCCEEDED;
}

void OnTimer() { ExportAll(); }

void ExportAll()
{
   if(g_db == INVALID_HANDLE) return;

   uint tickStart = GetTickCount();
   int symCount = SymbolsTotal(MarketWatchOnly);
   static datetime lastTickLog = 0;
   if(TimeCurrent() - lastTickLog > 300)
   {
      PrintFormat("BarCacheWriter: tick start — %d symbols", symCount);
      lastTickLog = TimeCurrent();
   }

   int exported = 0, skipped = 0, totalBars = 0;

   // Write metadata only every 5 minutes (not every tick) — avoids unnecessary DB writes
   // that change file mtime and prevent the Rust sync's fast-path mtime check
   static datetime lastMetaWrite = 0;
   if(lastMetaWrite == 0 || TimeCurrent() - lastMetaWrite >= 300)
   {
      SafeBegin();
      WriteSymbolList(symCount);
      WriteSymbolSpecs(symCount);
      SafeCommit();
      lastMetaWrite = TimeCurrent();
   }
   uint afterMeta = GetTickCount();
   if(afterMeta - tickStart > 1000)
      PrintFormat("  metadata took %d ms", afterMeta - tickStart);

   // Live bid/ask sync — all symbols, every tick, flat table (895 rows max)
   if(g_stmtQuoteInsert != INVALID_HANDLE)
   {
      SafeBegin();
      long now = (long)TimeCurrent();
      int quoteCount = 0;
      for(int q = 0; q < symCount; q++)
      {
         string qSym = SymbolName(q, MarketWatchOnly);
         if(StringLen(qSym) == 0) continue;
         double bid = SymbolInfoDouble(qSym, SYMBOL_BID);
         double ask = SymbolInfoDouble(qSym, SYMBOL_ASK);
         if(bid <= 0 && ask <= 0) continue; // no quote data
         double spread = (ask > 0 && bid > 0) ? (ask - bid) : 0;
         DatabaseReset(g_stmtQuoteInsert);
         DatabaseBind(g_stmtQuoteInsert, 0, qSym);
         DatabaseBind(g_stmtQuoteInsert, 1, bid);
         DatabaseBind(g_stmtQuoteInsert, 2, ask);
         DatabaseBind(g_stmtQuoteInsert, 3, spread);
         DatabaseBind(g_stmtQuoteInsert, 4, now);
         if(DatabaseRead(g_stmtQuoteInsert)) quoteCount++;
      }
      SafeCommit();
   }

   int batchCount = 0;  // symbols in current transaction batch
   bool inTxn = false;

   for(int i = 0; i < symCount; i++)
   {
      string symbol = SymbolName(i, MarketWatchOnly);
      if(StringLen(symbol) == 0) continue;

      // Skip forex symbols on non-CFD servers (crypto/futures don't need EURUSD etc.)
      if(!g_isCFDServer && IsForexSymbol(symbol)) continue;

      // Select once per symbol — covers all TFs (was duplicated inside ExportSymbolTF)
      SymbolSelect(symbol, true);

      // Batched transactions — group BatchSize symbols per BEGIN/COMMIT to reduce fsync overhead
      if(!inTxn)
      {
         SafeBegin();
         inTxn = true;
         batchCount = 0;
      }

      for(int tf = 0; tf < ArraySize(g_timeframes); tf++)
      {
         string trackKey = symbol + ":" + g_tfStrings[tf];
         int idx = GetTrackIndex(trackKey);

         // Single CopyRates call per symbol/TF — reuse for both change detection and tracking
         MqlRates lastRate[];
         int gotLast = CopyRates(symbol, g_timeframes[tf], 0, 1, lastRate);

         // Never synced — try to export whatever is locally available (non-blocking)
         if(g_trackTimes[idx] == 0)
         {
            // Give up after MAX_CONSEC_FAILS — broker doesn't have this history
            if(g_trackFails[idx] >= MAX_CONSEC_FAILS)
            {
               skipped++;
               continue;
            }
            // Fast pre-check: if broker has zero bars locally, skip without wasting a retry slot
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
            // Unlimited: maxBars=0 fetches ALL available history from server
            // Monitor MT5 memory if OOM occurs on M1 with millions of bars
            int bars = ExportSymbolTF(symbol, g_timeframes[tf], MaxBarsForTF(g_timeframes[tf]));

            if(bars > 0)
            {
               exported++;
               totalBars += bars;
               g_trackFails[idx] = 0; // Reset on success
               if(gotLast == 1)
               {
                  g_trackTimes[idx] = lastRate[0].time;
                  SaveTrackTime(trackKey, lastRate[0].time);
               }
            }
            else
            {
               g_trackFails[idx]++;
               if(g_trackFails[idx] >= MAX_CONSEC_FAILS)
               {
                  // Mark as permanently failed — stop retrying
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

         // Incremental sync: only fetch bars SINCE last sync, merge with existing DB blob.
         // Instead of re-exporting 100K bars, fetches ~200 recent bars and appends to existing data.
         // Preserves full history while only transferring the delta.
         int bars = IncrementalExportSymbolTF(symbol, g_timeframes[tf], g_trackTimes[idx]);
         if(bars > 0)
         {
            exported++;
            totalBars += bars;
            if(gotLast == 1)
            {
               g_trackTimes[idx] = lastRate[0].time;
               SaveTrackTime(trackKey, lastRate[0].time);
            }
         }
      }

      batchCount++;
      if(batchCount >= BatchSize)
      {
         SafeCommit();
         inTxn = false;
      }
   }

   // Commit any remaining symbols in the last partial batch
   if(inTxn) SafeCommit();

   // Sort tracking arrays after population phase for O(log n) lookups on subsequent ticks
   if(!g_trackSorted && g_trackCount > 0)
      SortTrackArrays();

   // Count TF slots still pending (never successfully exported, excluding permanently failed)
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

   if(first || TimeCurrent() - lastLog > 300 || failCount <= 3)
   {
      uint elapsed = GetTickCount() - tickStart;
      PrintFormat("BarCacheWriter: %d exported, %d skipped, %d bars | %d pending | %dms",
         exported, skipped, totalBars, pendingSlots, elapsed);

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
}

void WriteSymbolList(int symCount)
{
   string csv = "";
   for(int i = 0; i < symCount; i++)
   {
      string s = SymbolName(i, MarketWatchOnly);
      if(StringLen(s) == 0) continue;
      if(StringLen(csv) > 0) csv += ",";
      csv += s;
   }

   int req = DatabasePrepare(g_db,
      "INSERT OR REPLACE INTO bar_cache (key, data, timestamp, bar_count) VALUES (?1, ?2, ?3, ?4)");
   if(req != INVALID_HANDLE)
   {
      DatabaseBind(req, 0, "mt5:__SYMBOLS__:" + g_accountTag);
      DatabaseBind(req, 1, "[\"" + StringReplace2(csv, ",", "\",\"") + "\"]");
      DatabaseBind(req, 2, (long)TimeCurrent());
      DatabaseBind(req, 3, (long)symCount);
      DatabaseRead(req);
      DatabaseFinalize(req);
   }

   // Store broker/server identity — TyphooN-Terminal reads this for data source badge
   int srvReq = DatabasePrepare(g_db,
      "INSERT OR REPLACE INTO bar_cache (key, data, timestamp, bar_count) VALUES (?1, ?2, ?3, ?4)");
   if(srvReq != INVALID_HANDLE)
   {
      string server = AccountInfoString(ACCOUNT_SERVER);
      string company = AccountInfoString(ACCOUNT_COMPANY);
      string meta = "{\"server\":\"" + server + "\",\"company\":\"" + company + "\"}";
      DatabaseBind(srvReq, 0, "mt5:__SERVER__:" + g_accountTag);
      DatabaseBind(srvReq, 1, meta);
      DatabaseBind(srvReq, 2, (long)TimeCurrent());
      DatabaseBind(srvReq, 3, 0);
      DatabaseRead(srvReq);
      DatabaseFinalize(srvReq);
   }
}

void WriteSymbolSpecs(int symCount)
{
   // Export symbol specs as CSV rows: one line per symbol, compact format
   // TyphooN-Terminal does all heavy calculations (VaR, ATR, risk) — we just export raw specs
   // Format: Symbol,SectorName,IndustryName,TradeMode,SwapLong,SwapShort,Spread,
   //         VolumeMin,VolumeMax,VolumeStep,ContractSize,TickSize,TickValue,
   //         Digits,MarginInitial,MarginMaintenance,BaseCurrency,QuoteCurrency,Description
   string csv = "";
   int count = 0;

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

      // Sanitize description (remove commas/newlines that would break CSV)
      StringReplace(desc, ",", ";");
      StringReplace(desc, "\n", " ");
      StringReplace(desc, "\r", "");

      csv += s + ","
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
           + desc + "\n";
      count++;
   }

   int req = DatabasePrepare(g_db,
      "INSERT OR REPLACE INTO bar_cache (key, data, timestamp, bar_count) VALUES (?1, ?2, ?3, ?4)");
   if(req != INVALID_HANDLE)
   {
      DatabaseBind(req, 0, "mt5:__SPECS__:" + g_accountTag);
      DatabaseBind(req, 1, csv);
      DatabaseBind(req, 2, (long)TimeCurrent());
      DatabaseBind(req, 3, (long)count);
      DatabaseRead(req);
      DatabaseFinalize(req);
   }
}

// StringReplace that returns the new string (MQL5's StringReplace modifies in-place)
string StringReplace2(string src, string find, string replace)
{
   string result = src;
   StringReplace(result, find, replace);
   return result;
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

int IncrementalExportSymbolTF(string symbol, ENUM_TIMEFRAMES tf, datetime lastSyncTime)
{
   // Dynamic fetch: calculate exactly how many bars elapsed since last sync.
   // E.g., last sync was 3 H1 bars ago → fetch 5 (3 + 2 overlap for safety).
   // Minimum 3 (current bar + previous + safety), maximum 200 (cap for edge cases).
   int tfSeconds = PeriodSeconds(tf);
   int elapsed = (int)(TimeCurrent() - lastSyncTime);
   int estimatedNewBars = (tfSeconds > 0) ? (elapsed / tfSeconds) + 2 : 10;
   int fetchCount = MathMax(3, MathMin(estimatedNewBars, 200));

   MqlRates newRates[];
   ArraySetAsSeries(newRates, false);
   int newCopied = CopyRates(symbol, tf, 0, fetchCount, newRates);
   if(newCopied <= 0) return 0;

   string key = "mt5:" + symbol + ":" + TFToStr(tf);

   // Read existing blob from DB
   int readStmt = DatabasePrepare(g_db, "SELECT data FROM bar_cache WHERE key = ?1");
   if(readStmt == INVALID_HANDLE) return ExportSymbolTF(symbol, tf, 0); // fallback to full export

   DatabaseBind(readStmt, 0, key);
   uchar existingBlob[];
   bool hasExisting = false;
   if(DatabaseRead(readStmt))
   {
      DatabaseColumnBlob(readStmt, 0, existingBlob);
      hasExisting = (ArraySize(existingBlob) >= 8);
   }
   DatabaseFinalize(readStmt);

   if(!hasExisting)
   {
      // No existing data — do full export instead
      return ExportSymbolTF(symbol, tf, 0);
   }

   // Verify TTBR magic
   if(existingBlob[0] != 'T' || existingBlob[1] != 'T' || existingBlob[2] != 'B' || existingBlob[3] != 'R')
   {
      return ExportSymbolTF(symbol, tf, 0); // corrupt, full re-export
   }

   // Unpack existing bar count
   int existingCount = (int)existingBlob[4]
                     | ((int)existingBlob[5] << 8)
                     | ((int)existingBlob[6] << 16)
                     | ((int)existingBlob[7] << 24);

   // Find the timestamp of the last existing bar
   if(existingCount <= 0 || ArraySize(existingBlob) < 8 + existingCount * 48)
      return ExportSymbolTF(symbol, tf, 0); // corrupt

   ByteConv bc;
   int lastOff = 8 + (existingCount - 1) * 48;
   ReadRaw8(existingBlob, lastOff, bc);
   long lastExistingTsMs = bc.l;

   // Find where new bars start (skip bars that overlap with existing data)
   int newStart = 0;
   for(int i = 0; i < newCopied; i++)
   {
      long barTsMs = (long)newRates[i].time * 1000;
      if(barTsMs > lastExistingTsMs)
      {
         newStart = i;
         break;
      }
      // Also update the last existing bar if timestamps match (bar may have updated)
      if(barTsMs == lastExistingTsMs)
      {
         // Overwrite last bar in existing blob
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
      // No truly new bars — just update the last bar (close price may have changed)
      // Write existing blob back (last bar was updated in-place above)
      DatabaseReset(g_stmtBarInsert);
      if(DatabaseBind(g_stmtBarInsert, 0, key) &&
         DatabaseBindArray(g_stmtBarInsert, 1, existingBlob) &&
         DatabaseBind(g_stmtBarInsert, 2, (long)TimeCurrent()) &&
         DatabaseBind(g_stmtBarInsert, 3, (long)existingCount))
      {
         DatabaseRead(g_stmtBarInsert);
      }
      return existingCount;
   }

   // Build merged blob: existing bars + new bars appended
   int mergedCount = existingCount + appendCount;
   int mergedBytes = 8 + mergedCount * 48;
   uchar mergedBlob[];
   ArrayResize(mergedBlob, mergedBytes);

   // Copy existing header + bars
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

   // Write merged blob to DB
   DatabaseReset(g_stmtBarInsert);
   if(!DatabaseBind(g_stmtBarInsert, 0, key) ||
      !DatabaseBindArray(g_stmtBarInsert, 1, mergedBlob) ||
      !DatabaseBind(g_stmtBarInsert, 2, (long)TimeCurrent()) ||
      !DatabaseBind(g_stmtBarInsert, 3, (long)mergedCount))
   {
      PrintFormat("  IncrementalExport bind failed: %s (err %d)", key, GetLastError());
      return 0;
   }
   DatabaseRead(g_stmtBarInsert);

   static int incrLogCount = 0;
   if(incrLogCount < 10)
   {
      PrintFormat("  INCR OK: %s — +%d bars (total %d, %d bytes)", key, appendCount, mergedCount, mergedBytes);
      incrLogCount++;
   }

   return mergedCount;
}

int ExportSymbolTF(string symbol, ENUM_TIMEFRAMES tf, int maxBars)
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
            symbol, TFToStr(tf), maxBars, GetLastError());
         copyFailLog++;
      }
      return 0;
   }

   // Pack bars into TTBR binary format (48 bytes/bar, zero string overhead)
   uchar buffer[];
   PackBarsBinary(rates, 0, copied, buffer);

   string key = "mt5:" + symbol + ":" + TFToStr(tf);

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

void OnDeinit(const int reason)
{
   EventKillTimer();
   if(g_stmtBarInsert != INVALID_HANDLE) { DatabaseFinalize(g_stmtBarInsert); g_stmtBarInsert = INVALID_HANDLE; }
   if(g_stmtTrackInsert != INVALID_HANDLE) { DatabaseFinalize(g_stmtTrackInsert); g_stmtTrackInsert = INVALID_HANDLE; }
   if(g_stmtQuoteInsert != INVALID_HANDLE) { DatabaseFinalize(g_stmtQuoteInsert); g_stmtQuoteInsert = INVALID_HANDLE; }
   if(g_db != INVALID_HANDLE)
   {
      DatabaseClose(g_db);
      g_db = INVALID_HANDLE;
   }
   Print("BarCacheWriter: stopped");
}
