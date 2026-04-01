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
#property version   "1.429"
#property description "Writes bar data (TTBR binary) + symbol specs + live bid/ask to SQLite."
#property description "v1.429: Resource management — page_size 4096, periodic checkpoint, batch sleep, bar cap."
#property description "v1.428: CAST(?N AS BLOB) in SQL UPDATEs — MQL5 DatabaseBindArray may bind uchar[] as TEXT."
#property description "v1.427: SQL BLOB append — no full blob round-trip. Only delta bytes cross MQL5/SQLite boundary."
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
int g_stmtBarInsert = INVALID_HANDLE;       // Pre-prepared INSERT OR REPLACE for bar_cache (full export)
int g_stmtTrackInsert = INVALID_HANDLE;     // Pre-prepared INSERT OR REPLACE for bar_track
int g_stmtQuoteInsert = INVALID_HANDLE;     // Pre-prepared INSERT OR REPLACE for bid_ask
int g_stmtBarCountRead = INVALID_HANDLE;    // SELECT bar_count FROM bar_cache WHERE key=?1 (index-only, no blob read)
// SQL UPDATE statements that manipulate the BLOB server-side — only the 48-byte delta
// crosses the MQL5/SQLite boundary instead of the full 4.8MB blob every call.
// SUBSTR is 1-indexed in SQLite. Bar data layout: [4 magic][4 count LE][N*48 bars]
//   All bars except last: SUBSTR(data, 9, LENGTH(data)-56)   (from byte 9, length=total-8header-48lastbar)
//   Last bar only:        SUBSTR(data, LENGTH(data)-47, 48)  (last 48 bytes)
int g_stmtUpdateLastBar = INVALID_HANDLE;   // update last bar in-place (forming bar close changes)
int g_stmtReplaceLastAndAppend = INVALID_HANDLE; // replace last bar + append new bars
int g_stmtAppendOnly = INVALID_HANDLE;      // append new bars without touching last bar

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
#define MAX_BARS_PER_KEY 100000  // Hard cap: trim oldest bars during incremental merge if exceeded
int g_cycleCount = 0;           // Counts ExportAll() calls for periodic maintenance


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

// Pack a single MqlRates bar into a 48-byte TTBR bar record.
// buf must be pre-allocated to at least off+48 bytes.
void PackSingleBar(uchar &buf[], int off, const MqlRates &r)
{
   ByteConv bc;
   bc.l = (long)r.time * 1000;  WriteRaw8(buf, off,      bc);
   bc.d = r.open;               WriteRaw8(buf, off +  8, bc);
   bc.d = r.high;               WriteRaw8(buf, off + 16, bc);
   bc.d = r.low;                WriteRaw8(buf, off + 24, bc);
   bc.d = r.close;              WriteRaw8(buf, off + 32, bc);
   bc.d = (double)r.tick_volume; WriteRaw8(buf, off + 40, bc);
}

// Build a BLOB of count*48 bytes from rates[start .. start+count-1].
// Used for the "new bars to append" payload in SQL UPDATE statements.
void PackBarsBlob(const MqlRates &rates[], int start, int count, uchar &buf[])
{
   ArrayResize(buf, count * 48);
   for(int i = 0; i < count; i++)
      PackSingleBar(buf, i * 48, rates[start + i]);
}

// Encode an int as 4-byte little-endian blob (for TTBR bar count field).
void EncodeLE4(int v, uchar &buf[])
{
   ArrayResize(buf, 4);
   buf[0] = (uchar)(v & 0xFF);
   buf[1] = (uchar)((v >>  8) & 0xFF);
   buf[2] = (uchar)((v >> 16) & 0xFF);
   buf[3] = (uchar)((v >> 24) & 0xFF);
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

   // Use DELETE journal mode — WAL shared memory doesn't work across Wine/Linux boundary.
   // page_size=4096: default but explicit — matches OS page size for aligned I/O.
   // cache_size=-4000: 4MB page cache (reduced from 8MB — less Wine memory pressure).
   // busy_timeout=5000: retry for 5s on lock instead of failing immediately.
   DatabaseExecute(g_db, "PRAGMA journal_mode=DELETE");
   DatabaseExecute(g_db, "PRAGMA synchronous=NORMAL");
   DatabaseExecute(g_db, "PRAGMA page_size=4096");
   DatabaseExecute(g_db, "PRAGMA cache_size=-4000");
   DatabaseExecute(g_db, "PRAGMA busy_timeout=5000");

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

   // Index-only read: bar_count is in the covering index, never touches the blob page.
   // Used to check existence and get current count before SQL BLOB UPDATE.
   g_stmtBarCountRead = DatabasePrepare(g_db,
      "SELECT bar_count FROM bar_cache WHERE key = ?1");
   if(g_stmtBarCountRead == INVALID_HANDLE)
      PrintFormat("BarCacheWriter: WARN — failed to prepare bar count read stmt (err %d)", GetLastError());

   // Update last bar in-place: replaces the final 48 bytes of the BLOB.
   // Used every cycle for each active symbol/TF to refresh the forming bar's close.
   // SQLite reads LENGTH() from the page header and writes only the changed tail bytes —
   // the rest of the blob is unmodified. Zero MQL5 memory for the existing bars.
   // CAST(?N AS BLOB) on every bound parameter: MQL5's DatabaseBindArray may bind
   // uchar[] as TEXT type. SQLite's || operator returns TEXT if either operand is TEXT,
   // corrupting binary BLOB data. CAST forces BLOB type regardless of binding affinity.
   g_stmtUpdateLastBar = DatabasePrepare(g_db,
      "UPDATE bar_cache SET data=SUBSTR(data,1,LENGTH(data)-48)||CAST(?1 AS BLOB),timestamp=?2 WHERE key=?3");
   if(g_stmtUpdateLastBar == INVALID_HANDLE)
      PrintFormat("BarCacheWriter: WARN — failed to prepare update-last-bar stmt (err %d)", GetLastError());

   // Replace last bar + append new bars.
   // Params: ?1=4-byte LE count, ?2=48-byte updated last bar, ?3=new bars blob, ?4=new count, ?5=ts, ?6=key
   // SUBSTR breakdown: magic(4) || newCount(4) || allBarsExceptLast || newLastBar || newBars
   g_stmtReplaceLastAndAppend = DatabasePrepare(g_db,
      "UPDATE bar_cache SET data=SUBSTR(data,1,4)||CAST(?1 AS BLOB)||SUBSTR(data,9,LENGTH(data)-56)||CAST(?2 AS BLOB)||CAST(?3 AS BLOB),bar_count=?4,timestamp=?5 WHERE key=?6");
   if(g_stmtReplaceLastAndAppend == INVALID_HANDLE)
      PrintFormat("BarCacheWriter: WARN — failed to prepare replace-last-and-append stmt (err %d)", GetLastError());

   // Append new bars only (no last-bar update — timestamps in new batch start past existing last).
   // Params: ?1=4-byte LE count, ?2=new bars blob, ?3=new count, ?4=ts, ?5=key
   g_stmtAppendOnly = DatabasePrepare(g_db,
      "UPDATE bar_cache SET data=SUBSTR(data,1,4)||CAST(?1 AS BLOB)||SUBSTR(data,9)||CAST(?2 AS BLOB),bar_count=?3,timestamp=?4 WHERE key=?5");
   if(g_stmtAppendOnly == INVALID_HANDLE)
      PrintFormat("BarCacheWriter: WARN — failed to prepare append-only stmt (err %d)", GetLastError());

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

   PrintFormat("BarCacheWriter v1.429: %s symbols(%d), %ds interval, batch=%d, %d cached keys, SQL BLOB append, forex=%s",
      MarketWatchOnly ? "MW" : "ALL", initSymCount, UpdateIntervalSec, BatchSize, g_trackCount,
      g_isCFDServer ? "ENABLED" : "SKIPPED");

   EventSetTimer(UpdateIntervalSec);
   return INIT_SUCCEEDED;
}

void OnTimer() { ExportAll(); }

void ExportAll()
{
   if(g_db == INVALID_HANDLE) return;

   g_cycleCount++;

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
         // Yield CPU between batches — prevents Wine from monopolizing resources.
         // 50ms sleep × (symCount/BatchSize) batches = ~2s total for 851 symbols.
         // Without this, the tight loop starves other Wine processes and the
         // SQLite page cache never gets a chance to flush to disk.
         Sleep(50);
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
// Bar count for the new header is read from the covering index (g_stmtBarCountRead) —
// an index-only scan that never touches the blob page.
int IncrementalExportSymbolTF(string symbol, ENUM_TIMEFRAMES tf, datetime lastSyncTime)
{
   // Dynamic fetch: bars elapsed since last sync + 2 overlap for safety.
   int tfSeconds = PeriodSeconds(tf);
   int elapsed = (int)(TimeCurrent() - lastSyncTime);
   int estimatedNewBars = (tfSeconds > 0) ? (elapsed / tfSeconds) + 2 : 10;
   int fetchCount = MathMax(3, MathMin(estimatedNewBars, 200));

   MqlRates newRates[];
   ArraySetAsSeries(newRates, false);
   int newCopied = CopyRates(symbol, tf, 0, fetchCount, newRates);
   if(newCopied <= 0) return 0;

   string key = "mt5:" + symbol + ":" + TFToStr(tf);

   // ── Step 1: read bar_count only (index-only scan, never loads blob page) ──
   if(g_stmtBarCountRead == INVALID_HANDLE) return ExportSymbolTF(symbol, tf, 0);
   DatabaseReset(g_stmtBarCountRead);
   DatabaseBind(g_stmtBarCountRead, 0, key);
   int existingCount = 0;
   if(!DatabaseRead(g_stmtBarCountRead))
   {
      // Key not in DB — full export
      return ExportSymbolTF(symbol, tf, 0);
   }
   DatabaseColumnInteger(g_stmtBarCountRead, 0, existingCount);
   if(existingCount <= 0) return ExportSymbolTF(symbol, tf, 0); // empty / corrupt

   // ── Step 2: find merge point using lastSyncTime (already in memory, no blob read) ──
   // lastSyncTime == last bar's open time in seconds; blob stores ts as epoch milliseconds.
   long lastExistingTsMs = (long)lastSyncTime * 1000;

   int newStart = newCopied; // default: no bars found beyond existing (will be overridden below)
   bool updatedLast = false;

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
         // This bar matches the existing last bar — it may have updated (close, high, low, volume)
         updatedLast = true;
         newStart = i + 1;
         // (no break: scan continues; subsequent bars with higher ts will set newStart further)
      }
   }

   int appendCount = newCopied - newStart;
   long nowTs = (long)TimeCurrent();

   // ── Step 3: apply delta via SQL BLOB UPDATE — zero large-blob MQL5 allocation ──

   if(appendCount <= 0 && updatedLast)
   {
      // Case 1: Only the forming bar's OHLCV changed — replace last 48 bytes in-place.
      // Transfers exactly 48 bytes across MQL5/SQLite boundary regardless of blob size.
      if(g_stmtUpdateLastBar == INVALID_HANDLE) return ExportSymbolTF(symbol, tf, 0);
      uchar lastBarBlob[48];
      PackSingleBar(lastBarBlob, 0, newRates[newStart - 1]);
      DatabaseReset(g_stmtUpdateLastBar);
      if(!DatabaseBindArray(g_stmtUpdateLastBar, 0, lastBarBlob) ||
         !DatabaseBind(g_stmtUpdateLastBar, 1, nowTs) ||
         !DatabaseBind(g_stmtUpdateLastBar, 2, key))
      {
         return 0;
      }
      DatabaseRead(g_stmtUpdateLastBar);
      return existingCount;
   }

   if(appendCount <= 0)
   {
      // No new bars, last bar not matched (nothing to do this cycle)
      return existingCount;
   }

   int mergedCount = existingCount + appendCount;

   // Cap total bars per key — prevents blobs from growing without bound.
   // If we'd exceed MAX_BARS_PER_KEY, SQLite trims oldest bars server-side via SUBSTR.
   // E.g., 100K existing + 5 new = 100005 → trim 5 oldest → 100000 total.
   // The trim is implicit in the SUBSTR: we skip the first N*48 bytes of existing data.
   int trimCount = 0;
   if(mergedCount > MAX_BARS_PER_KEY)
   {
      trimCount = mergedCount - MAX_BARS_PER_KEY;
      mergedCount = MAX_BARS_PER_KEY;
   }

   // If trimming needed, fall back to full re-export capped at MAX_BARS_PER_KEY.
   // This only triggers once when a key first hits the cap. After that, each cycle
   // adds 1-5 and would trim 1-5, keeping count at MAX_BARS_PER_KEY. But the trim
   // requires dynamic SUBSTR offsets incompatible with pre-prepared statements, so
   // a full export (which already caps at MaxBarsForTF = 100K) is simpler and correct.
   if(trimCount > 0)
   {
      static int trimLogCount = 0;
      if(trimLogCount < 5)
      {
         PrintFormat("  CAP: %s — %d+%d=%d exceeds %d, full re-export with cap",
            key, existingCount, appendCount, existingCount + appendCount, MAX_BARS_PER_KEY);
         trimLogCount++;
      }
      return ExportSymbolTF(symbol, tf, MAX_BARS_PER_KEY);
   }

   uchar countLE4[];
   EncodeLE4(mergedCount, countLE4);

   if(updatedLast && newStart > 0)
   {
      // Case 2: Replace last bar + append new bars.
      // SQL: magic(4) || newCount(4) || allBarsExceptLast || updatedLastBar(48) || newBars
      if(g_stmtReplaceLastAndAppend == INVALID_HANDLE) return ExportSymbolTF(symbol, tf, 0);
      uchar lastBarBlob[48];
      PackSingleBar(lastBarBlob, 0, newRates[newStart - 1]);
      uchar newBarsBlob[];
      PackBarsBlob(newRates, newStart, appendCount, newBarsBlob);
      DatabaseReset(g_stmtReplaceLastAndAppend);
      if(!DatabaseBindArray(g_stmtReplaceLastAndAppend, 0, countLE4) ||
         !DatabaseBindArray(g_stmtReplaceLastAndAppend, 1, lastBarBlob) ||
         !DatabaseBindArray(g_stmtReplaceLastAndAppend, 2, newBarsBlob) ||
         !DatabaseBind(g_stmtReplaceLastAndAppend, 3, (long)mergedCount) ||
         !DatabaseBind(g_stmtReplaceLastAndAppend, 4, nowTs) ||
         !DatabaseBind(g_stmtReplaceLastAndAppend, 5, key))
      {
         return 0;
      }
      DatabaseRead(g_stmtReplaceLastAndAppend);
   }
   else
   {
      // Case 3: Append-only — new bars start past the existing last timestamp.
      // SQL: magic(4) || newCount(4) || existingBars(unchanged) || newBars
      if(g_stmtAppendOnly == INVALID_HANDLE) return ExportSymbolTF(symbol, tf, 0);
      uchar newBarsBlob[];
      PackBarsBlob(newRates, newStart, appendCount, newBarsBlob);
      DatabaseReset(g_stmtAppendOnly);
      if(!DatabaseBindArray(g_stmtAppendOnly, 0, countLE4) ||
         !DatabaseBindArray(g_stmtAppendOnly, 1, newBarsBlob) ||
         !DatabaseBind(g_stmtAppendOnly, 2, (long)mergedCount) ||
         !DatabaseBind(g_stmtAppendOnly, 3, nowTs) ||
         !DatabaseBind(g_stmtAppendOnly, 4, key))
      {
         return 0;
      }
      DatabaseRead(g_stmtAppendOnly);
   }

   static int incrLogCount = 0;
   if(incrLogCount < 10)
   {
      PrintFormat("  INCR OK: %s — +%d bars (total %d, %d delta bytes)", key, appendCount, mergedCount, appendCount * 48 + 4);
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
   if(g_stmtBarInsert != INVALID_HANDLE)             { DatabaseFinalize(g_stmtBarInsert);             g_stmtBarInsert = INVALID_HANDLE; }
   if(g_stmtTrackInsert != INVALID_HANDLE)           { DatabaseFinalize(g_stmtTrackInsert);           g_stmtTrackInsert = INVALID_HANDLE; }
   if(g_stmtQuoteInsert != INVALID_HANDLE)           { DatabaseFinalize(g_stmtQuoteInsert);           g_stmtQuoteInsert = INVALID_HANDLE; }
   if(g_stmtBarCountRead != INVALID_HANDLE)          { DatabaseFinalize(g_stmtBarCountRead);          g_stmtBarCountRead = INVALID_HANDLE; }
   if(g_stmtUpdateLastBar != INVALID_HANDLE)         { DatabaseFinalize(g_stmtUpdateLastBar);         g_stmtUpdateLastBar = INVALID_HANDLE; }
   if(g_stmtReplaceLastAndAppend != INVALID_HANDLE)  { DatabaseFinalize(g_stmtReplaceLastAndAppend);  g_stmtReplaceLastAndAppend = INVALID_HANDLE; }
   if(g_stmtAppendOnly != INVALID_HANDLE)            { DatabaseFinalize(g_stmtAppendOnly);            g_stmtAppendOnly = INVALID_HANDLE; }
   if(g_db != INVALID_HANDLE)
   {
      DatabaseClose(g_db);
      g_db = INVALID_HANDLE;
   }
   Print("BarCacheWriter: stopped");
}
