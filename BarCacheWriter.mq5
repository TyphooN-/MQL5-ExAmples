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
#property version   "1.439"
#property description "TTBR binary bar cache + specs + bid/ask to SQLite."
#property description "v1.439: Perf — sorted demand lookup, linear CSV build, specs caching, pre-prepared metadata stmts."
#property description "v1.437: Batched integrity, demand.txt, OOM fix."
#property strict


input int    UpdateIntervalSec = 30;     // Update interval (seconds)
input bool   MarketWatchOnly   = false;  // false = ALL broker symbols
input int    BatchSize         = 10;     // Symbols per SQLite transaction
input bool   ForceReExport     = false;  // true = clear tracking, re-export all history once
input bool   IntegrityCheck    = true;   // Verify bar counts on startup, re-export short keys
input int    SpecsCacheMin     = 60;    // Minutes between full symbol spec refreshes (default 1h)

int g_db = INVALID_HANDLE;
string g_accountTag = "";
int g_stmtBarInsert = INVALID_HANDLE;       // Pre-prepared INSERT OR REPLACE for bar_cache
int g_stmtBarRead = INVALID_HANDLE;         // Pre-prepared SELECT data FROM bar_cache WHERE key=?1
int g_stmtTrackInsert = INVALID_HANDLE;     // Pre-prepared INSERT OR REPLACE for bar_track
int g_stmtQuoteInsert = INVALID_HANDLE;     // Pre-prepared INSERT OR REPLACE for bid_ask
int g_stmtMetaInsert  = INVALID_HANDLE;     // Pre-prepared INSERT OR REPLACE for metadata (specs/symbols/server)
bool g_demandSorted = false;                 // true after demand symbols are sorted for binary search
string g_cachedSpecsCsv = "";                // Cached specs CSV (avoid rebuilding every 5min)
datetime g_specsLastBuild = 0;               // When specs CSV was last rebuilt

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
// Per-TF last export time — skip TFs that can't have new bars since last check.
// E.g., H4 bars only change every 4 hours, no point checking every 30 seconds.
datetime g_tfLastExportTime[9]; // indexed by g_timeframes[] order


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

   // DELETE journal mode — WAL shared memory doesn't work across Wine/Linux boundary.
   DatabaseExecute(g_db, "PRAGMA journal_mode=DELETE");
   // NORMAL sync: fsync only on critical moments, not every transaction.
   // DELETE mode already journals changes — NORMAL is safe for power loss.
   DatabaseExecute(g_db, "PRAGMA synchronous=NORMAL");
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

   ArrayInitialize(g_tfLastExportTime, 0);

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
      #define INTEGRITY_BAR_CAP 10000  // Cap bars during integrity (full history via normal cycle)

      // Read demand list from TyphooN-Terminal (if present)
      // demand.txt v2: SYMBOL:TF:LAST_TS format (lines starting with # are comments)
      // v1 compat: plain symbol names (no colons) are treated as "all TFs, full export"
      string demandSymbols[];    // flat symbol list (v1 compat), sorted for binary search
      ArrayResize(demandSymbols, 100);  // Pre-allocate to avoid quadratic resize
      int demandCount = 0;
      // v2: per-symbol:TF last timestamp — only export bars AFTER this time
      string demandKeys[];       // "EURUSD:1Hour"
      datetime demandTimestamps[];  // last known timestamp
      ArrayResize(demandKeys, 100);
      ArrayResize(demandTimestamps, 100);
      int demandV2Count = 0;

      string demandFile = g_accountTag + "_demand.txt";
      int demandHandle = FileOpen(demandFile, FILE_READ | FILE_ANSI | FILE_COMMON);
      if(demandHandle == INVALID_HANDLE)
         demandHandle = FileOpen("demand.txt", FILE_READ | FILE_ANSI | FILE_COMMON);
      if(demandHandle != INVALID_HANDLE)
      {
         while(!FileIsEnding(demandHandle))
         {
            string line = FileReadString(demandHandle);
            StringTrimRight(line);
            StringTrimLeft(line);
            if(StringLen(line) == 0 || StringGetCharacter(line, 0) == '#') continue; // skip empty/comments

            // v2 format: SYMBOL:TF:TIMESTAMP (3 colons)
            string parts[];
            int nParts = StringSplit(line, ':', parts);
            if(nParts == 3)
            {
               // v2 entry: EURUSD:1Hour:1743897600000
               string symTf = parts[0] + ":" + parts[1];
               long tsMs = StringToInteger(parts[2]);
               datetime ts = (datetime)(tsMs / 1000);
               if(demandV2Count >= ArraySize(demandKeys))
               {
                  ArrayResize(demandKeys, demandV2Count * 2 + 1);
                  ArrayResize(demandTimestamps, demandV2Count * 2 + 1);
               }
               demandKeys[demandV2Count] = symTf;
               demandTimestamps[demandV2Count] = ts;
               demandV2Count++;
               // Also add symbol to flat list for v1 compat matching
               bool symFound = false;
               for(int di = 0; di < demandCount; di++)
                  if(demandSymbols[di] == parts[0]) { symFound = true; break; }
               if(!symFound) {
                  if(demandCount >= ArraySize(demandSymbols))
                     ArrayResize(demandSymbols, demandCount * 2 + 1);
                  demandSymbols[demandCount] = parts[0];
                  demandCount++;
               }
            }
            else if(nParts == 1)
            {
               // v1 format: plain symbol name
               if(demandCount >= ArraySize(demandSymbols))
                  ArrayResize(demandSymbols, demandCount * 2 + 1);
               demandSymbols[demandCount] = line;
               demandCount++;
            }
         }
         FileClose(demandHandle);
         // Sort demand symbols for O(log n) binary search in main loop
         if(demandCount > 1)
         {
            // Simple insertion sort on small array
            for(int si = 1; si < demandCount; si++)
            {
               string tmp = demandSymbols[si];
               int sj = si - 1;
               while(sj >= 0 && StringCompare(demandSymbols[sj], tmp) > 0)
               {
                  demandSymbols[sj + 1] = demandSymbols[sj];
                  sj--;
               }
               demandSymbols[sj + 1] = tmp;
            }
         }
         g_demandSorted = true;
         PrintFormat("BarCacheWriter: demand.txt loaded — %d symbols (sorted), %d v2 entries", demandCount, demandV2Count);
      }

      int batchCount = 0;
      SafeBegin();
      for(int si = 0; si < symCount; si++)
      {
         string sym = SymbolName(si, MarketWatchOnly);
         if(StringLen(sym) == 0) continue;
         if(!g_isCFDServer && IsForexSymbol(sym)) continue;

         // If demand list exists and this symbol isn't in it, defer to normal cycle
         if(demandCount > 0)
         {
            bool inDemand = false;
            for(int di = 0; di < demandCount; di++)
            {
               if(demandSymbols[di] == sym) { inDemand = true; break; }
            }
            if(!inDemand) continue;
         }

         SymbolSelect(sym, true);

         for(int ti = 0; ti < ArraySize(g_timeframes); ti++)
         {
            ENUM_TIMEFRAMES enumTf = g_timeframes[ti];
            int mt5Count = Bars(sym, enumTf);
            if(mt5Count < 100) continue; // skip symbols with minimal data

            // Get DB bar count
            string cacheKey = "mt5:" + sym + ":" + g_tfStrings[ti];
            int dbCount = 0;
            if(g_stmtBarRead != INVALID_HANDLE)
            {
               DatabaseReset(g_stmtBarRead);
               DatabaseBind(g_stmtBarRead, 0, cacheKey);
               if(DatabaseRead(g_stmtBarRead))
               {
                  uchar tmpBlob[];
                  DatabaseColumnBlob(g_stmtBarRead, 0, tmpBlob);
                  if(ArraySize(tmpBlob) >= 8 && tmpBlob[0] == 'T' && tmpBlob[1] == 'T')
                     dbCount = (int)tmpBlob[4] | ((int)tmpBlob[5] << 8) | ((int)tmpBlob[6] << 16) | ((int)tmpBlob[7] << 24);
               }
            }
            checkedCount++;

            // Re-export if DB has <50% of MT5's available bars
            if(dbCount < mt5Count / 2)
            {
               // Cap bars during integrity to prevent OOM — full history fills via normal 30s cycle
               int maxBars = MathMin(MaxBarsForTF(enumTf), INTEGRITY_BAR_CAP);
               int bars = ExportSymbolTF(sym, enumTf, maxBars);
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
                     PrintFormat("  Integrity fix: %s — DB %d bars, MT5 %d, exported %d (cap %d)",
                        cacheKey, dbCount, mt5Count, bars, maxBars);
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
         checkedCount, reExportCount, totalReExportedBars, intElapsed, demandCount);
   }

   PrintFormat("BarCacheWriter v1.438: %s symbols(%d), %ds interval, batch=%d, %d cached keys, 16MB cache, forex=%s, integrity=%s",
      MarketWatchOnly ? "MW" : "ALL", initSymCount, UpdateIntervalSec, BatchSize, g_trackCount,
      g_isCFDServer ? "ENABLED" : "SKIPPED",
      IntegrityCheck ? "ON" : "OFF");

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

   // Live bid/ask sync — every OTHER cycle (60s instead of 30s) to reduce CPU
   // v1.438: halved bid/ask frequency — 30s is overkill for most trading
   static int quoteSkip = 0;
   quoteSkip++;
   if(g_stmtQuoteInsert != INVALID_HANDLE && quoteSkip % 2 == 0)
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
      SafeCommit();
   }

   int batchCount = 0;  // symbols in current transaction batch
   bool inTxn = false;

   // v1.438 CPU optimization: process symbols in rotating batches instead of all 851 every cycle.
   // Each cycle processes BatchSize*10 symbols (default 100). Full rotation in ~8 cycles (~4 min).
   // Demand symbols (from TyphooN-Terminal) are ALWAYS processed every cycle for low latency.
   static int rotationOffset = 0;
   int symbolsPerCycle = BatchSize * 10; // default 100 symbols per 30s cycle

   for(int i = 0; i < symCount; i++)
   {
      string symbol = SymbolName(i, MarketWatchOnly);
      if(StringLen(symbol) == 0) continue;

      // Skip forex symbols on non-CFD servers
      if(!g_isCFDServer && IsForexSymbol(symbol)) continue;

      // Rotation: skip symbols outside current batch window UNLESS they're in demand list
      // O(log n) binary search on sorted demand array (was O(n) linear scan)
      bool isDemand = (g_demandSorted && demandCount > 0)
         ? BinarySearchKey(demandSymbols, demandCount, symbol) >= 0
         : false;
      if(!isDemand && demandCount > 0 && !g_demandSorted)
      {
         for(int di = 0; di < demandCount; di++)
            if(demandSymbols[di] == symbol) { isDemand = true; break; }
      }
      if(!isDemand && (i < rotationOffset || i >= rotationOffset + symbolsPerCycle))
         continue;

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
         // TF gating: skip TFs that can't have new bars yet.
         // If less than 80% of the TF period has elapsed since last export,
         // a new bar can't have formed. 80% threshold allows for early checks
         // near bar boundaries. This eliminates ~90% of CopyRates calls:
         // at 30s intervals, only M1 is checked every cycle; M5 every ~4 min,
         // H1 every ~48 min, D1 every ~19 hours, etc.
         int tfPeriod = PeriodSeconds(g_timeframes[tf]);
         datetime now = TimeCurrent();
         if(g_tfLastExportTime[tf] > 0 && tfPeriod > 0)
         {
            int elapsed = (int)(now - g_tfLastExportTime[tf]);
            if(elapsed < (int)(tfPeriod * 0.8))
            {
               skipped++;
               continue;
            }
         }

         string trackKey = symbol + ":" + g_tfStrings[tf];
         int idx = GetTrackIndex(trackKey);

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
               g_trackFails[idx] = 0;
               g_tfLastExportTime[tf] = now;
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
            g_tfLastExportTime[tf] = now; // update TF gate timer
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
         // Yield CPU between batches. EXCLUSIVE locking mode means BarCacheWriter
         // holds the lock for the entire session — readers (Mt5Sync) wait on
         // busy_timeout. 200ms sleep gives readers a window between batches.
         // With TF gating + EXCLUSIVE lock, total cycle time is minimal.
         Sleep(200);
      }
   }

   // Commit any remaining symbols in the last partial batch
   if(inTxn) SafeCommit();

   // Sort tracking arrays after population phase for O(log n) lookups on subsequent ticks
   if(!g_trackSorted && g_trackCount > 0)
      SortTrackArrays();

   // Periodic compact: VACUUM every ~2 hours to reclaim /dev/shm space
   if(g_cycleCount % 240 == 0 && g_cycleCount > 0)
   {
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
      PrintFormat("BarCacheWriter: %d exported, %d skipped, %d bars | %d pending | %dms (batch %d-%d of %d)",
         exported, skipped, totalBars, pendingSlots, elapsed,
         rotationOffset, MathMin(rotationOffset + symbolsPerCycle, symCount), symCount);

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

   // Use pre-prepared statement for metadata writes
   if(g_stmtMetaInsert != INVALID_HANDLE)
   {
      DatabaseReset(g_stmtMetaInsert);
      DatabaseBind(g_stmtMetaInsert, 0, "mt5:__SYMBOLS__:" + g_accountTag);
      DatabaseBind(g_stmtMetaInsert, 1, "[\"" + StringReplace2(csv, ",", "\",\"") + "\"]");
      DatabaseBind(g_stmtMetaInsert, 2, (long)TimeCurrent());
      DatabaseBind(g_stmtMetaInsert, 3, (long)symCount);
      DatabaseRead(g_stmtMetaInsert);
   }

   // Store broker/server identity — TyphooN-Terminal reads this for data source badge
   if(g_stmtMetaInsert != INVALID_HANDLE)
   {
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
}

void WriteSymbolSpecs(int symCount)
{
   // v1.439: Cache specs and only rebuild when SpecsCacheMin has elapsed.
   // SymbolInfo calls are expensive (~16 per symbol × 851 symbols = 13,616 calls).
   // Specs rarely change (swap rates, margins updated by broker infrequently).
   if(StringLen(g_cachedSpecsCsv) > 0 && g_specsLastBuild > 0
      && TimeCurrent() - g_specsLastBuild < SpecsCacheMin * 60)
   {
      // Use cached CSV — just write it to DB (timestamps updated)
      if(g_stmtMetaInsert != INVALID_HANDLE)
      {
         DatabaseReset(g_stmtMetaInsert);
         DatabaseBind(g_stmtMetaInsert, 0, "mt5:__SPECS__:" + g_accountTag);
         DatabaseBind(g_stmtMetaInsert, 1, g_cachedSpecsCsv);
         DatabaseBind(g_stmtMetaInsert, 2, (long)TimeCurrent());
         DatabaseBind(g_stmtMetaInsert, 3, (long)symCount);
         DatabaseRead(g_stmtMetaInsert);
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

      // Sanitize description (remove commas/newlines that would break CSV)
      StringReplace(desc, ",", ";");
      StringReplace(desc, "\n", " ");
      StringReplace(desc, "\r", "");

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

   // Join all lines with newline separator (single large allocation at the end)
   string csv = "";
   for(int i = 0; i < count; i++)
   {
      if(i > 0) csv += "\n";
      csv += lines[i];
   }
   csv += "\n";

   // Cache the built CSV
   g_cachedSpecsCsv = csv;
   g_specsLastBuild = TimeCurrent();

   // Write to DB using pre-prepared statement
   if(g_stmtMetaInsert != INVALID_HANDLE)
   {
      DatabaseReset(g_stmtMetaInsert);
      DatabaseBind(g_stmtMetaInsert, 0, "mt5:__SPECS__:" + g_accountTag);
      DatabaseBind(g_stmtMetaInsert, 1, csv);
      DatabaseBind(g_stmtMetaInsert, 2, (long)TimeCurrent());
      DatabaseBind(g_stmtMetaInsert, 3, (long)count);
      DatabaseRead(g_stmtMetaInsert);
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
// v1.430: Reliable in-memory merge. Reads existing blob, merges new bars, writes back.
// v1.427-v1.429 used SQL SUBSTR/BLOB UPDATE which corrupted data due to MQL5's
// DatabaseBindArray binding uchar[] as TEXT — SUBSTR on TEXT uses character offsets
// not byte offsets, producing truncated output (110 bytes instead of megabytes).
int IncrementalExportSymbolTF(string symbol, ENUM_TIMEFRAMES tf, datetime lastSyncTime)
{
   int tfSeconds = PeriodSeconds(tf);
   int elapsed = (int)(TimeCurrent() - lastSyncTime);
   int estimatedNewBars = (tfSeconds > 0) ? (elapsed / tfSeconds) + 2 : 10;
   int fetchCount = MathMax(3, MathMin(estimatedNewBars, 200));

   MqlRates newRates[];
   ArraySetAsSeries(newRates, false);
   int newCopied = CopyRates(symbol, tf, 0, fetchCount, newRates);
   if(newCopied <= 0) return 0;

   string key = "mt5:" + symbol + ":" + TFToStr(tf);

   // Read existing blob from DB via pre-prepared statement
   if(g_stmtBarRead == INVALID_HANDLE) return ExportSymbolTF(symbol, tf, 0);
   DatabaseReset(g_stmtBarRead);
   DatabaseBind(g_stmtBarRead, 0, key);
   uchar existingBlob[];
   bool hasExisting = false;
   if(DatabaseRead(g_stmtBarRead))
   {
      DatabaseColumnBlob(g_stmtBarRead, 0, existingBlob);
      hasExisting = (ArraySize(existingBlob) >= 8);
   }

   if(!hasExisting)
      return ExportSymbolTF(symbol, tf, 0);

   // Verify TTBR magic
   if(existingBlob[0] != 'T' || existingBlob[1] != 'T' || existingBlob[2] != 'B' || existingBlob[3] != 'R')
      return ExportSymbolTF(symbol, tf, 0);

   int existingCount = (int)existingBlob[4]
                     | ((int)existingBlob[5] << 8)
                     | ((int)existingBlob[6] << 16)
                     | ((int)existingBlob[7] << 24);

   if(existingCount <= 0 || ArraySize(existingBlob) < 8 + existingCount * 48)
      return ExportSymbolTF(symbol, tf, 0);

   // Find last existing bar timestamp
   ByteConv bc;
   int lastOff = 8 + (existingCount - 1) * 48;
   ReadRaw8(existingBlob, lastOff, bc);
   long lastExistingTsMs = bc.l;

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
      // No new bars — skip the blob write entirely.
      // The forming bar's close/high/low changes every tick but rewriting a 4.8MB
      // blob for a 48-byte update is the #1 cause of Wine I/O contention.
      // The forming bar will be captured when the NEXT bar opens (appendCount > 0).
      // Bid/ask table already captures live prices for the watchlist.
      return existingCount;
   }

   int mergedCount = existingCount + appendCount;

   // Bar cap: if exceeded, do full re-export capped at MAX_BARS_PER_KEY
   if(mergedCount > MAX_BARS_PER_KEY)
      return ExportSymbolTF(symbol, tf, MAX_BARS_PER_KEY);

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
   if(g_stmtBarInsert != INVALID_HANDLE)   { DatabaseFinalize(g_stmtBarInsert);   g_stmtBarInsert = INVALID_HANDLE; }
   if(g_stmtBarRead != INVALID_HANDLE)     { DatabaseFinalize(g_stmtBarRead);     g_stmtBarRead = INVALID_HANDLE; }
   if(g_stmtTrackInsert != INVALID_HANDLE) { DatabaseFinalize(g_stmtTrackInsert); g_stmtTrackInsert = INVALID_HANDLE; }
   if(g_stmtQuoteInsert != INVALID_HANDLE) { DatabaseFinalize(g_stmtQuoteInsert); g_stmtQuoteInsert = INVALID_HANDLE; }
   if(g_stmtMetaInsert != INVALID_HANDLE)  { DatabaseFinalize(g_stmtMetaInsert);  g_stmtMetaInsert = INVALID_HANDLE; }
   if(g_db != INVALID_HANDLE)
   {
      DatabaseClose(g_db);
      g_db = INVALID_HANDLE;
   }
   Print("BarCacheWriter: stopped");
}
