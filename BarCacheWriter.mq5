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
#property version   "1.412"
#property description "Writes bar data (TTBR binary) + symbol specs to SQLite."
#property description "v1.412: eliminate redundant CopyRates/SymbolSelect, cache TF strings, schema TEXT→BLOB."
#property description "v1.411: metadata writes every 5min (was every tick) — enables Rust mtime fast-path."
#property description "v1.410: tiered bar limits per TF + configurable MaxPendingPerTick (OOM guard)."
#property description "v1.400: binary TTBR format (48 bytes/bar, zero string overhead)."
#property description "v1.300: adds __SPECS__ export (Sector, Industry, TradeMode, Swaps, Spread)."
#property strict


input int    UpdateIntervalSec = 30;     // Update interval (seconds)
input int    BarsPerUpdate     = 100;    // Bars per incremental update (recent only)
input bool   MarketWatchOnly   = false;  // false = ALL broker symbols
input int    MaxPendingPerTick = 10;     // Max full exports per tick (memory guard)

int g_db = INVALID_HANDLE;
string g_accountTag = "";

// Track last bar time per symbol:TF to skip unchanged data
// Uses sorted arrays + binary search for O(log n) lookup instead of O(n) linear scan
// With 851 symbols × 9 TFs = 7,659 keys, this matters every tick
string g_trackKeys[];
datetime g_trackTimes[];
int g_trackCount = 0;
bool g_trackSorted = true; // false when new keys appended, triggers re-sort


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

// Detect whether this is a specialized account (crypto/futures) that should skip forex pairs.
// Forex pairs appear on ALL Darwinex accounts — the main CFD account exports them,
// so crypto and futures accounts should skip them to avoid redundant writes.
// Detection: the main CFD account has 100+ symbols (forex + commodities + stocks + ETFs).
// Specialized accounts (crypto, futures) have far fewer (<100).
bool g_skipForex = false;

void DetectAccountType(int symCount)
{
   // The main CFD account has hundreds of symbols (stocks, ETFs, commodities, forex).
   // Specialized accounts (crypto ~10, futures ~40) have far fewer.
   g_skipForex = (symCount < 100);

   PrintFormat("BarCacheWriter: %s forex (%d symbols — %s account)",
      g_skipForex ? "SKIPPING" : "EXPORTING", symCount,
      g_skipForex ? "specialized" : "main CFD");
}

bool IsNativeSymbol(string symbol)
{
   if(!g_skipForex) return true;

   string sector = "";
   if(SymbolInfoString(symbol, SYMBOL_SECTOR_NAME, sector))
   {
      if(sector == "Currency") return false;
   }
   return true;
}

// Timeframes: MN1 first (higher TFs are smaller, export fast)
ENUM_TIMEFRAMES g_timeframes[] = {
   PERIOD_MN1, PERIOD_W1, PERIOD_D1, PERIOD_H4, PERIOD_H1,
   PERIOD_M30, PERIOD_M15, PERIOD_M5, PERIOD_M1
};
// Pre-cached TF strings — avoids 7,659 switch evaluations per tick
string g_tfStrings[9];

// Max bars for initial full export per timeframe — prevents OOM on lower TFs
// M1: 10K bars = 480KB, M5: 20K = 960KB, vs old 100K = 4.8MB per entry
int MaxBarsForTF(ENUM_TIMEFRAMES tf)
{
   switch(tf)
   {
      case PERIOD_MN1: return 1000;
      case PERIOD_W1:  return 2000;
      case PERIOD_D1:  return 10000;
      case PERIOD_H4:  return 20000;
      case PERIOD_H1:  return 50000;
      case PERIOD_M30: return 50000;
      case PERIOD_M15: return 50000;
      case PERIOD_M5:  return 20000;
      case PERIOD_M1:  return 10000;
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
      int j = i - 1;
      while(j >= 0 && StringCompare(g_trackKeys[j], tmpKey) > 0)
      {
         g_trackKeys[j + 1] = g_trackKeys[j];
         g_trackTimes[j + 1] = g_trackTimes[j];
         j--;
      }
      g_trackKeys[j + 1] = tmpKey;
      g_trackTimes[j + 1] = tmpTime;
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
   g_trackKeys[g_trackCount - 1] = key;
   g_trackTimes[g_trackCount - 1] = 0;
   g_trackSorted = false;
   return g_trackCount - 1;
}

// Persist last bar time to DB so we survive restarts
void SaveTrackTime(string trackKey, datetime barTime)
{
   int req = DatabasePrepare(g_db,
      "INSERT OR REPLACE INTO bar_track (key, last_bar_time) VALUES (?1, ?2)");
   if(req != INVALID_HANDLE)
   {
      DatabaseBind(req, 0, trackKey);
      DatabaseBind(req, 1, (long)barTime);
      DatabaseRead(req);
      DatabaseFinalize(req);
   }
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

   // Covering index: lets readers get key/timestamp/bar_count from index alone,
   // without scanning through multi-MB blob rows. Drops metadata queries from ~12s to <100ms.
   DatabaseExecute(g_db, "CREATE INDEX IF NOT EXISTS idx_bar_meta ON bar_cache(key, timestamp, bar_count)");

   // Use DELETE journal mode — WAL shared memory doesn't work across Wine/Linux boundary
   DatabaseExecute(g_db, "PRAGMA journal_mode=DELETE");
   DatabaseExecute(g_db, "PRAGMA synchronous=NORMAL");
   DatabaseExecute(g_db, "PRAGMA cache_size=-8000"); // 8MB page cache
   DatabaseExecute(g_db, "PRAGMA busy_timeout=5000"); // Wait up to 5s for lock instead of failing

   // Cache TF strings once — eliminates switch evaluation per symbol×TF per tick
   for(int t = 0; t < ArraySize(g_timeframes); t++)
      g_tfStrings[t] = TFToStr(g_timeframes[t]);

   // Detect which sector this instance primarily serves
   int initSymCount = SymbolsTotal(MarketWatchOnly);
   DetectAccountType(initSymCount);

   long acct = AccountInfoInteger(ACCOUNT_LOGIN);
   g_accountTag = IntegerToString(acct);

   // Restore tracking state from DB — survive restarts without re-exporting everything
   LoadTrackingFromDB();

   PrintFormat("BarCacheWriter v1.412: %s symbols(%d), %ds interval, %d bars/update, %d pending/tick, %d cached keys",
      MarketWatchOnly ? "MW" : "ALL", initSymCount, UpdateIntervalSec, BarsPerUpdate, MaxPendingPerTick, g_trackCount);

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
   int skippedNative = 0;
   int pendingRetries = 0;        // count of pending full exports attempted this tick

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

   for(int i = 0; i < symCount; i++)
   {
      string symbol = SymbolName(i, MarketWatchOnly);
      if(StringLen(symbol) == 0) continue;

      // Skip shared forex pairs on non-forex instances
      if(!IsNativeSymbol(symbol))
      {
         skippedNative += ArraySize(g_timeframes);
         continue;
      }

      // Select once per symbol — covers all TFs (was duplicated inside ExportSymbolTF)
      SymbolSelect(symbol, true);

      // Per-symbol transaction — keeps lock hold time short so readers aren't blocked
      SafeBegin();

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
            if(pendingRetries >= MaxPendingPerTick) continue; // defer to next tick
            pendingRetries++;

            // Non-blocking: use (start_pos, count) form which returns locally available data
            // immediately without waiting for server download.
            // Tiered limits prevent OOM on lower timeframes (M1 capped at 10K vs old 100K)
            int bars = ExportSymbolTF(symbol, g_timeframes[tf], MaxBarsForTF(g_timeframes[tf]));

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
            continue;
         }

         // Already synced — skip if last bar hasn't changed
         if(gotLast == 1 && lastRate[0].time == g_trackTimes[idx])
         {
            skipped++;
            continue;
         }

         // Incremental: export recent bars only
         int bars = ExportSymbolTF(symbol, g_timeframes[tf], BarsPerUpdate);
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

      SafeCommit();
   }

   // Sort tracking arrays after population phase for O(log n) lookups on subsequent ticks
   if(!g_trackSorted && g_trackCount > 0)
      SortTrackArrays();

   // Count TF slots still pending (never successfully exported)
   int pendingSlots = 0;
   for(int i = 0; i < g_trackCount; i++)
      if(g_trackTimes[i] == 0) pendingSlots++;

   static datetime lastLog = 0;
   static bool first = true;
   static int failCount = 0;
   if(exported == 0 && skipped == 0) failCount++;
   else failCount = 0;

   if(first || TimeCurrent() - lastLog > 300 || failCount <= 3)
   {
      uint elapsed = GetTickCount() - tickStart;
      PrintFormat("BarCacheWriter: %d exported, %d skipped, %d bars | %d pending (%d retried) | %dms",
         exported, skipped, totalBars, pendingSlots, pendingRetries, elapsed);

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

   int req = DatabasePrepare(g_db,
      "INSERT OR REPLACE INTO bar_cache (key, data, timestamp, bar_count) VALUES (?1, ?2, ?3, ?4)");
   if(req == INVALID_HANDLE)
   {
      PrintFormat("  prepare failed: %s (err %d)", key, GetLastError());
      return 0;
   }

   if(!DatabaseBind(req, 0, key) ||
      !DatabaseBindArray(req, 1, buffer) ||
      !DatabaseBind(req, 2, (long)TimeCurrent()) ||
      !DatabaseBind(req, 3, (long)copied))
   {
      PrintFormat("  bind failed: %s (err %d, bytes=%d)", key, GetLastError(), ArraySize(buffer));
      DatabaseFinalize(req);
      return 0;
   }

   DatabaseRead(req);
   DatabaseFinalize(req);

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
   if(g_db != INVALID_HANDLE)
   {
      DatabaseClose(g_db);
      g_db = INVALID_HANDLE;
   }
   Print("BarCacheWriter: stopped");
}
