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
#property version   "1.306"
#property description "Writes bar data + symbol specs to SQLite using CSV format."
#property description "v1.300: adds __SPECS__ export (Sector, Industry, TradeMode, Swaps, Spread)."
#property strict

#define CHUNK_SIZE 5000

input int    UpdateIntervalSec = 30;     // Update interval (seconds)
input int    BarsPerUpdate     = 100;    // Bars per incremental update (recent only)
input int    FullExportBars    = 0;      // Full export bar count (0 = max, only on first run)
input bool   MarketWatchOnly   = false;  // false = ALL broker symbols

int g_db = INVALID_HANDLE;

// Track last bar time per symbol:TF to skip unchanged data
string g_trackKeys[];
datetime g_trackTimes[];
int g_trackCount = 0;

bool g_fullExportDone = false;

// Cached set of keys already in DB (for skip-on-restart optimization)
string g_existingKeys[];
int g_existingKeyCount = 0;

// Check if a key already exists in the DB (populated on startup)
bool KeyExistsInDB(string key)
{
   for(int i = 0; i < g_existingKeyCount; i++)
      if(g_existingKeys[i] == key) return true;
   return false;
}

// Load existing bar keys from DB on startup
void LoadExistingKeys()
{
   if(g_db == INVALID_HANDLE) return;
   int req = DatabasePrepare(g_db,
      "SELECT key FROM bar_cache WHERE key NOT LIKE '%__SYMBOLS__%' AND key NOT LIKE '%__SPECS__%' AND key NOT LIKE '%:chunks' AND key NOT LIKE '%:chunk_%'");
   if(req == INVALID_HANDLE) return;

   g_existingKeyCount = 0;
   while(DatabaseRead(req))
   {
      string key = "";
      DatabaseColumnText(req, 0, key);
      g_existingKeyCount++;
      ArrayResize(g_existingKeys, g_existingKeyCount);
      g_existingKeys[g_existingKeyCount - 1] = key;
   }
   DatabaseFinalize(req);
   if(g_existingKeyCount > 0)
      PrintFormat("BarCacheWriter: %d existing keys in DB (will skip on full export)", g_existingKeyCount);
}

// Safe transaction wrappers — handle dangling transactions from prior lock failures
bool SafeBegin()
{
   if(!SafeBegin())
   {
      // Likely already in a transaction from a prior failed commit — rollback and retry
      DatabaseExecute(g_db, "ROLLBACK");
      return SafeBegin();
   }
   return true;
}

bool SafeCommit()
{
   if(!SafeCommit())
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
bool g_skipForex = false;

void DetectAccountType(int symCount)
{
   int forexCount = 0, cryptoCount = 0, futuresCount = 0, stockCount = 0;
   for(int i = 0; i < symCount; i++)
   {
      string sym = SymbolName(i, MarketWatchOnly);
      string sector = "";
      if(!SymbolInfoString(sym, SYMBOL_SECTOR_NAME, sector)) continue;
      if(sector == "Currency") forexCount++;
      else if(sector == "Crypto" || sector == "Cryptocurrency") cryptoCount++;
      else if(sector == "Indexes" || sector == "Commodity" || sector == "Energy") futuresCount++;
      else stockCount++;
   }

   // If this account has ANY crypto or futures symbols, it's a specialized account —
   // the main CFD account (forex + commodities + stocks) handles forex export.
   g_skipForex = (cryptoCount > 0 || futuresCount > 0);

   PrintFormat("BarCacheWriter: %s forex (forex=%d, crypto=%d, futures=%d, stocks=%d)",
      g_skipForex ? "SKIPPING" : "EXPORTING", forexCount, cryptoCount, futuresCount, stockCount);
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

int GetTrackIndex(string key)
{
   for(int i = 0; i < g_trackCount; i++)
      if(g_trackKeys[i] == key) return i;
   g_trackCount++;
   ArrayResize(g_trackKeys, g_trackCount);
   ArrayResize(g_trackTimes, g_trackCount);
   g_trackKeys[g_trackCount - 1] = key;
   g_trackTimes[g_trackCount - 1] = 0;
   return g_trackCount - 1;
}

int OnInit()
{
   g_db = DatabaseOpen("typhoon_mt5_cache.db", DATABASE_OPEN_READWRITE | DATABASE_OPEN_CREATE);
   if(g_db == INVALID_HANDLE)
   {
      PrintFormat("BarCacheWriter: DB open failed (error %d)", GetLastError());
      return INIT_FAILED;
   }

   // CSV data table: stores bars as compact CSV text (no JSON overhead)
   // Format: "timestamp,open,high,low,close,volume\n" per bar
   if(!DatabaseExecute(g_db,
      "CREATE TABLE IF NOT EXISTS bar_cache ("
      "  key TEXT PRIMARY KEY,"
      "  data TEXT NOT NULL,"
      "  timestamp INTEGER NOT NULL,"
      "  bar_count INTEGER NOT NULL DEFAULT 0"
      ")"))
   {
      PrintFormat("BarCacheWriter: table create failed (error %d)", GetLastError());
      DatabaseClose(g_db); g_db = INVALID_HANDLE;
      return INIT_FAILED;
   }

   // Use DELETE journal mode — WAL shared memory doesn't work across Wine/Linux boundary
   DatabaseExecute(g_db, "PRAGMA journal_mode=DELETE");
   DatabaseExecute(g_db, "PRAGMA synchronous=NORMAL");
   DatabaseExecute(g_db, "PRAGMA cache_size=-8000"); // 8MB page cache
   DatabaseExecute(g_db, "PRAGMA busy_timeout=5000"); // Wait up to 5s for lock instead of failing

   // Load existing keys to skip redundant full exports on restart
   LoadExistingKeys();

   // Detect which sector this instance primarily serves
   int initSymCount = SymbolsTotal(MarketWatchOnly);
   DetectAccountType(initSymCount);

   PrintFormat("BarCacheWriter v1.306: chunked(%d), %s symbols(%d), %ds interval, %d bars/update",
      CHUNK_SIZE, MarketWatchOnly ? "MW" : "ALL", initSymCount, UpdateIntervalSec, BarsPerUpdate);

   EventSetTimer(UpdateIntervalSec);
   ExportAll(true); // initial full export (skips keys already in DB)
   return INIT_SUCCEEDED;
}

void OnTimer() { ExportAll(false); }

void ExportAll(bool fullExport)
{
   if(g_db == INVALID_HANDLE) return;

   int symCount = SymbolsTotal(MarketWatchOnly);
   int exported = 0, skipped = 0, totalBars = 0;

   // Write metadata in its own transaction (available immediately)
   SafeBegin();
   WriteSymbolList(symCount);
   WriteSymbolSpecs(symCount);
   SafeCommit();

   int skippedNative = 0, skippedExisting = 0;

   for(int i = 0; i < symCount; i++)
   {
      string symbol = SymbolName(i, MarketWatchOnly);
      if(StringLen(symbol) == 0) continue;

      // Opt 1: Skip shared forex pairs on non-forex instances
      if(!IsNativeSymbol(symbol))
      {
         skippedNative += ArraySize(g_timeframes);
         continue;
      }

      int symExported = 0;

      for(int tf = 0; tf < ArraySize(g_timeframes); tf++)
      {
         string trackKey = symbol + ":" + TFToStr(g_timeframes[tf]);
         int idx = GetTrackIndex(trackKey);

         // Skip if last bar hasn't changed (incremental mode)
         MqlRates lastRate[];
         if(CopyRates(symbol, g_timeframes[tf], 0, 1, lastRate) == 1)
         {
            if(!fullExport && lastRate[0].time == g_trackTimes[idx])
            {
               skipped++;
               continue;
            }
         }

         // Opt 2: On full export, skip keys that already exist in DB (restart optimization)
         if(fullExport)
         {
            string baseKey = "mt5:" + symbol + ":" + TFToStr(g_timeframes[tf]);
            if(KeyExistsInDB(baseKey))
            {
               skippedExisting++;
               // Still update trackTime so incrementals work correctly
               if(CopyRates(symbol, g_timeframes[tf], 0, 1, lastRate) == 1)
                  g_trackTimes[idx] = lastRate[0].time;
               continue;
            }
         }

         // Fetch ALL bars chunked (full) or recent bars (incremental)
         int bars;
         if(fullExport)
            bars = ExportSymbolTFChunked(symbol, g_timeframes[tf]);
         else
         {
            SafeBegin();
            bars = ExportSymbolTF(symbol, g_timeframes[tf], BarsPerUpdate);
            SafeCommit();
         }

         if(bars > 0)
         {
            symExported++;
            exported++;
            totalBars += bars;
            if(CopyRates(symbol, g_timeframes[tf], 0, 1, lastRate) == 1)
               g_trackTimes[idx] = lastRate[0].time;
         }
      }

      if(fullExport && symExported > 0)
         PrintFormat("BarCacheWriter: %s — %d TFs exported", symbol, symExported);
   }

   static datetime lastLog = 0;
   static bool first = true;
   static int failCount = 0;
   if(exported == 0 && skipped == 0) failCount++;
   else failCount = 0;

   if(first || TimeCurrent() - lastLog > 300 || failCount <= 3)
   {
      if(fullExport)
         PrintFormat("BarCacheWriter: FULL — %d exported, %d skipped(existing), %d skipped(shared), %d bars, %d symbols",
            exported, skippedExisting, skippedNative, totalBars, symCount);
      else
         PrintFormat("BarCacheWriter: incremental — %d exported, %d skipped(unchanged), %d bars, %d symbols",
            exported, skipped, totalBars, symCount);

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
      DatabaseBind(req, 0, "mt5:__SYMBOLS__");
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
      DatabaseBind(req, 0, "mt5:__SPECS__");
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

// Chunked full export: fetches ALL bars, writes to DB in chunks of CHUNK_SIZE
// Key format: "mt5:SYMBOL:TF:chunk_N" for N>0, "mt5:SYMBOL:TF" for chunk 0
// This avoids building huge CSV strings that crash MQL5

int ExportSymbolTFChunked(string symbol, ENUM_TIMEFRAMES tf)
{
   MqlRates rates[];
   ArraySetAsSeries(rates, false);
   SymbolSelect(symbol, true);

   int copied = CopyRates(symbol, tf, D'1970.01.01 00:00', TimeCurrent(), rates);
   if(copied <= 0) return 0;

   int digits = (int)SymbolInfoInteger(symbol, SYMBOL_DIGITS);
   string baseKey = "mt5:" + symbol + ":" + TFToStr(tf);
   int totalChunks = (int)MathCeil((double)copied / CHUNK_SIZE);

   for(int chunk = 0; chunk < totalChunks; chunk++)
   {
      int startIdx = chunk * CHUNK_SIZE;
      int endIdx = MathMin(startIdx + CHUNK_SIZE, copied);
      int chunkBars = endIdx - startIdx;

      // Build CSV for this chunk only
      string csv = "";
      StringInit(csv, chunkBars * 65);
      csv = "";

      for(int i = startIdx; i < endIdx; i++)
      {
         MqlDateTime mdt;
         TimeToStruct(rates[i].time, mdt);
         csv += StringFormat("%04d-%02d-%02dT%02d:%02d:%02dZ,%s,%s,%s,%s,%s\n",
            mdt.year, mdt.mon, mdt.day, mdt.hour, mdt.min, mdt.sec,
            DoubleToString(rates[i].open, digits),
            DoubleToString(rates[i].high, digits),
            DoubleToString(rates[i].low, digits),
            DoubleToString(rates[i].close, digits),
            IntegerToString(rates[i].tick_volume));
      }

      // chunk 0 = "mt5:EURUSD:1Day", chunk 1+ = "mt5:EURUSD:1Day:chunk_1"
      string key = baseKey;
      if(chunk > 0) key += ":chunk_" + IntegerToString(chunk);

      SafeBegin();
      int req = DatabasePrepare(g_db,
         "INSERT OR REPLACE INTO bar_cache (key, data, timestamp, bar_count) VALUES (?1, ?2, ?3, ?4)");
      if(req != INVALID_HANDLE)
      {
         if(DatabaseBind(req, 0, key) &&
            DatabaseBind(req, 1, csv) &&
            DatabaseBind(req, 2, (long)TimeCurrent()) &&
            DatabaseBind(req, 3, (long)chunkBars))
         {
            DatabaseRead(req);
         }
         DatabaseFinalize(req);
      }
      SafeCommit();
   }

   // Store total chunk count so reader knows how many to merge
   // Key: "mt5:SYMBOL:TF:chunks" → value = totalChunks
   if(totalChunks > 1)
   {
      SafeBegin();
      int req = DatabasePrepare(g_db,
         "INSERT OR REPLACE INTO bar_cache (key, data, timestamp, bar_count) VALUES (?1, ?2, ?3, ?4)");
      if(req != INVALID_HANDLE)
      {
         DatabaseBind(req, 0, baseKey + ":chunks");
         DatabaseBind(req, 1, IntegerToString(totalChunks));
         DatabaseBind(req, 2, (long)TimeCurrent());
         DatabaseBind(req, 3, (long)copied);
         DatabaseRead(req);
         DatabaseFinalize(req);
      }
      SafeCommit();
   }

   static int chunkLog = 0;
   if(chunkLog < 10)
   {
      PrintFormat("  CHUNKED OK: %s — %d bars in %d chunks", baseKey, copied, totalChunks);
      chunkLog++;
   }

   return copied;
}

int ExportSymbolTF(string symbol, ENUM_TIMEFRAMES tf, int maxBars)
{
   MqlRates rates[];
   ArraySetAsSeries(rates, false);

   // Ensure symbol is selected in Market Watch (required for CopyRates to work)
   SymbolSelect(symbol, true);

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

   // Build CSV: one line per bar, no header, no repeated key names
   // Format: "2026-03-20T16:00:00Z,1.2345,1.2350,1.2340,1.2348,1234\n"
   // This is 60% smaller than JSON and O(n) to build (no key names repeated)
   int digits = (int)SymbolInfoInteger(symbol, SYMBOL_DIGITS);

   // Pre-allocate approximate size: ~60 chars per bar
   // MQL5 doesn't have StringBuilder but string concat with += is optimized for append
   string csv = "";
   // Reserve hint (MQL5 may or may not optimize based on this)
   StringInit(csv, copied * 65);
   csv = "";

   for(int i = 0; i < copied; i++)
   {
      MqlDateTime mdt;
      TimeToStruct(rates[i].time, mdt);

      csv += StringFormat("%04d-%02d-%02dT%02d:%02d:%02dZ",
         mdt.year, mdt.mon, mdt.day, mdt.hour, mdt.min, mdt.sec);
      csv += ",";
      csv += DoubleToString(rates[i].open, digits);
      csv += ",";
      csv += DoubleToString(rates[i].high, digits);
      csv += ",";
      csv += DoubleToString(rates[i].low, digits);
      csv += ",";
      csv += DoubleToString(rates[i].close, digits);
      csv += ",";
      csv += IntegerToString(rates[i].tick_volume);
      csv += "\n";
   }

   string key = "mt5:" + symbol + ":" + TFToStr(tf);

   int req = DatabasePrepare(g_db,
      "INSERT OR REPLACE INTO bar_cache (key, data, timestamp, bar_count) VALUES (?1, ?2, ?3, ?4)");
   if(req == INVALID_HANDLE)
   {
      PrintFormat("  prepare failed: %s (err %d)", key, GetLastError());
      return 0;
   }

   if(!DatabaseBind(req, 0, key) ||
      !DatabaseBind(req, 1, csv) ||
      !DatabaseBind(req, 2, (long)TimeCurrent()) ||
      !DatabaseBind(req, 3, (long)copied))
   {
      PrintFormat("  bind failed: %s (err %d, len=%d)", key, GetLastError(), StringLen(csv));
      DatabaseFinalize(req);
      return 0;
   }

   DatabaseRead(req);
   DatabaseFinalize(req);

   static int writeOkLog = 0;
   if(writeOkLog < 5)
   {
      PrintFormat("  DB WRITE OK: %s — %d bars, csv=%d chars", key, copied, StringLen(csv));
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
