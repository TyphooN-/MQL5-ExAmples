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
#property version   "1.100"
#property description "Writes bar data directly to SQLite database using MQL5 native DatabaseOpen."
#property description "v1.100: Incremental mode — only exports symbols/TFs that have new bars."
#property description "Full export runs ONCE on init, then only updates changed data."
#property strict

input int    UpdateIntervalSec = 30;     // Update interval (seconds) — 30s default to reduce I/O
input int    BarsPerTF         = 500;    // Bars per timeframe on incremental updates (recent only)
input int    FullExportBars    = 0;      // Full export bar count (0 = max history, only on first run)
input bool   MarketWatchOnly   = true;   // true = Market Watch only

// DB handle
int g_db = INVALID_HANDLE;

// Track last bar time per symbol:TF to skip unchanged data
string g_lastBarKeys[];    // "SYMBOL:TF" keys
datetime g_lastBarTimes[]; // last bar timestamp per key
int g_trackCount = 0;

// Timeframes: MN1 first (most useful immediately)
ENUM_TIMEFRAMES g_timeframes[] = {
   PERIOD_MN1, PERIOD_W1, PERIOD_D1, PERIOD_H4, PERIOD_H1,
   PERIOD_M30, PERIOD_M15, PERIOD_M5, PERIOD_M1
};

bool g_fullExportDone = false;  // only do full export once

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
   return "Unknown";
}

// Find or create tracking slot for a symbol:TF pair
int GetTrackIndex(string key)
{
   for(int i = 0; i < g_trackCount; i++)
      if(g_lastBarKeys[i] == key) return i;
   // New entry
   g_trackCount++;
   ArrayResize(g_lastBarKeys, g_trackCount);
   ArrayResize(g_lastBarTimes, g_trackCount);
   g_lastBarKeys[g_trackCount - 1] = key;
   g_lastBarTimes[g_trackCount - 1] = 0;
   return g_trackCount - 1;
}

int OnInit()
{
   string dbPath = "typhoon_mt5_cache.db";
   g_db = DatabaseOpen(dbPath, DATABASE_OPEN_READWRITE | DATABASE_OPEN_CREATE);
   if(g_db == INVALID_HANDLE)
   {
      PrintFormat("BarCacheWriter: failed to open database (error %d)", GetLastError());
      return INIT_FAILED;
   }

   if(!DatabaseExecute(g_db,
      "CREATE TABLE IF NOT EXISTS bar_cache ("
      "  key TEXT PRIMARY KEY,"
      "  data TEXT NOT NULL,"
      "  timestamp INTEGER NOT NULL,"
      "  bar_count INTEGER NOT NULL DEFAULT 0"
      ")"))
   {
      PrintFormat("BarCacheWriter: failed to create table (error %d)", GetLastError());
      DatabaseClose(g_db);
      g_db = INVALID_HANDLE;
      return INIT_FAILED;
   }

   DatabaseExecute(g_db, "PRAGMA journal_mode=WAL");
   DatabaseExecute(g_db, "PRAGMA synchronous=NORMAL");

   PrintFormat("BarCacheWriter v1.100: SQLite opened, %s symbols, update every %ds",
      MarketWatchOnly ? "Market Watch" : "ALL", UpdateIntervalSec);

   EventSetTimer(UpdateIntervalSec);

   // First run: full export (all bars)
   ExportAll(true);
   return INIT_SUCCEEDED;
}

void OnTimer()
{
   // Subsequent runs: incremental (only recent bars for changed symbols)
   ExportAll(false);
}

void ExportAll(bool fullExport)
{
   if(g_db == INVALID_HANDLE) return;

   int symCount = SymbolsTotal(MarketWatchOnly);
   int exported = 0;
   int skipped = 0;
   int totalBars = 0;

   DatabaseExecute(g_db, "BEGIN TRANSACTION");

   // Write symbol list
   WriteSymbolList(symCount);

   for(int i = 0; i < symCount; i++)
   {
      string symbol = SymbolName(i, MarketWatchOnly);
      if(StringLen(symbol) == 0) continue;

      for(int tf = 0; tf < ArraySize(g_timeframes); tf++)
      {
         string trackKey = symbol + ":" + TFToStr(g_timeframes[tf]);
         int trackIdx = GetTrackIndex(trackKey);

         // Check if this symbol:TF has new bars since last export
         MqlRates lastRate[];
         if(CopyRates(symbol, g_timeframes[tf], 0, 1, lastRate) == 1)
         {
            if(!fullExport && lastRate[0].time == g_lastBarTimes[trackIdx])
            {
               skipped++;
               continue; // no new bar — skip
            }
         }

         int barsToFetch = fullExport ? (FullExportBars > 0 ? FullExportBars : 0) : BarsPerTF;
         int bars = ExportSymbolTF(symbol, g_timeframes[tf], barsToFetch);
         if(bars > 0)
         {
            exported++;
            totalBars += bars;
            // Update tracking
            if(CopyRates(symbol, g_timeframes[tf], 0, 1, lastRate) == 1)
               g_lastBarTimes[trackIdx] = lastRate[0].time;
         }
      }
   }

   DatabaseExecute(g_db, "COMMIT");

   // Always log on full export, periodically on incremental
   static datetime lastLog = 0;
   if(fullExport || TimeCurrent() - lastLog > 300)
   {
      PrintFormat("BarCacheWriter: %s — %d exported, %d skipped (unchanged), %d bars",
         fullExport ? "FULL EXPORT" : "incremental",
         exported, skipped, totalBars);
      lastLog = TimeCurrent();
   }
}

void WriteSymbolList(int symCount)
{
   string json = "[";
   for(int i = 0; i < symCount; i++)
   {
      string s = SymbolName(i, MarketWatchOnly);
      if(StringLen(s) == 0) continue;
      if(i > 0) json += ",";
      json += "\"" + s + "\"";
   }
   json += "]";

   int req = DatabasePrepare(g_db,
      "INSERT OR REPLACE INTO bar_cache (key, data, timestamp, bar_count) VALUES (?1, ?2, ?3, ?4)");
   if(req != INVALID_HANDLE)
   {
      DatabaseBind(req, 0, "mt5:__SYMBOLS__");
      DatabaseBind(req, 1, json);
      DatabaseBind(req, 2, (long)TimeCurrent());
      DatabaseBind(req, 3, (long)symCount);
      DatabaseRead(req);
      DatabaseFinalize(req);
   }
}

int ExportSymbolTF(string symbol, ENUM_TIMEFRAMES tf, int maxBars)
{
   MqlRates rates[];
   ArraySetAsSeries(rates, false);

   int copied;
   if(maxBars <= 0)
      copied = CopyRates(symbol, tf, D'1970.01.01 00:00', TimeCurrent(), rates);
   else
      copied = CopyRates(symbol, tf, 0, maxBars, rates);
   if(copied <= 0) return 0;

   // Build JSON — concatenate strings (no StringFormat for large data)
   int digits = (int)SymbolInfoInteger(symbol, SYMBOL_DIGITS);
   string json = "[";

   for(int i = 0; i < copied; i++)
   {
      if(i > 0) json += ",";

      MqlDateTime mdt;
      TimeToStruct(rates[i].time, mdt);

      json += "{\"timestamp\":\"";
      json += StringFormat("%04d-%02d-%02dT%02d:%02d:%02dZ", mdt.year, mdt.mon, mdt.day, mdt.hour, mdt.min, mdt.sec);
      json += "\",\"open\":";
      json += DoubleToString(rates[i].open, digits);
      json += ",\"high\":";
      json += DoubleToString(rates[i].high, digits);
      json += ",\"low\":";
      json += DoubleToString(rates[i].low, digits);
      json += ",\"close\":";
      json += DoubleToString(rates[i].close, digits);
      json += ",\"volume\":";
      json += IntegerToString(rates[i].tick_volume);
      json += "}";
   }
   json += "]";

   string key = "mt5:" + symbol + ":" + TFToStr(tf);
   long timestamp = (long)TimeCurrent();

   int req = DatabasePrepare(g_db,
      "INSERT OR REPLACE INTO bar_cache (key, data, timestamp, bar_count) VALUES (?1, ?2, ?3, ?4)");
   if(req == INVALID_HANDLE)
   {
      PrintFormat("BarCacheWriter: prepare failed for %s (error %d)", key, GetLastError());
      return 0;
   }

   if(!DatabaseBind(req, 0, key))
      { PrintFormat("  bind 0 failed: %d", GetLastError()); DatabaseFinalize(req); return 0; }
   if(!DatabaseBind(req, 1, json))
      { PrintFormat("  bind 1 failed: %d (len=%d)", GetLastError(), StringLen(json)); DatabaseFinalize(req); return 0; }
   if(!DatabaseBind(req, 2, timestamp))
      { PrintFormat("  bind 2 failed: %d", GetLastError()); DatabaseFinalize(req); return 0; }
   if(!DatabaseBind(req, 3, (long)copied))
      { PrintFormat("  bind 3 failed: %d", GetLastError()); DatabaseFinalize(req); return 0; }

   DatabaseRead(req);
   DatabaseFinalize(req);

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
   Print("BarCacheWriter: stopped, database closed");
}
