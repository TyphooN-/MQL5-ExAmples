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
#property version   "1.000"
#property description "Writes bar data directly to SQLite database using MQL5 native DatabaseOpen."
#property description "Shares cache with TyphooN-Terminal — zero CSV, zero file IPC."
#property description "DB location: MQL5/Files/typhoon_mt5_cache.db"
#property description "TyphooN-Terminal reads this DB via Rust (full filesystem access to Wine paths)."
#property strict

input int    UpdateIntervalSec = 10;     // Update interval (seconds)
input bool   MaxBars           = true;   // true = export ALL available bars
input int    MaxBarsOverride   = 0;      // >0 overrides with fixed limit
input bool   MarketWatchOnly   = true;   // true = Market Watch only

// DB handle
int g_db = INVALID_HANDLE;

// Timeframes: MN1 first (most useful immediately)
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
   return "Unknown";
}

int OnInit()
{
   // Open/create SQLite database in MQL5/Files/ (MQL5 sandbox)
   string dbPath = "typhoon_mt5_cache.db";
   g_db = DatabaseOpen(dbPath, DATABASE_OPEN_READWRITE | DATABASE_OPEN_CREATE);
   if(g_db == INVALID_HANDLE)
   {
      PrintFormat("BarCacheWriter: failed to open database: %s (error %d)", dbPath, GetLastError());
      return INIT_FAILED;
   }

   // Create table matching TyphooN-Terminal's bar_cache schema
   // key = "mt5:SYMBOL:TIMEFRAME"
   // data = raw bar data as TEXT (JSON array — no binary packing, simpler from MQL5)
   // timestamp = unix epoch seconds
   // bar_count = number of bars
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

   // Enable WAL mode for concurrent read/write (TyphooN-Terminal reads while we write)
   DatabaseExecute(g_db, "PRAGMA journal_mode=WAL");
   DatabaseExecute(g_db, "PRAGMA synchronous=NORMAL");

   PrintFormat("BarCacheWriter v1.000: SQLite opened at MQL5/Files/%s (WAL mode)", dbPath);
   PrintFormat("BarCacheWriter: %s symbols, updating every %ds",
      MarketWatchOnly ? "Market Watch" : "ALL", UpdateIntervalSec);

   EventSetTimer(UpdateIntervalSec);

   // Initial export
   ExportAll();
   return INIT_SUCCEEDED;
}

void OnTimer()
{
   ExportAll();
}

void ExportAll()
{
   if(g_db == INVALID_HANDLE) return;

   int symCount = SymbolsTotal(MarketWatchOnly);
   int exported = 0;
   int totalBars = 0;

   // Begin transaction for bulk insert (much faster than individual inserts)
   DatabaseExecute(g_db, "BEGIN TRANSACTION");

   // Write symbol list to a special key so TyphooN-Terminal can discover all symbols
   string symListJson = "[";
   for(int si = 0; si < symCount; si++)
   {
      string s = SymbolName(si, MarketWatchOnly);
      if(StringLen(s) == 0) continue;
      if(si > 0) symListJson += ",";
      symListJson += "\"" + s + "\"";
   }
   symListJson += "]";
   {
      int req = DatabasePrepare(g_db,
         "INSERT OR REPLACE INTO bar_cache (key, data, timestamp, bar_count) VALUES (?1, ?2, ?3, ?4)");
      if(req != INVALID_HANDLE)
      {
         DatabaseBind(req, 0, "mt5:__SYMBOLS__");
         DatabaseBind(req, 1, symListJson);
         DatabaseBind(req, 2, (long)TimeCurrent());
         DatabaseBind(req, 3, (long)symCount);
         DatabaseRead(req);
         DatabaseFinalize(req);
      }
   }

   for(int i = 0; i < symCount; i++)
   {
      string symbol = SymbolName(i, MarketWatchOnly);
      if(StringLen(symbol) == 0) continue;

      for(int tf = 0; tf < ArraySize(g_timeframes); tf++)
      {
         int bars = ExportSymbolTF(symbol, g_timeframes[tf]);
         if(bars > 0)
         {
            exported++;
            totalBars += bars;
         }
      }
   }

   DatabaseExecute(g_db, "COMMIT");

   // Log periodically
   static datetime lastLog = 0;
   if(TimeCurrent() - lastLog > 300)
   {
      PrintFormat("BarCacheWriter: %d symbols × %d TFs = %d entries, %d total bars",
         symCount, ArraySize(g_timeframes), exported, totalBars);
      lastLog = TimeCurrent();
   }
}

int ExportSymbolTF(string symbol, ENUM_TIMEFRAMES tf)
{
   MqlRates rates[];
   ArraySetAsSeries(rates, false);

   int copied;
   if(MaxBars && MaxBarsOverride <= 0)
      copied = CopyRates(symbol, tf, D'1970.01.01 00:00', TimeCurrent(), rates);
   else
   {
      int limit = (MaxBarsOverride > 0) ? MaxBarsOverride : 10000;
      copied = CopyRates(symbol, tf, 0, limit, rates);
   }
   if(copied <= 0) return 0;

   // Build JSON array of bars
   // Format: [{"t":"2026-03-20T16:00:00Z","o":1.2345,"h":1.2350,"l":1.2340,"c":1.2348,"v":1234}, ...]
   string json = "[";
   int digits = (int)SymbolInfoInteger(symbol, SYMBOL_DIGITS);

   for(int i = 0; i < copied; i++)
   {
      if(i > 0) json += ",";

      MqlDateTime mdt;
      TimeToStruct(rates[i].time, mdt);
      string ts = StringFormat("%04d-%02d-%02dT%02d:%02d:%02dZ",
         mdt.year, mdt.mon, mdt.day, mdt.hour, mdt.min, mdt.sec);

      json += StringFormat("{\"timestamp\":\"%s\",\"open\":%s,\"high\":%s,\"low\":%s,\"close\":%s,\"volume\":%d}",
         ts,
         DoubleToString(rates[i].open, digits),
         DoubleToString(rates[i].high, digits),
         DoubleToString(rates[i].low, digits),
         DoubleToString(rates[i].close, digits),
         rates[i].tick_volume);
   }
   json += "]";

   // Upsert into SQLite
   string key = "mt5:" + symbol + ":" + TFToStr(tf);
   long timestamp = (long)TimeCurrent();

   string sql = StringFormat(
      "INSERT OR REPLACE INTO bar_cache (key, data, timestamp, bar_count) VALUES ('%s', '%s', %d, %d)",
      key, json, timestamp, copied);

   if(!DatabaseExecute(g_db, sql))
   {
      // JSON might contain single quotes — use parameterized approach
      // Fall back to prepare/bind
      int req = DatabasePrepare(g_db,
         "INSERT OR REPLACE INTO bar_cache (key, data, timestamp, bar_count) VALUES (?1, ?2, ?3, ?4)");
      if(req != INVALID_HANDLE)
      {
         DatabaseBind(req, 0, key);
         DatabaseBind(req, 1, json);
         DatabaseBind(req, 2, timestamp);
         DatabaseBind(req, 3, (long)copied);
         DatabaseRead(req); // execute
         DatabaseFinalize(req);
      }
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
   Print("BarCacheWriter: stopped, database closed");
}
