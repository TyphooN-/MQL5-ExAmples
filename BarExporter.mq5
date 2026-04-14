/**=             BarExporter.mq5  (TyphooN's Bar Data Exporter for TyphooN-Terminal)
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
#property version   "1.001"
#property description "Exports bar data for all symbols to CSV for TyphooN-Terminal import."
#property description "Runs on timer — keeps bars updated for real-time charting."
#property description "Output: ~/.config/typhoon-terminal/mt5-bars/ (or MQL5/Files/typhoon-bars/)"
#property strict

input int    UpdateIntervalSec = 10;     // Update interval (seconds)
input bool   MaxBars           = true;   // true = export ALL available bars (max history)
input int    MaxBarsOverride   = 0;      // >0 overrides MaxBars with fixed limit per TF
input bool   MarketWatchOnly   = true;   // true = Market Watch symbols only
input string CustomExportPath  = "";     // Empty = auto (MQL5/Files/typhoon-bars/)

// All timeframes — ordered MN1→M1 so higher TFs export first (immediately useful)
ENUM_TIMEFRAMES g_timeframes[] = {
   PERIOD_MN1, PERIOD_W1, PERIOD_D1, PERIOD_H4, PERIOD_H1,
   PERIOD_M30, PERIOD_M15, PERIOD_M5, PERIOD_M1
};

// Init-time caches — avoid recomputing constants inside hot export loop.
string g_exportDir = "";
string g_tfStrings[];   // parallel to g_timeframes, built once in OnInit
int    g_tfCount = 0;

// Map MT5 timeframe enum to TyphooN-Terminal timeframe string
string TFToTerminalString(ENUM_TIMEFRAMES tf)
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
   g_exportDir = (CustomExportPath != "") ? CustomExportPath : "typhoon-bars";
   g_tfCount = ArraySize(g_timeframes);
   ArrayResize(g_tfStrings, g_tfCount);
   for(int i = 0; i < g_tfCount; i++)
      g_tfStrings[i] = TFToTerminalString(g_timeframes[i]);
   FolderCreate(g_exportDir, 0);

   PrintFormat("BarExporter v1.001: exporting to MQL5/Files/%s/ every %ds", g_exportDir, UpdateIntervalSec);
   PrintFormat("BarExporter: %s symbols, maxBars=%s, overrideCap=%d, %d timeframes",
      MarketWatchOnly ? "Market Watch" : "ALL",
      MaxBars ? "ALL" : "10000",
      MaxBarsOverride,
      g_tfCount);

   EventSetTimer(UpdateIntervalSec);

   // Initial full export
   ExportAll();
   return INIT_SUCCEEDED;
}

void OnTimer()
{
   ExportAll();
}

void ExportAll()
{
   int symCount = SymbolsTotal(MarketWatchOnly);
   int exported = 0;
   int errors = 0;
   datetime nowTs = TimeCurrent();

   for(int i = 0; i < symCount; i++)
   {
      string symbol = SymbolName(i, MarketWatchOnly);
      if(StringLen(symbol) == 0) continue;

      // Hoist digits: one SymbolInfoInteger call per symbol instead of per (symbol,TF).
      int digits = (int)SymbolInfoInteger(symbol, SYMBOL_DIGITS);

      for(int tf = 0; tf < g_tfCount; tf++)
      {
         if(ExportSymbolTF(symbol, g_timeframes[tf], g_tfStrings[tf], digits, nowTs))
            exported++;
         else
            errors++;
      }
   }

   // Only log periodically (not every 10s) to avoid spam
   static datetime lastLog = 0;
   if(nowTs - lastLog > 300) // log every 5 min
   {
      PrintFormat("BarExporter: %d symbols × %d TFs = %d exports (%d skipped)",
         symCount, g_tfCount, exported, errors);
      lastLog = nowTs;
   }
}

bool ExportSymbolTF(string symbol, ENUM_TIMEFRAMES tf, const string tfStr, int digits, datetime nowTs)
{
   MqlRates rates[];
   ArraySetAsSeries(rates, false);

   int copied;
   if(MaxBars && MaxBarsOverride <= 0)
   {
      // Max history: request from 1970 to now (server returns all available)
      copied = CopyRates(symbol, tf, D'1970.01.01 00:00', nowTs, rates);
   }
   else
   {
      int limit = (MaxBarsOverride > 0) ? MaxBarsOverride : 10000;
      copied = CopyRates(symbol, tf, 0, limit, rates);
   }
   if(copied <= 0) return false;

   // Filename: typhoon-bars/EURUSD_1Hour.csv
   string filename = g_exportDir + "/" + symbol + "_" + tfStr + ".csv";

   int handle = FileOpen(filename, FILE_WRITE|FILE_CSV|FILE_ANSI, ',');
   if(handle == INVALID_HANDLE) return false;

   // Header row — matches TyphooN-Terminal Bar struct fields
   FileWrite(handle, "timestamp", "open", "high", "low", "close", "volume");

   for(int i = 0; i < copied; i++)
   {
      FileWrite(handle,
         FormatRFC3339(rates[i].time),
         DoubleToString(rates[i].open, digits),
         DoubleToString(rates[i].high, digits),
         DoubleToString(rates[i].low, digits),
         DoubleToString(rates[i].close, digits),
         IntegerToString(rates[i].tick_volume)
      );
   }

   FileClose(handle);
   return true;
}

// Convert MT5 datetime to RFC3339 string: "2026-03-20T16:00:00Z"
string FormatRFC3339(datetime dt)
{
   MqlDateTime mdt;
   TimeToStruct(dt, mdt);
   return StringFormat("%04d-%02d-%02dT%02d:%02d:%02dZ",
      mdt.year, mdt.mon, mdt.day, mdt.hour, mdt.min, mdt.sec);
}

void OnDeinit(const int reason)
{
   EventKillTimer();
   Print("BarExporter: stopped");
}
