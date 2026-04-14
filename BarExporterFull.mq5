/**=             BarExporterFull.mq5  (TyphooN's Full History Bar Exporter)
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
#property description "Downloads FULL bar history from broker for ALL symbols, then exports."
#property description "Run once to seed the cache, then use BarExporter.mq5 for updates."
#property description "Downloads from 1970-01-01 to present for max history depth."
#property strict

input bool   MarketWatchOnly = true;  // true = Market Watch symbols only
input string CustomExportPath = "";   // Empty = auto (MQL5/Files/typhoon-bars/)

// Ordered MN1→M1: higher TFs export first (immediately useful for analysis)
ENUM_TIMEFRAMES g_timeframes[] = {
   PERIOD_MN1, PERIOD_W1, PERIOD_D1, PERIOD_H4, PERIOD_H1,
   PERIOD_M30, PERIOD_M15, PERIOD_M5, PERIOD_M1
};

// Init-time caches — avoid recomputing constants inside hot export loop.
string g_exportDir = "";
string g_tfStrings[];   // parallel to g_timeframes, built once in OnInit
int    g_tfCount = 0;

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

string FormatRFC3339(datetime dt)
{
   MqlDateTime mdt;
   TimeToStruct(dt, mdt);
   return StringFormat("%04d-%02d-%02dT%02d:%02d:%02dZ",
      mdt.year, mdt.mon, mdt.day, mdt.hour, mdt.min, mdt.sec);
}

int OnInit()
{
   g_exportDir = (CustomExportPath != "") ? CustomExportPath : "typhoon-bars";
   g_tfCount = ArraySize(g_timeframes);
   ArrayResize(g_tfStrings, g_tfCount);
   for(int i = 0; i < g_tfCount; i++)
      g_tfStrings[i] = TFToTerminalString(g_timeframes[i]);
   FolderCreate(g_exportDir, 0);

   PrintFormat("BarExporterFull v1.001: downloading FULL history for %s symbols",
      MarketWatchOnly ? "Market Watch" : "ALL");

   // Run on timer so MT5 has time to initialize
   EventSetTimer(2);
   return INIT_SUCCEEDED;
}

void OnTimer()
{
   EventKillTimer(); // one-shot

   int symCount = SymbolsTotal(MarketWatchOnly);
   PrintFormat("BarExporterFull: processing %d symbols × %d timeframes...",
      symCount, g_tfCount);

   int totalExported = 0;
   int totalBars = 0;
   // Hoist TimeCurrent() — a single "now" for the entire download session; drift
   // across ~minutes of per-symbol downloads is immaterial for historical bars.
   datetime nowTs = TimeCurrent();

   for(int i = 0; i < symCount; i++)
   {
      string symbol = SymbolName(i, MarketWatchOnly);
      if(StringLen(symbol) == 0) continue;

      // Hoist digits once per symbol — 7659 SymbolInfoInteger calls → 851.
      int digits = (int)SymbolInfoInteger(symbol, SYMBOL_DIGITS);

      for(int tf = 0; tf < g_tfCount; tf++)
      {
         // Download full history and export in one pass — avoids redundant CopyRates
         MqlRates rates[];
         ArraySetAsSeries(rates, false);
         int bars = CopyRates(symbol, g_timeframes[tf], D'1970.01.01 00:00', nowTs, rates);
         if(bars <= 0) continue;

         if(ExportRates(symbol, g_tfStrings[tf], rates, bars, digits))
         {
            totalExported++;
            totalBars += bars;
         }

         PrintFormat("  %s @ %s: %d bars exported", symbol, g_tfStrings[tf], bars);
      }
   }

   PrintFormat("BarExporterFull: DONE — %d files, %d total bars exported to MQL5/Files/%s/",
      totalExported, totalBars, g_exportDir);

   // Alert user
   Alert(StringFormat("BarExporterFull: exported %d files (%d bars) for %d symbols",
      totalExported, totalBars, symCount));
}

// Export pre-copied rates to CSV — single CopyRates call in caller, no redundant copy
bool ExportRates(string symbol, const string tfStr, const MqlRates &rates[], int count, int digits)
{
   string filename = g_exportDir + "/" + symbol + "_" + tfStr + ".csv";

   int handle = FileOpen(filename, FILE_WRITE|FILE_CSV|FILE_ANSI, ',');
   if(handle == INVALID_HANDLE) return false;

   FileWrite(handle, "timestamp", "open", "high", "low", "close", "volume");

   for(int i = 0; i < count; i++)
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

void OnDeinit(const int reason)
{
   EventKillTimer();
}
