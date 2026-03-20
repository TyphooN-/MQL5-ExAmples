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
#property version   "1.000"
#property description "Downloads FULL bar history from broker for ALL symbols, then exports."
#property description "Run once to seed the cache, then use BarExporter.mq5 for updates."
#property description "Downloads from 1970-01-01 to present for max history depth."
#property strict

input bool   MarketWatchOnly = true;  // true = Market Watch symbols only
input string CustomExportPath = "";   // Empty = auto (MQL5/Files/typhoon-bars/)

ENUM_TIMEFRAMES g_timeframes[] = {
   PERIOD_M1, PERIOD_M5, PERIOD_M15, PERIOD_M30,
   PERIOD_H1, PERIOD_H4, PERIOD_D1, PERIOD_W1, PERIOD_MN1
};

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

string GetExportDir()
{
   if(CustomExportPath != "")
      return CustomExportPath;
   return "typhoon-bars";
}

int OnInit()
{
   PrintFormat("BarExporterFull v1.000: downloading FULL history for %s symbols",
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
      symCount, ArraySize(g_timeframes));

   int totalExported = 0;
   int totalBars = 0;

   for(int i = 0; i < symCount; i++)
   {
      string symbol = SymbolName(i, MarketWatchOnly);
      if(StringLen(symbol) == 0) continue;

      for(int tf = 0; tf < ArraySize(g_timeframes); tf++)
      {
         // Step 1: Force download full history from broker
         int bars = DownloadHistory(symbol, g_timeframes[tf]);
         if(bars <= 0) continue;

         // Step 2: Export to CSV
         if(ExportSymbolTF(symbol, g_timeframes[tf], bars))
         {
            totalExported++;
            totalBars += bars;
         }

         PrintFormat("  %s @ %s: %d bars exported",
            symbol, TFToTerminalString(g_timeframes[tf]), bars);
      }
   }

   PrintFormat("BarExporterFull: DONE — %d files, %d total bars exported to MQL5/Files/%s/",
      totalExported, totalBars, GetExportDir());

   // Alert user
   Alert(StringFormat("BarExporterFull: exported %d files (%d bars) for %d symbols",
      totalExported, totalBars, symCount));
}

// Download full history from broker (1970 to now)
int DownloadHistory(string symbol, ENUM_TIMEFRAMES tf)
{
   MqlRates rates[];
   ArraySetAsSeries(rates, false);

   datetime start_time = D'1970.01.01 00:00';
   datetime end_time = TimeCurrent();

   // CopyRates with full date range triggers MT5 to download from server
   int bars = CopyRates(symbol, tf, start_time, end_time, rates);
   return bars;
}

bool ExportSymbolTF(string symbol, ENUM_TIMEFRAMES tf, int barCount)
{
   MqlRates rates[];
   ArraySetAsSeries(rates, false);

   // Re-copy after download (may have more bars now)
   int copied = CopyRates(symbol, tf, 0, barCount, rates);
   if(copied <= 0) return false;

   string tfStr = TFToTerminalString(tf);
   string filename = GetExportDir() + "/" + symbol + "_" + tfStr + ".csv";

   // Create directory
   FolderCreate(GetExportDir(), 0);

   int handle = FileOpen(filename, FILE_WRITE|FILE_CSV|FILE_ANSI, ',');
   if(handle == INVALID_HANDLE) return false;

   FileWrite(handle, "timestamp", "open", "high", "low", "close", "volume");

   int digits = (int)SymbolInfoInteger(symbol, SYMBOL_DIGITS);

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

void OnDeinit(const int reason)
{
   EventKillTimer();
}
