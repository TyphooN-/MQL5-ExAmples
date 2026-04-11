/**=             BarServer.mq5  (TyphooN's Bar Data Server for TyphooN-Terminal)
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
#property description "Listens for bar data requests from TyphooN-Terminal."
#property description "TyphooN-Terminal writes a request file, EA reads it, exports bars, writes response."
#property description "Enables on-demand bar fetching without bulk export."
#property strict

input int PollIntervalMs = 250;  // How often to check for requests (milliseconds)

// File-based IPC:
//   Request:  typhoon-bars/REQUEST.txt  — format: "SYMBOL,TIMEFRAME,LIMIT"
//   Response: typhoon-bars/SYMBOL_TIMEFRAME.csv — the bar data
//   Done:     typhoon-bars/RESPONSE.txt — "OK SYMBOL TIMEFRAME BARS_COUNT"

string GetBaseDir() { return "typhoon-bars"; }

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

ENUM_TIMEFRAMES TerminalStringToTF(string tf)
{
   if(tf == "1Min")   return PERIOD_M1;
   if(tf == "5Min")   return PERIOD_M5;
   if(tf == "15Min")  return PERIOD_M15;
   if(tf == "30Min")  return PERIOD_M30;
   if(tf == "1Hour")  return PERIOD_H1;
   if(tf == "4Hour")  return PERIOD_H4;
   if(tf == "1Day")   return PERIOD_D1;
   if(tf == "1Week")  return PERIOD_W1;
   if(tf == "1Month") return PERIOD_MN1;
   return PERIOD_D1; // default
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
   FolderCreate(GetBaseDir(), 0);
   // Use millisecond timer for fast response
   EventSetMillisecondTimer(PollIntervalMs);
   Print("BarServer v1.000: listening for requests in MQL5/Files/", GetBaseDir(), "/");
   return INIT_SUCCEEDED;
}

void OnTimer()
{
   ProcessRequests();
}

void ProcessRequests()
{
   string reqFile = GetBaseDir() + "/REQUEST.txt";

   // Check if request file exists
   if(!FileIsExist(reqFile)) return;

   // Read request
   int handle = FileOpen(reqFile, FILE_READ|FILE_TXT|FILE_ANSI);
   if(handle == INVALID_HANDLE) return;

   string line = "";
   while(!FileIsEnding(handle))
   {
      string req = FileReadString(handle);
      if(StringLen(req) > 0) line = req;
   }
   FileClose(handle);

   // Delete request file immediately (prevents re-processing)
   if(!FileDelete(reqFile))
      PrintFormat("BarServer: WARNING — failed to delete %s (error %d), may re-process", reqFile, GetLastError());

   if(StringLen(line) == 0) return;

   // Parse: "SYMBOL,TIMEFRAME,LIMIT" or "SYMBOL,TIMEFRAME" (default max bars)
   string parts[];
   int count = StringSplit(line, ',', parts);
   if(count < 2)
   {
      WriteResponse("ERR Invalid request format: " + line);
      return;
   }

   string symbol = parts[0];
   StringTrimLeft(symbol); StringTrimRight(symbol);
   string tfStr = parts[1];
   StringTrimLeft(tfStr); StringTrimRight(tfStr);
   int limit = (count >= 3) ? (int)StringToInteger(parts[2]) : 0; // 0 = max

   ENUM_TIMEFRAMES tf = TerminalStringToTF(tfStr);

   PrintFormat("BarServer: request %s @ %s (limit=%d)", symbol, tfStr, limit);

   // Fetch bars
   MqlRates rates[];
   ArraySetAsSeries(rates, false);

   int copied;
   if(limit <= 0)
      copied = CopyRates(symbol, tf, D'1970.01.01 00:00', TimeCurrent(), rates);
   else
      copied = CopyRates(symbol, tf, 0, limit, rates);

   if(copied <= 0)
   {
      WriteResponse("ERR No data for " + symbol + " @ " + tfStr);
      return;
   }

   // Export to CSV
   string filename = GetBaseDir() + "/" + symbol + "_" + tfStr + ".csv";
   int fh = FileOpen(filename, FILE_WRITE|FILE_CSV|FILE_ANSI, ',');
   if(fh == INVALID_HANDLE)
   {
      WriteResponse("ERR Cannot write " + filename);
      return;
   }

   FileWrite(fh, "timestamp", "open", "high", "low", "close", "volume");
   int digits = (int)SymbolInfoInteger(symbol, SYMBOL_DIGITS);

   for(int i = 0; i < copied; i++)
   {
      FileWrite(fh,
         FormatRFC3339(rates[i].time),
         DoubleToString(rates[i].open, digits),
         DoubleToString(rates[i].high, digits),
         DoubleToString(rates[i].low, digits),
         DoubleToString(rates[i].close, digits),
         IntegerToString(rates[i].tick_volume)
      );
   }
   FileClose(fh);

   // Write response
   WriteResponse(StringFormat("OK %s %s %d", symbol, tfStr, copied));
   PrintFormat("BarServer: served %d bars for %s @ %s", copied, symbol, tfStr);
}

void WriteResponse(string msg)
{
   string respFile = GetBaseDir() + "/RESPONSE.txt";
   int handle = FileOpen(respFile, FILE_WRITE|FILE_TXT|FILE_ANSI);
   if(handle != INVALID_HANDLE)
   {
      FileWriteString(handle, msg);
      FileClose(handle);
   }
}

void OnDeinit(const int reason)
{
   EventKillTimer();
   Print("BarServer: stopped");
}
