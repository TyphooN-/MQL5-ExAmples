/**=        HistoryIntegrityCheck.mq5  (TyphooN's History Integrity Checker)
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
#property copyright   "Copyright 2026 TyphooN (MarketWizardry.org)"
#property link        "https://www.marketwizardry.info"
#property version     "1.000"
#property description "Export MN1 history summary for ALL broker symbols (not just Market Watch)"
#property description "Checks every symbol on the server for history integrity"
#property strict
#property script_show_inputs
input bool ExportStocksOnly = true;   // Only export stock symbols
input int  BatchSleepMs     = 100;    // Sleep between batches (ms) to let server respond
input int  BatchSize        = 50;     // Symbols per batch before sleep
void OnStart()
{
   string server_name = AccountInfoString(ACCOUNT_SERVER);
   string server_date = TimeToString(TimeCurrent(), TIME_DATE);
   string filename = StringFormat("HistoryCheck-%s-%s.csv", server_name, server_date);
   int handle = FileOpen(filename, FILE_WRITE | FILE_CSV | FILE_ANSI, ';');
   if (handle == INVALID_HANDLE)
   {
      PrintFormat("ERROR: Cannot open file %s for writing", filename);
      return;
   }
   FileWrite(handle, "Symbol", "BrokerATH", "BrokerATL", "MN1Bars", "EarliestDate", "LatestDate",
             "CurrentBid", "Sector", "Industry", "Description");
   // Use false to get ALL symbols from broker, not just Market Watch
   int total_symbols = SymbolsTotal(false);
   int symbols_exported = 0;
   int symbols_skipped = 0;
   int symbols_no_data = 0;
   // Track which symbols we added to Market Watch so we can clean up
   string added_symbols[];
   int added_count = 0;
   PrintFormat("Scanning ALL %d symbols on server for history integrity...", total_symbols);
   for (int i = 0; i < total_symbols; i++)
   {
      string symbol = SymbolName(i, false);  // false = all server symbols
      if (symbol == "") continue;
      if (ExportStocksOnly)
      {
         long sector_id = SymbolInfoInteger(symbol, SYMBOL_SECTOR);
         if (sector_id == SECTOR_UNDEFINED || sector_id == SECTOR_CURRENCY ||
             sector_id == SECTOR_CURRENCY_CRYPTO || sector_id == SECTOR_INDEXES ||
             sector_id == SECTOR_COMMODITIES || sector_id == SECTOR_ENERGY)
         {
            symbols_skipped++;
            continue;
         }
      }
      // Must select symbol to Market Watch to request data from server
      bool was_selected = SymbolInfoInteger(symbol, SYMBOL_SELECT);
      if (!was_selected)
      {
         SymbolSelect(symbol, true);
         ArrayResize(added_symbols, added_count + 1);
         added_symbols[added_count] = symbol;
         added_count++;
      }
      // Request full MN1 history from server
      MqlRates rates[];
      datetime start_time = D'1970.01.01 00:00';
      datetime end_time = TimeCurrent();
      // First call triggers server download, may need retry
      int bars = CopyRates(symbol, PERIOD_MN1, start_time, end_time, rates);
      if (bars <= 0)
      {
         // Retry once after brief pause — server may need time to deliver
         Sleep(200);
         bars = CopyRates(symbol, PERIOD_MN1, start_time, end_time, rates);
      }
      if (bars <= 0)
      {
         symbols_no_data++;
         // Still record it — 0 bars is useful info
         string sector = SymbolInfoString(symbol, SYMBOL_SECTOR_NAME);
         string industry = SymbolInfoString(symbol, SYMBOL_INDUSTRY_NAME);
         string desc = SymbolInfoString(symbol, SYMBOL_DESCRIPTION);
         double bid = SymbolInfoDouble(symbol, SYMBOL_BID);
         int digits = (int)SymbolInfoInteger(symbol, SYMBOL_DIGITS);
         FileWrite(handle, symbol, "0", "0", "0", "", "",
                   DoubleToString(bid, digits), sector, industry, desc);
         continue;
      }
      double ath = -DBL_MAX;
      double atl = DBL_MAX;
      for (int b = 0; b < bars; b++)
      {
         if (rates[b].high > ath) ath = rates[b].high;
         if (rates[b].low < atl && rates[b].low > 0) atl = rates[b].low;
      }
      string earliest = TimeToString(rates[0].time, TIME_DATE);
      string latest = TimeToString(rates[bars - 1].time, TIME_DATE);
      double bid = SymbolInfoDouble(symbol, SYMBOL_BID);
      string sector = SymbolInfoString(symbol, SYMBOL_SECTOR_NAME);
      string industry = SymbolInfoString(symbol, SYMBOL_INDUSTRY_NAME);
      string desc = SymbolInfoString(symbol, SYMBOL_DESCRIPTION);
      int digits = (int)SymbolInfoInteger(symbol, SYMBOL_DIGITS);
      FileWrite(handle, symbol,
                DoubleToString(ath, digits),
                DoubleToString(atl, digits),
                IntegerToString(bars),
                earliest, latest,
                DoubleToString(bid, digits),
                sector, industry, desc);
      symbols_exported++;
      if (symbols_exported % BatchSize == 0)
      {
         PrintFormat("Checked %d symbols so far (%d exported, %d no data, %d skipped)...",
                     i + 1, symbols_exported, symbols_no_data, symbols_skipped);
         Sleep(BatchSleepMs);
      }
   }
   FileClose(handle);
   // Clean up — remove symbols we added to Market Watch
   for (int j = 0; j < added_count; j++)
   {
      SymbolSelect(added_symbols[j], false);
   }
   PrintFormat("=== HISTORY INTEGRITY CHECK COMPLETE ===");
   PrintFormat("Total server symbols: %d", total_symbols);
   PrintFormat("Exported with data:   %d", symbols_exported);
   PrintFormat("No MN1 data:          %d", symbols_no_data);
   PrintFormat("Skipped (filtered):   %d", symbols_skipped);
   PrintFormat("Added/removed from Market Watch: %d", added_count);
   PrintFormat("File: MQL5/Files/%s", filename);
}
