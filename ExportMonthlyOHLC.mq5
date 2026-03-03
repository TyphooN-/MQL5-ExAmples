/**=        ExportMonthlyOHLC.mq5  (TyphooN's Monthly OHLC Exporter)
 *               Copyright 2023, TyphooN (https://www.marketwizardry.org/)
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
#property copyright   "Copyright 2024 TyphooN (MarketWizardry.org)"
#property link        "https://www.marketwizardry.info"
#property version     "1.000"
#property description "Export Monthly OHLC data for all symbols to CSV (for data integrity comparison)"
#property strict
#property script_show_inputs
input bool ExportStocksOnly = true; // Only export stock symbols
void OnStart()
{
   string server_name = AccountInfoString(ACCOUNT_SERVER);
   string server_date = TimeToString(TimeCurrent(), TIME_DATE);
   string filename = StringFormat("MonthlyOHLC-%s-%s.csv", server_name, server_date);
   int handle = FileOpen(filename, FILE_WRITE | FILE_CSV | FILE_ANSI, ';');
   if (handle == INVALID_HANDLE)
   {
      PrintFormat("ERROR: Cannot open file %s for writing", filename);
      return;
   }
   // Header
   FileWrite(handle, "Symbol", "Date", "Open", "High", "Low", "Close", "Volume", "Sector", "Industry");
   int total_symbols = SymbolsTotal(false);
   int symbols_exported = 0;
   int symbols_skipped = 0;
   PrintFormat("Scanning %d symbols...", total_symbols);
   for (int i = 0; i < total_symbols; i++)
   {
      string symbol = SymbolName(i, false);
      if (symbol == "") continue;
      // Filter to stocks if requested
      if (ExportStocksOnly)
      {
         ENUM_SYMBOL_CALC_MODE calcMode = (ENUM_SYMBOL_CALC_MODE)SymbolInfoInteger(symbol, SYMBOL_TRADE_CALC_MODE);
         // Stock CFDs use SYMBOL_CALC_MODE_CFDLEVERAGE or SYMBOL_CALC_MODE_CFD
         // Also check sector — stocks have sector info, forex/crypto/indices don't
         string sector = "";
         // Try to detect stock symbols: they typically have sector/industry info
         // or their calc mode indicates stock-like instruments
         long sector_id = SymbolInfoInteger(symbol, SYMBOL_SECTOR);
         if (sector_id == SECTOR_UNDEFINED || sector_id == SECTOR_CURRENCY ||
             sector_id == SECTOR_CURRENCY_CRYPTO || sector_id == SECTOR_INDEXES ||
             sector_id == SECTOR_COMMODITIES || sector_id == SECTOR_ENERGY)
         {
            symbols_skipped++;
            continue;
         }
      }
      // Get monthly bars
      MqlRates rates[];
      datetime start_time = D'1970.01.01 00:00';
      datetime end_time = TimeCurrent();
      int bars = CopyRates(symbol, PERIOD_MN1, start_time, end_time, rates);
      if (bars <= 0)
      {
         symbols_skipped++;
         continue;
      }
      string sector = SymbolInfoString(symbol, SYMBOL_SECTOR_NAME);
      string industry = SymbolInfoString(symbol, SYMBOL_INDUSTRY_NAME);
      for (int b = 0; b < bars; b++)
      {
         string date = TimeToString(rates[b].time, TIME_DATE);
         FileWrite(handle, symbol, date,
                   DoubleToString(rates[b].open, 4),
                   DoubleToString(rates[b].high, 4),
                   DoubleToString(rates[b].low, 4),
                   DoubleToString(rates[b].close, 4),
                   IntegerToString(rates[b].tick_volume),
                   sector, industry);
      }
      symbols_exported++;
      if (symbols_exported % 50 == 0)
         PrintFormat("Exported %d symbols so far...", symbols_exported);
   }
   FileClose(handle);
   PrintFormat("Export complete: %d symbols exported, %d skipped. File: %s", symbols_exported, symbols_skipped, filename);
   PrintFormat("File location: MQL5/Files/%s", filename);
}
