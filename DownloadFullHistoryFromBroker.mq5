/**=             Discord.mq5  (TyphooN's DownloadFullHistoryFromBroker)
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
#property strict
#property copyright "TyphooN"
#property link      "https://www.marketwizardry.org/"
#property version   "1.002"
bool EnsureFullHistory(const string symbol, const ENUM_TIMEFRAMES timeframe)
{
   PrintFormat("Downloading full history for symbol: %s on timeframe: %d", symbol, timeframe);
   // Define an array to store historical data
   MqlRates rates[];
   // Set start and end times to cover the maximum range
   datetime start_time = D'1970.01.01 00:00'; // Earliest possible time
   datetime end_time   = TimeCurrent();      // Current server time
   // Request all available bars
   int bars = CopyRates(symbol, timeframe, start_time, end_time, rates);
   // Check if the data was successfully downloaded
   if (bars > 0)
   {
      PrintFormat("Downloaded %d bars for symbol: %s on timeframe: %d", bars, symbol, timeframe);
      return true; // Success
   }
   else
   {
      PrintFormat("Failed to download data for symbol: %s on timeframe: %d", symbol, timeframe);
      return false; // Failure
   }
}
void OnStart()
{
   int total_symbols = SymbolsTotal(false);
   ENUM_TIMEFRAMES timeframe = PERIOD_MN1;
   int success_count = 0;
   int failure_count = 0;
   string failed_symbols = "";
   PrintFormat("Total symbols available from broker: %d", total_symbols);
   for (int i = 0; i < total_symbols; i++)
   {
      string symbol = SymbolName(i, false);
      if (symbol != "")
      {
         if (EnsureFullHistory(symbol, timeframe))
         {
            success_count++;
         }
         else
         {
            failure_count++;
            failed_symbols += symbol + "\n";
         }
      }
   }
   PrintFormat("Full historical data download completed.");
   PrintFormat("Total symbols processed: %d", total_symbols);
   PrintFormat("Successfully downloaded data for %d symbols.", success_count);
   PrintFormat("Failed to download data for %d symbols.", failure_count);
   if (failed_symbols != "")
      PrintFormat("Failed symbols:\n%s", failed_symbols);
}
