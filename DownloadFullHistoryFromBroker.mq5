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
#property version   "1.000"
// Function to ensure full historical data is downloaded
void EnsureFullHistory(const string symbol, const ENUM_TIMEFRAMES timeframe)
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
      PrintFormat("Downloaded %d bars for symbol: %s on timeframe: %d", bars, symbol, timeframe);
   else
      PrintFormat("Failed to download data for symbol: %s on timeframe: %d", symbol, timeframe);
}

// Script entry point
void OnStart()
{
   // Get the total number of symbols in the Market Watch
   int total_symbols = SymbolsTotal(true);

   // Set the target timeframe
   ENUM_TIMEFRAMES timeframe = PERIOD_MN1;

   // Loop through all symbols
   for (int i = 0; i < total_symbols; i++) 
   {
      // Get the symbol name
      string symbol = SymbolName(i, true);

      if (symbol != "") 
      {
         // Ensure full historical data is downloaded
         EnsureFullHistory(symbol, timeframe);
      } 
      else 
      {
         PrintFormat("Failed to retrieve symbol at index: %d", i);
      }
   }

   Print("Full historical data download completed for all symbols.");
}
