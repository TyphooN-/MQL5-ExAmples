/**=        ExportSymbols.mq5  (TyphooN's CSV Symbol Exporter)
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
#property version     "1.002"
#property description "TyphooN's CSV Symbol Exporter"
#property strict
#include <Darwinex\DWEX Portfolio Risk Man.mqh>
// Input parameter for the output CSV file path
input string CSVFilePath = "SymbolsList.csv";
// Instantiate the portfolio risk manager
CPortfolioRiskMan portfolioRiskMan(PERIOD_D1, 20);  // Example: Using daily timeframe and 20 periods for StdDev
// Indicator initialization function
int OnInit()
{
   // Call the function to export symbols to CSV
   ExportSymbolsToCSV();
   // Terminate the indicator after initialization
   return(INIT_SUCCEEDED);
}
void ExportSymbolsToCSV()
{
   // Open the CSV file for writing
   int file_handle = FileOpen(CSVFilePath, FILE_WRITE|FILE_CSV|FILE_ANSI);
   if(file_handle == INVALID_HANDLE)
   {
      Print("Failed to open file: ", CSVFilePath);
      return;
   }
   // Write the header row with semicolon as the delimiter
   FileWriteString(file_handle, "Symbol;BaseCurrency;QuoteCurrency;Description;Digits;Point;Spread;TickSize;TickValue;TradeContractSize;TradeMode;TradeExecutionMode;VolumeMin;VolumeMax;VolumeStep;MarginInitial;MarginMaintenance;MarginHedged;MarginRate;MarginCurrency;StartDate;ExpirationDate;SwapLong;SwapShort;SwapType;Swap3Days;TradeSessions;VaR_1_Lot;BidPrice;AskPrice;Sector;Industry\n");
   // Get the total number of symbols
   int total_symbols = SymbolsTotal(false);
   // Loop through all symbols and write their details to the CSV file
   for(int i = 0; i < total_symbols; i++)
   {
      string symbol = SymbolName(i, false);
      if(symbol != "")
      {
         string base_currency = SymbolInfoString(symbol, SYMBOL_CURRENCY_BASE);
         string quote_currency = SymbolInfoString(symbol, SYMBOL_CURRENCY_PROFIT);
         string description = SymbolInfoString(symbol, SYMBOL_DESCRIPTION);
         int digits = (int)SymbolInfoInteger(symbol, SYMBOL_DIGITS);
         double point = SymbolInfoDouble(symbol, SYMBOL_POINT);
         int spread = (int)SymbolInfoInteger(symbol, SYMBOL_SPREAD);
         double tick_size = SymbolInfoDouble(symbol, SYMBOL_TRADE_TICK_SIZE);
         double tick_value = SymbolInfoDouble(symbol, SYMBOL_TRADE_TICK_VALUE);
         double trade_contract_size = SymbolInfoDouble(symbol, SYMBOL_TRADE_CONTRACT_SIZE);
         int trade_mode = (int)SymbolInfoInteger(symbol, SYMBOL_TRADE_MODE);
         int trade_execution_mode = (int)SymbolInfoInteger(symbol, SYMBOL_TRADE_EXEMODE);
         double volume_min = SymbolInfoDouble(symbol, SYMBOL_VOLUME_MIN);
         double volume_max = SymbolInfoDouble(symbol, SYMBOL_VOLUME_MAX);
         double volume_step = SymbolInfoDouble(symbol, SYMBOL_VOLUME_STEP);
         double margin_initial = SymbolInfoDouble(symbol, SYMBOL_MARGIN_INITIAL);
         double margin_maintenance = SymbolInfoDouble(symbol, SYMBOL_MARGIN_MAINTENANCE);
         double margin_hedged = SymbolInfoDouble(symbol, SYMBOL_MARGIN_HEDGED);
         double margin_long = SymbolInfoDouble(symbol, SYMBOL_MARGIN_LONG);
         double margin_short = SymbolInfoDouble(symbol, SYMBOL_MARGIN_SHORT);
         string margin_currency = SymbolInfoString(symbol, SYMBOL_CURRENCY_MARGIN);
         datetime start_date = (datetime)SymbolInfoInteger(symbol, SYMBOL_START_TIME);
         datetime expiration_date = (datetime)SymbolInfoInteger(symbol, SYMBOL_EXPIRATION_TIME);
         double swap_long = SymbolInfoDouble(symbol, SYMBOL_SWAP_LONG);
         double swap_short = SymbolInfoDouble(symbol, SYMBOL_SWAP_SHORT);
         int swap_type = (int)SymbolInfoInteger(symbol, SYMBOL_SWAP_MODE);
         int swap_3days = (int)SymbolInfoInteger(symbol, SYMBOL_SWAP_ROLLOVER3DAYS);
         string trade_sessions = GetTradeSessions(symbol);
         // Calculate VaR for 1 lot
         double var_1_lot = 0.0;
         if(portfolioRiskMan.CalculateVaR(symbol, 1.0))
         {
            var_1_lot = portfolioRiskMan.SinglePositionVaR;
         }
         // Get bid and ask prices
         double bid_price = SymbolInfoDouble(symbol, SYMBOL_BID);
         double ask_price = SymbolInfoDouble(symbol, SYMBOL_ASK);
         // Get sector and industry
         string sector = GetSector(symbol);
         string industry = GetIndustry(symbol);
         // Create a line with semicolon as the delimiter
         string line = StringFormat("%s;%s;%s;%s;%d;%f;%d;%f;%f;%f;%d;%d;%f;%f;%f;%f;%f;%f;%f;%s;%s;%f;%f;%d;%d;%s;%f;%f;%f;%s;%s\n",
                                    symbol, base_currency, quote_currency, description, digits, point, spread, 
                                    tick_size, tick_value, trade_contract_size, trade_mode, trade_execution_mode, 
                                    volume_min, volume_max, volume_step, margin_long, margin_short, margin_maintenance, 
                                    margin_hedged, margin_currency, TimeToString(start_date, TIME_DATE|TIME_MINUTES), TimeToString(expiration_date, TIME_DATE|TIME_MINUTES), 
                                    swap_long, swap_short, swap_type, swap_3days, trade_sessions, var_1_lot, bid_price, ask_price, sector, industry);
         
         // Write the line to the CSV file
         FileWriteString(file_handle, line);
      }
   }
   // Close the CSV file
   FileClose(file_handle);
   Print("Export completed. File saved at: ", CSVFilePath);
   // Remove the EA from the chart
   Print("ExportSymbols EA removed from chart.");
   ExpertRemove();
}
string GetTradeSessions(string symbol)
{
   string result = "";
   for(int day=0; day<7; day++)
   {
      for(int session=0; session<3; session++)
      {
         datetime open_time, close_time;
         if(SymbolInfoSessionQuote(symbol, ENUM_DAY_OF_WEEK(day), session, open_time, close_time))
         {
            if(open_time != 0 && close_time != 0)
            {
               result += StringFormat("%s-%s ", TimeToString(open_time, TIME_MINUTES), TimeToString(close_time, TIME_MINUTES));
            }
         }
      }
   }
   return result;
}
string GetSector(string symbol)
{
   int sector = (int)SymbolInfoInteger(symbol, SYMBOL_SECTOR);
   switch(sector)
   {
      case 1:  return "Basic Materials";
      case 2:  return "Communications";
      case 3:  return "Consumer Cyclical";
      case 4:  return "Consumer Non-Cyclical";
      case 5:  return "Energy";
      case 6:  return "Financial";
      case 7:  return "Healthcare";
      case 8:  return "Industrial";
      case 9:  return "Technology";
      case 10: return "Utilities";
      default: return "Unknown";
   }
}
string GetIndustry(string symbol)
{
   int industry = (int)SymbolInfoInteger(symbol, SYMBOL_INDUSTRY);
   switch(industry)
   {
      case 1:  return "Automobiles";
      case 2:  return "Banks";
      case 3:  return "Chemicals";
      case 4:  return "Construction";
      case 5:  return "Consumer Products";
      case 6:  return "Electronics";
      case 7:  return "Energy";
      case 8:  return "Financial";
      case 9:  return "Healthcare";
      case 10: return "Industrial";
      case 11: return "Insurance";
      case 12: return "Media";
      case 13: return "Pharmaceuticals";
      case 14: return "Real Estate";
      case 15: return "Retail";
      case 16: return "Software";
      case 17: return "Technology";
      case 18: return "Telecommunications";
      case 19: return "Transportation";
      case 20: return "Utilities";
      default: return "Unknown";
   }
}
