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
#property version     "1.009"
#property description "TyphooN's CSV Symbol Exporter"
#property strict
#include <Darwinex/DWEX Portfolio Risk Man.mqh>
// Instantiate the portfolio risk manager
CPortfolioRiskMan portfolioRiskMan(PERIOD_D1, 20);  // Example: Using daily timeframe and 20 periods for StdDev
// Input parameter for optional user-specified CSV file path
input string UserCSVFilePath = "";
// Variable to hold the actual file path used for export
string CSVFilePath;
int OnInit()
{
   string server_name = AccountInfoString(ACCOUNT_SERVER);
   string server_date = TimeToString(TimeCurrent(), TIME_DATE); // Only the date
   string server_type = "";
   if (SymbolInfoDouble("USDMXN", SYMBOL_BID) > 0) {
     server_type = "CFD";
   }
   else if (SymbolInfoDouble("ES_U", SYMBOL_BID) > 0) {
     server_type = "Futures";
   }
   else
   {
     server_type = "Stocks";
   }
   CSVFilePath = StringFormat("SymbolsExport-%s-%s-%s.csv", server_name, server_type, server_date);
   Print("Exporting symbols to file: ", CSVFilePath);
   // Call the function to export symbols to CSV
   ExportSymbolsToCSV();
   // Terminate the indicator after initialization
   return(INIT_SUCCEEDED);
}
//+------------------------------------------------------------------+
//| Symbol Information Class                                         |
//+------------------------------------------------------------------+
class CSymbolInfo
{
public:
    string symbol;
    string base_currency;
    string quote_currency;
    string description;
    int digits;
    double point;
    int spread;
    double tick_size;
    double tick_value;
    double trade_contract_size;
    int trade_mode;
    int trade_execution_mode;
    double volume_min;
    double volume_max;
    double volume_step;
    double margin_initial;
    double margin_maintenance;
    double margin_hedged;
    string margin_currency;
    datetime start_date;
    datetime expiration_date;
    double swap_long;
    double swap_short;
    int swap_type;
    int swap_3days;
    string trade_sessions;
    double var_1_lot;
    double bid_price;
    double ask_price;
    string sector_name;
    string industry_name;
    double atr_daily;
    double atr_weekly;
    double atr_monthly;

    void CSymbolInfo(string sym)
    {
        symbol = sym;
        base_currency = SymbolInfoString(symbol, SYMBOL_CURRENCY_BASE);
        quote_currency = SymbolInfoString(symbol, SYMBOL_CURRENCY_PROFIT);
        description = SymbolInfoString(symbol, SYMBOL_DESCRIPTION);
        digits = (int)SymbolInfoInteger(symbol, SYMBOL_DIGITS);
        point = SymbolInfoDouble(symbol, SYMBOL_POINT);
        spread = (int)SymbolInfoInteger(symbol, SYMBOL_SPREAD);
        tick_size = SymbolInfoDouble(symbol, SYMBOL_TRADE_TICK_SIZE);
        tick_value = SymbolInfoDouble(symbol, SYMBOL_TRADE_TICK_VALUE);
        trade_contract_size = SymbolInfoDouble(symbol, SYMBOL_TRADE_CONTRACT_SIZE);
        trade_mode = (int)SymbolInfoInteger(symbol, SYMBOL_TRADE_MODE);
        trade_execution_mode = (int)SymbolInfoInteger(symbol, SYMBOL_TRADE_EXEMODE);
        volume_min = SymbolInfoDouble(symbol, SYMBOL_VOLUME_MIN);
        volume_max = SymbolInfoDouble(symbol, SYMBOL_VOLUME_MAX);
        volume_step = SymbolInfoDouble(symbol, SYMBOL_VOLUME_STEP);
        margin_initial = SymbolInfoDouble(symbol, SYMBOL_MARGIN_INITIAL);
        margin_maintenance = SymbolInfoDouble(symbol, SYMBOL_MARGIN_MAINTENANCE);
        margin_hedged = SymbolInfoDouble(symbol, SYMBOL_MARGIN_HEDGED);
        margin_currency = SymbolInfoString(symbol, SYMBOL_CURRENCY_MARGIN);
        start_date = (datetime)SymbolInfoInteger(symbol, SYMBOL_START_TIME);
        expiration_date = (datetime)SymbolInfoInteger(symbol, SYMBOL_EXPIRATION_TIME);
        swap_long = SymbolInfoDouble(symbol, SYMBOL_SWAP_LONG);
        swap_short = SymbolInfoDouble(symbol, SYMBOL_SWAP_SHORT);
        swap_type = (int)SymbolInfoInteger(symbol, SYMBOL_SWAP_MODE);
        swap_3days = (int)SymbolInfoInteger(symbol, SYMBOL_SWAP_ROLLOVER3DAYS);
        trade_sessions = GetTradeSessions(symbol);
        var_1_lot = 0.0;
        if (portfolioRiskMan.CalculateVaR(symbol, 1.0))
        {
            var_1_lot = portfolioRiskMan.SinglePositionVaR;
        }
        bid_price = SymbolInfoDouble(symbol, SYMBOL_BID);
        ask_price = SymbolInfoDouble(symbol, SYMBOL_ASK);
        sector_name = SymbolInfoString(symbol, SYMBOL_SECTOR_NAME);
        industry_name = SymbolInfoString(symbol, SYMBOL_INDUSTRY_NAME);
        
        double atr_values_d[], atr_values_w[], atr_values_m[];
        if(CopyBuffer(iATR(symbol, PERIOD_D1, 14), 0, 1, 1, atr_values_d) > 0) atr_daily = atr_values_d[0]; else atr_daily = 0;
        if(CopyBuffer(iATR(symbol, PERIOD_W1, 14), 0, 1, 1, atr_values_w) > 0) atr_weekly = atr_values_w[0]; else atr_weekly = 0;
        if(CopyBuffer(iATR(symbol, PERIOD_MN1, 14), 0, 1, 1, atr_values_m) > 0) atr_monthly = atr_values_m[0]; else atr_monthly = 0;
    }
};

// Function to export symbols to CSV
void ExportSymbolsToCSV()
{
    int file_handle = FileOpen(CSVFilePath, FILE_WRITE | FILE_CSV | FILE_ANSI);
    if (file_handle == INVALID_HANDLE)
    {
        Print("Failed to open file: ", CSVFilePath);
        return;
    }

    FileWriteString(file_handle, "Symbol;BaseCurrency;QuoteCurrency;Description;Digits;Point;Spread;TickSize;TickValue;TradeContractSize;TradeMode;TradeExecutionMode;VolumeMin;VolumeMax;VolumeStep;MarginInitial;MarginMaintenance;MarginHedged;MarginCurrency;StartDate;ExpirationDate;SwapLong;SwapShort;SwapType;Swap3Days;TradeSessions;VaR_1_Lot;BidPrice;AskPrice;SectorName;IndustryName;ATR_D1;ATR_W1;ATR_MN1\n");

    int total_symbols = SymbolsTotal(false);
    int batch_size = 100; // Process 100 symbols at a time

    for (int i = 0; i < total_symbols; i += batch_size)
    {
        CSymbolInfo* symbols_data[];
        int current_batch_size = MathMin(batch_size, total_symbols - i);
        ArrayResize(symbols_data, current_batch_size);

        for (int j = 0; j < current_batch_size; j++)
        {
            string symbol = SymbolName(i + j, false);
            if (symbol != "")
            {
                symbols_data[j] = new CSymbolInfo(symbol);
            }
        }

        for (int j = 0; j < current_batch_size; j++)
        {
            if (CheckPointer(symbols_data[j]) == POINTER_INVALID) continue;
            string line = StringFormat("%s;%s;%s;%s;%d;%f;%d;%f;%f;%f;%d;%d;%f;%f;%f;%f;%f;%f;%s;%s;%s;%f;%f;%d;%d;%s;%f;%f;%f;%s;%s;%f;%f;%f\n",
                symbols_data[j].symbol, symbols_data[j].base_currency, symbols_data[j].quote_currency, symbols_data[j].description,
                symbols_data[j].digits, symbols_data[j].point, symbols_data[j].spread, symbols_data[j].tick_size,
                symbols_data[j].tick_value, symbols_data[j].trade_contract_size, symbols_data[j].trade_mode,
                symbols_data[j].trade_execution_mode, symbols_data[j].volume_min, symbols_data[j].volume_max,
                symbols_data[j].volume_step, symbols_data[j].margin_initial, symbols_data[j].margin_maintenance,
                symbols_data[j].margin_hedged, symbols_data[j].margin_currency,
                TimeToString(symbols_data[j].start_date, TIME_DATE|TIME_MINUTES),
                TimeToString(symbols_data[j].expiration_date, TIME_DATE|TIME_MINUTES),
                symbols_data[j].swap_long, symbols_data[j].swap_short, symbols_data[j].swap_type,
                symbols_data[j].swap_3days, symbols_data[j].trade_sessions, symbols_data[j].var_1_lot,
                symbols_data[j].bid_price, symbols_data[j].ask_price, symbols_data[j].sector_name,
                symbols_data[j].industry_name, symbols_data[j].atr_daily, symbols_data[j].atr_weekly, symbols_data[j].atr_monthly);
            FileWriteString(file_handle, line);
        }

        // Clear the data for the current batch
        for (int j = 0; j < current_batch_size; j++)
        {
            if (CheckPointer(symbols_data[j]) != POINTER_INVALID)
            {
                delete symbols_data[j];
            }
        }
    }

    FileClose(file_handle);
    Print("Export completed. File saved at: ", CSVFilePath);
    Print("ExportSymbols EA removed from chart.");
    ExpertRemove();
}
// Function to get the trade session information as a formatted string
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
