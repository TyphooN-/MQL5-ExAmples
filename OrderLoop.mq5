/**=             OrderLoop.mq5  (TyphooN's Batch Order Placer)
 *               Copyright 2023, TyphooN (https://www.marketwizardry.org/)
 *
 * This file is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 **/
#property copyright "TyphooN"
#property link      "https://www.marketwizardry.org/"
#property version   "1.00"
#property strict
#include <Trade\Trade.mqh>
input double Lots = 1000;              // Lots per order
input double TotalLotsToSell = 21000;  // Total lots to sell
double LotsSold = 0.0;
CTrade trade;
//+------------------------------------------------------------------+
//| Expert initialization function                                   |
//+------------------------------------------------------------------+
int OnInit()
{
   if (!CheckTradingConditions())
   {
       Print("Trading conditions not met. EA initialization failed.");
       return(INIT_FAILED);
   }
   return(INIT_SUCCEEDED);
}
void OnTick()
{
   if(LotsSold >= TotalLotsToSell) return;
   if(!PlaceSellOrder())
      Print("Exiting due to failed order placement.");
}
bool PlaceSellOrder()
{
   double price = SymbolInfoDouble(_Symbol, SYMBOL_BID);
   if(price <= 0)
   {
      PrintFormat("No valid bid price for %s (error %d)", _Symbol, GetLastError());
      return false;
   }
   PrintFormat("Placing sell order: %.0f lots @ %s", Lots, DoubleToString(price, (int)SymbolInfoInteger(_Symbol, SYMBOL_DIGITS)));
   if(trade.Sell(Lots, _Symbol, price, 0, 0, "Sell " + DoubleToString(Lots, 0) + " lots"))
   {
      LotsSold += Lots;
      PrintFormat("Sell OK. Total lots sold: %.0f / %.0f", LotsSold, TotalLotsToSell);
      return true;
   }
   else
   {
      PrintFormat("Sell failed: retcode=%d, error=%d, desc=%s",
         trade.ResultRetcode(), GetLastError(), trade.ResultComment());
      return false;
   }
}
bool CheckTradingConditions()
{
   if (!TerminalInfoInteger(TERMINAL_TRADE_ALLOWED))
   {
      Print("Trading is not allowed in the terminal settings.");
      return false;
   }
   if (!AccountInfoInteger(ACCOUNT_TRADE_ALLOWED))
   {
      Print("Trading is not allowed for this account.");
      return false;
   }
   return true;
}
