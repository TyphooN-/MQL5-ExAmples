/**=             Market At Open.mq5  (TyphooN's Market Reopen Order Queue EA)
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
#property copyright "TyphooN"
#property link      "https://www.marketwizardry.org/"
#property version   "1.00"
#property strict
#include <Trade\Trade.mqh>

input ENUM_ORDER_TYPE OrderType = ORDER_TYPE_BUY; // Order type (Buy/Sell)
input double OrderLots = 1;                      // Order lot size
input double StopLossPrice = 0.0;               // Fixed SL price (0 for no SL or ATR-based only)
input double StopLossATR = 1.5;                 // ATR multiple SL (if SL Price is 0 or invalid on entry)
input double TakeProfitPrice = 0.0;             // Fixed TP price (0 for no TP or ATR-based only)
input double TakeProfitATR = 2.0;               // ATR multiple TP (if TP Price is 0 or invalid on entry)
input int MagicNumber = 123456;                 // Magic number for orders

CTrade trade;  // Instance of CTrade class

bool orderPlaced = false;

int OnInit()
{
   trade.SetExpertMagicNumber(MagicNumber);
   Print("Market Reopen Order Queue EA initialized. Waiting for trading availability...");
   return INIT_SUCCEEDED;
}

//+------------------------------------------------------------------+
//| Expert tick function                                             |
//+------------------------------------------------------------------+
void OnTick()
{
   if (orderPlaced) return;

   if (IsTradingAvailable())
      PlaceMarketOrder();
}

//+------------------------------------------------------------------+
//| Check if trading is available for the symbol                     |
//+------------------------------------------------------------------+
bool IsTradingAvailable()
{
   return (SymbolInfoInteger(_Symbol, SYMBOL_TRADE_MODE) == SYMBOL_TRADE_MODE_FULL);
}

//+------------------------------------------------------------------+
//| Calculate ATR for Daily timeframe                                |
//+------------------------------------------------------------------+
double CalculateATR(int period)
{
   int atrHandle = iATR(_Symbol, PERIOD_D1, period);
   if (atrHandle == INVALID_HANDLE)
   {
      Print("Failed to get ATR handle.");
      return 0;
   }

   double atrValue[];
   int copied = CopyBuffer(atrHandle, 0, 0, 1, atrValue);
   IndicatorRelease(atrHandle);

   if (copied <= 0)
   {
      Print("Failed to copy ATR data.");
      return 0;
   }

   return atrValue[0];
}

//+------------------------------------------------------------------+
//| Validate stop loss and take profit prices                        |
//+------------------------------------------------------------------+
bool IsStopLossValid(double price, double stopLoss, bool isBuy)
{
   double stopLevel = SymbolInfoInteger(_Symbol, SYMBOL_TRADE_STOPS_LEVEL) * _Point;

   if (isBuy)
      return (stopLoss < price && (price - stopLoss) >= stopLevel);
   else
      return (stopLoss > price && (stopLoss - price) >= stopLevel);
}

bool IsTakeProfitValid(double price, double takeProfit, bool isBuy)
{
   double stopLevel = SymbolInfoInteger(_Symbol, SYMBOL_TRADE_STOPS_LEVEL) * _Point;

   if (isBuy)
      return (takeProfit > price && (takeProfit - price) >= stopLevel);
   else
      return (takeProfit < price && (price - takeProfit) >= stopLevel);
}

//+------------------------------------------------------------------+
//| Place market order function                                      |
//+------------------------------------------------------------------+
void PlaceMarketOrder()
{
   double ask = SymbolInfoDouble(_Symbol, SYMBOL_ASK);
   double bid = SymbolInfoDouble(_Symbol, SYMBOL_BID);
   double atr = CalculateATR(10);
   bool isBuy = (OrderType == ORDER_TYPE_BUY);
   double price = isBuy ? ask : bid;

   int digits = (int)SymbolInfoInteger(_Symbol, SYMBOL_DIGITS);
   double sl = NormalizeDouble(StopLossPrice, digits);
   double tp = NormalizeDouble(TakeProfitPrice, digits);

   if (!IsStopLossValid(price, sl, isBuy) && StopLossATR > 0)
   {
      sl = NormalizeDouble(isBuy ? price - (atr * StopLossATR) : price + (atr * StopLossATR), digits);
      if (!IsStopLossValid(price, sl, isBuy)) sl = 0;
   }

   if (!IsTakeProfitValid(price, tp, isBuy) && TakeProfitATR > 0)
   {
      tp = NormalizeDouble(isBuy ? price + (atr * TakeProfitATR) : price - (atr * TakeProfitATR), digits);
      if (!IsTakeProfitValid(price, tp, isBuy)) tp = 0;
   }

   if (isBuy)
      trade.Buy(OrderLots, _Symbol, 0.0, sl, tp, "Queued Buy Market Order");
   else
      trade.Sell(OrderLots, _Symbol, 0.0, sl, tp, "Queued Sell Market Order");

   if (trade.ResultRetcode() == TRADE_RETCODE_DONE)
   {
      Print("Order successfully placed.");
      orderPlaced = true;
   }
   else
   {
      Print("Failed to place order. Error: ", trade.ResultRetcode());
   }
}
