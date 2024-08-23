#include <Trade\Trade.mqh>

//+------------------------------------------------------------------+
//| Expert initialization function                                   |
//+------------------------------------------------------------------+

input ENUM_ORDER_TYPE OrderType = ORDER_TYPE_BUY; // Order type (Buy/Sell)
input double OrderLots = 1;                      // Order lot size
input double StopLossPrice = 0.0;               // Fixed SL price (0 for no SL or ATR-based only)
input double StopLossATR = 1.5;                 // ATR multiple SL (if SL Price is 0 or invalid on entry)
input double TakeProfitPrice = 0.0;             // Fixed TP price (0 for no TP or ATR-based only)
input double TakeProfitATR = 2.0;               // ATR multiple TP (if TP Price is 0 or invalid on entry)
input int MagicNumber = 123456;                 // Magic number for orders

CTrade trade;  // Instance of CTrade class

bool orderPlaced = false;

//+------------------------------------------------------------------+
//| Expert initialization function                                   |
//+------------------------------------------------------------------+
int OnInit()
{
   // Set the expert's magic number
   trade.SetExpertMagicNumber(MagicNumber);

   Print("Market Reopen Order Queue EA initialized. Waiting for trading availability...");
   return INIT_SUCCEEDED;
}

//+------------------------------------------------------------------+
//| Expert deinitialization function                                 |
//+------------------------------------------------------------------+
void OnDeinit(const int reason)
{
}

//+------------------------------------------------------------------+
//| Expert tick function                                             |
//+------------------------------------------------------------------+
void OnTick()
{
   // Check if trading is available
   if (IsTradingAvailable())
   {
      if (!orderPlaced)
      {
         // If trading is available and the order is not placed yet, place the order
         PlaceMarketOrder();
      }
   }
   else
   {
      Print("Trading is not available for this symbol, waiting...");
   }
}

//+------------------------------------------------------------------+
//| Check if trading is available for the symbol                     |
//+------------------------------------------------------------------+
bool IsTradingAvailable()
{
   long tradeMode = SymbolInfoInteger(Symbol(), SYMBOL_TRADE_MODE); // Corrected variable type to long
   return (tradeMode == SYMBOL_TRADE_MODE_FULL);
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
   if (CopyBuffer(atrHandle, 0, 0, 1, atrValue) < 0)
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
   // Retrieve the minimum stop level from the broker in points
   long stopLevelPoints = SymbolInfoInteger(_Symbol, SYMBOL_TRADE_STOPS_LEVEL);
   double stopLevel = stopLevelPoints * _Point;

   if (isBuy)
      return (stopLoss < price && (price - stopLoss) >= stopLevel);
   else
      return (stopLoss > price && (stopLoss - price) >= stopLevel);
}

bool IsTakeProfitValid(double price, double takeProfit, bool isBuy)
{
   // Retrieve the minimum stop level from the broker in points
   long stopLevelPoints = SymbolInfoInteger(_Symbol, SYMBOL_TRADE_STOPS_LEVEL);
   double stopLevel = stopLevelPoints * _Point;

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
   double ask = SymbolInfoDouble(Symbol(), SYMBOL_ASK); // Retrieve Ask price
   double bid = SymbolInfoDouble(Symbol(), SYMBOL_BID); // Retrieve Bid price
   double atr = CalculateATR(10);  // Calculate 10-day ATR from the daily timeframe
   double price = OrderType == ORDER_TYPE_BUY ? ask : bid;

   double sl = StopLossPrice;
   double tp = TakeProfitPrice;

   // Determine if the given stop loss price is valid
   bool validStopLoss = IsStopLossValid(price, sl, OrderType == ORDER_TYPE_BUY);
   if (!validStopLoss && StopLossATR > 0)
   {
      // Calculate the stop loss using ATR if the provided stop loss is invalid or 0
      sl = OrderType == ORDER_TYPE_BUY ? price - (atr * StopLossATR) : price + (atr * StopLossATR);
      validStopLoss = IsStopLossValid(price, sl, OrderType == ORDER_TYPE_BUY);
      if (!validStopLoss) sl = 0; // If still invalid, don't use any stop loss
   }

   // Determine if the given take profit price is valid
   bool validTakeProfit = IsTakeProfitValid(price, tp, OrderType == ORDER_TYPE_BUY);
   if (!validTakeProfit && TakeProfitATR > 0)
   {
      // Calculate the take profit using ATR if the provided take profit is invalid or 0
      tp = OrderType == ORDER_TYPE_BUY ? price + (atr * TakeProfitATR) : price - (atr * TakeProfitATR);
      validTakeProfit = IsTakeProfitValid(price, tp, OrderType == ORDER_TYPE_BUY);
      if (!validTakeProfit) tp = 0; // If still invalid, don't use any take profit
   }

   // Place market order based on the selected order type
   if (OrderType == ORDER_TYPE_BUY)
   {
      trade.Buy(OrderLots, Symbol(), 0.0, sl, tp, "Queued Buy Market Order");
   }
   else if (OrderType == ORDER_TYPE_SELL)
   {
      trade.Sell(OrderLots, Symbol(), 0.0, sl, tp, "Queued Sell Market Order");
   }

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

//+------------------------------------------------------------------+
