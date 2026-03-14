/**=             LotsTotal.mq5  (TyphooN's LotsTotal Indicator)
 *               Copyright 2023, TyphooN (https://www.marketwizardry.info)
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
#property link      "https://www.marketwizardry.info"
#property version   "1.002"
#property indicator_chart_window
#property strict
void GetVolumesForSymbol(string symbol, double &longVol, double &shortVol)
{
   longVol = 0;
   shortVol = 0;
   for(int i = PositionsTotal() - 1; i >= 0; i--)
   {
      if(PositionGetSymbol(i) != symbol) continue;
      double volume = PositionGetDouble(POSITION_VOLUME);
      if(PositionGetInteger(POSITION_TYPE) == POSITION_TYPE_BUY)
         longVol += volume;
      else
         shortVol += volume;
   }
}
int OnInit()
{
   double totalLotsLong, totalLotsShort;
   GetVolumesForSymbol(_Symbol, totalLotsLong, totalLotsShort);
   Print("Total Lots Long for ", _Symbol, ": ", totalLotsLong);
   Print("Total Lots Short for ", _Symbol, ": ", totalLotsShort);
   double tickValue = SymbolInfoDouble(_Symbol, SYMBOL_TRADE_TICK_VALUE);
   double plPerTickLong = totalLotsLong * tickValue;
   double plPerTickShort = totalLotsShort * tickValue;
   Print("P/L per Tick (Long): ", plPerTickLong);
   Print("P/L per Tick (Short): ", plPerTickShort);
   Print("Net P/L per Tick (Long - Short): ", plPerTickLong - plPerTickShort);
   return(INIT_SUCCEEDED);
}
int OnCalculate(const int rates_total,
                const int prev_calculated,
                const datetime &time[],
                const double &open[],
                const double &high[],
                const double &low[],
                const double &close[],
                const long &tick_volume[],
                const long &volume[],
                const int &spread[])
{
    return(rates_total);
}
