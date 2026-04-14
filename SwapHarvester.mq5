/**=        SwapHarvester.mq5  (TyphooN's Darwinex Swap Harvester)
 *               Copyright 2024, TyphooN (https://www.marketwizardry.org/)
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
#property description "Darwinex Swap Harvester - finds symbols with positive swap for carry trades"
#property strict

input double MinSwap        = 0.0;   // Minimum swap value to display (0 = any positive)
input bool   ShowLongSwap   = true;  // Show symbols with positive long swap
input bool   ShowShortSwap  = true;  // Show symbols with positive short swap
input bool   ExportCSV      = true;  // Export results to CSV file

struct SwapEntry
{
   string symbol;
   string description;
   string sector;
   string industry;
   double swap_long;
   double swap_short;
   double spread;
   double bid;
   double volume_min;
   double margin_per_lot;
   string direction;      // "LONG", "SHORT", or "BOTH"
};

int OnInit()
{
   ScanSwaps();
   return(INIT_SUCCEEDED);
}

void ScanSwaps()
{
   int total = SymbolsTotal(false);
   SwapEntry entries[];
   ArrayResize(entries, 0, total);  // Pre-allocate reserve — avoids O(n) realloc per element
   int count = 0;

   Print("=== SWAPHARVEST ===");
   Print("Scanning ", total, " symbols on ", AccountInfoString(ACCOUNT_SERVER), "...");
   Print("");

   for (int i = 0; i < total; i++)
   {
      string sym = SymbolName(i, false);
      if (sym == "") continue;

      // Skip symbols that aren't tradeable
      int trade_mode = (int)SymbolInfoInteger(sym, SYMBOL_TRADE_MODE);
      if (trade_mode == SYMBOL_TRADE_MODE_DISABLED) continue;

      double swap_long  = SymbolInfoDouble(sym, SYMBOL_SWAP_LONG);
      double swap_short = SymbolInfoDouble(sym, SYMBOL_SWAP_SHORT);

      bool has_positive_long  = ShowLongSwap  && swap_long  > MinSwap;
      bool has_positive_short = ShowShortSwap && swap_short > MinSwap;

      if (!has_positive_long && !has_positive_short) continue;

      int idx = count;
      count++;
      ArrayResize(entries, count);

      entries[idx].symbol      = sym;
      entries[idx].description = SymbolInfoString(sym, SYMBOL_DESCRIPTION);
      entries[idx].sector      = SymbolInfoString(sym, SYMBOL_SECTOR_NAME);
      entries[idx].industry    = SymbolInfoString(sym, SYMBOL_INDUSTRY_NAME);
      entries[idx].swap_long   = swap_long;
      entries[idx].swap_short  = swap_short;
      entries[idx].spread      = (double)SymbolInfoInteger(sym, SYMBOL_SPREAD) * SymbolInfoDouble(sym, SYMBOL_POINT);
      entries[idx].bid         = SymbolInfoDouble(sym, SYMBOL_BID);
      entries[idx].volume_min  = SymbolInfoDouble(sym, SYMBOL_VOLUME_MIN);

      double margin = 0;
      OrderCalcMargin(ORDER_TYPE_BUY, sym, 1.0, SymbolInfoDouble(sym, SYMBOL_ASK), margin);
      entries[idx].margin_per_lot = margin;

      if (has_positive_long && has_positive_short)
         entries[idx].direction = "BOTH";
      else if (has_positive_long)
         entries[idx].direction = "LONG";
      else
         entries[idx].direction = "SHORT";
   }

   // Pre-cache sort keys — avoids recomputing MathMax inside O(n²) sort loop
   double sortKeys[];
   ArrayResize(sortKeys, count);
   for (int i = 0; i < count; i++)
      sortKeys[i] = MathMax(entries[i].swap_long, entries[i].swap_short);

   // Selection sort descending (n < 200, acceptable for cold path)
   for (int i = 0; i < count - 1; i++)
   {
      int best = i;
      for (int j = i + 1; j < count; j++)
      {
         if (sortKeys[j] > sortKeys[best])
            best = j;
      }
      if (best != i)
      {
         SwapEntry tmp = entries[i];
         entries[i] = entries[best];
         entries[best] = tmp;
         double tmpKey = sortKeys[i];
         sortKeys[i] = sortKeys[best];
         sortKeys[best] = tmpKey;
      }
   }

   // Print results grouped by direction
   PrintSection("=== POSITIVE LONG SWAP (Buy & Hold) ===", entries, count, "LONG");
   PrintSection("=== POSITIVE SHORT SWAP (Sell & Hold) ===", entries, count, "SHORT");
   PrintSection("=== POSITIVE BOTH DIRECTIONS ===", entries, count, "BOTH");

   Print("");
   Print("=== SUMMARY ===");
   Print("Total symbols scanned: ", SymbolsTotal(false));
   Print("Symbols with positive swap: ", count);

   int long_count = 0, short_count = 0, both_count = 0;
   for (int i = 0; i < count; i++)
   {
      if (entries[i].direction == "LONG")  long_count++;
      else if (entries[i].direction == "SHORT") short_count++;
      else both_count++;
   }
   Print("  Long only:  ", long_count);
   Print("  Short only: ", short_count);
   Print("  Both:       ", both_count);

   if (ExportCSV)
      WriteCSV(entries, count);

   Print("");
   Print("SwapHarvester complete. Removing EA from chart.");
   ExpertRemove();
}

void PrintSection(string header, SwapEntry &entries[], int count, string filter_dir)
{
   Print("");
   Print(header);
   Print(StringFormat("%-20s %10s %10s %10s %10s %10s %s",
         "Symbol", "SwapLong", "SwapShort", "Spread", "Bid", "Margin/Lot", "Description"));
   Print("--------------------------------------------------------------------------------");

   for (int i = 0; i < count; i++)
   {
      bool match = false;
      if (filter_dir == "LONG"  && (entries[i].direction == "LONG"  || entries[i].direction == "BOTH")) match = true;
      if (filter_dir == "SHORT" && (entries[i].direction == "SHORT" || entries[i].direction == "BOTH")) match = true;
      if (filter_dir == "BOTH"  && entries[i].direction == "BOTH") match = true;
      if (!match) continue;

      Print(StringFormat("%-20s %10.4f %10.4f %10.5f %10.5f %10.2f %s",
            entries[i].symbol,
            entries[i].swap_long,
            entries[i].swap_short,
            entries[i].spread,
            entries[i].bid,
            entries[i].margin_per_lot,
            entries[i].description));
   }
}

void WriteCSV(SwapEntry &entries[], int count)
{
   string server_name = AccountInfoString(ACCOUNT_SERVER);
   string date_str = TimeToString(TimeCurrent(), TIME_DATE);
   string path = StringFormat("SwapHarvester-%s-%s.csv", server_name, date_str);

   int fh = FileOpen(path, FILE_WRITE | FILE_CSV | FILE_ANSI);
   if (fh == INVALID_HANDLE)
   {
      Print("Failed to open CSV: ", path);
      return;
   }

   FileWriteString(fh, "Symbol;Direction;SwapLong;SwapShort;Spread;Bid;VolumeMin;MarginPerLot;Sector;Industry;Description\n");

   for (int i = 0; i < count; i++)
   {
      string line = StringFormat("%s;%s;%.4f;%.4f;%.5f;%.5f;%.2f;%.2f;%s;%s;%s\n",
            entries[i].symbol,
            entries[i].direction,
            entries[i].swap_long,
            entries[i].swap_short,
            entries[i].spread,
            entries[i].bid,
            entries[i].volume_min,
            entries[i].margin_per_lot,
            entries[i].sector,
            entries[i].industry,
            entries[i].description);
      FileWriteString(fh, line);
   }

   FileClose(fh);
   Print("CSV exported: ", path);
}
