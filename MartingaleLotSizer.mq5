/**=        MartingaleLotSizer.mq5  (TyphooN's Martingale Lot Sizer)
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
#property description "Martingale Lot Sizer - Single Symbol Export"
#property strict
#include <Darwinex/DWEX Portfolio Risk Man.mqh>

input string InpSymbol       = "XNGUSD";    // Symbol
input double InpBaseLot      = 0.01;        // Base Lot Size (Level 1)
input double InpMultiplier   = 2.0;         // Lot Multiplier Per Level
input int    InpMaxLevels    = 10;          // Max Martingale Levels

CPortfolioRiskMan portfolioRiskMan(PERIOD_D1, 20);

int OnInit()
{
    if (SymbolInfoDouble(InpSymbol, SYMBOL_BID) <= 0)
    {
        Print("ERROR: Symbol ", InpSymbol, " not found or no price data.");
        ExpertRemove();
        return(INIT_FAILED);
    }

    double volume_min  = SymbolInfoDouble(InpSymbol, SYMBOL_VOLUME_MIN);
    double volume_max  = SymbolInfoDouble(InpSymbol, SYMBOL_VOLUME_MAX);
    double volume_step = SymbolInfoDouble(InpSymbol, SYMBOL_VOLUME_STEP);
    double tick_value  = SymbolInfoDouble(InpSymbol, SYMBOL_TRADE_TICK_VALUE);
    double tick_size   = SymbolInfoDouble(InpSymbol, SYMBOL_TRADE_TICK_SIZE);
    double contract    = SymbolInfoDouble(InpSymbol, SYMBOL_TRADE_CONTRACT_SIZE);
    double margin_init = SymbolInfoDouble(InpSymbol, SYMBOL_MARGIN_INITIAL);
    double margin_maint= SymbolInfoDouble(InpSymbol, SYMBOL_MARGIN_MAINTENANCE);
    double bid         = SymbolInfoDouble(InpSymbol, SYMBOL_BID);
    double ask         = SymbolInfoDouble(InpSymbol, SYMBOL_ASK);
    int    digits      = (int)SymbolInfoInteger(InpSymbol, SYMBOL_DIGITS);
    int    spread      = (int)SymbolInfoInteger(InpSymbol, SYMBOL_SPREAD);
    double point       = SymbolInfoDouble(InpSymbol, SYMBOL_POINT);
    double balance     = AccountInfoDouble(ACCOUNT_BALANCE);
    double equity      = AccountInfoDouble(ACCOUNT_EQUITY);
    long   leverage    = AccountInfoInteger(ACCOUNT_LEVERAGE);

    double var_1_lot = 0.0;
    if (portfolioRiskMan.CalculateVaR(InpSymbol, 1.0))
        var_1_lot = portfolioRiskMan.SinglePositionVaR;

    double atr_values[];
    int hATR = iATR(InpSymbol, PERIOD_D1, 14);
    double atr_d1 = (hATR != INVALID_HANDLE && CopyBuffer(hATR, 0, 1, 1, atr_values) > 0) ? atr_values[0] : 0;
    if (hATR != INVALID_HANDLE) IndicatorRelease(hATR);

    // --- Print symbol specs ---
    Print("=== Martingale Lot Sizer: ", InpSymbol, " ===");
    Print("Bid: ", bid, " | Ask: ", ask, " | Spread: ", spread, " pts");
    Print("Digits: ", digits, " | Point: ", point, " | TickSize: ", tick_size, " | TickValue: ", tick_value);
    Print("ContractSize: ", contract, " | MarginInit: ", margin_init, " | MarginMaint: ", margin_maint);
    Print("VolumeMin: ", volume_min, " | VolumeMax: ", volume_max, " | VolumeStep: ", volume_step);
    Print("VaR(1 lot): ", var_1_lot, " | ATR(D1,14): ", atr_d1);
    Print("Balance: ", balance, " | Equity: ", equity, " | Leverage: 1:", leverage);
    Print("");

    // --- Build lot table ---
    string filename = StringFormat("MartingaleLots-%s-%s.csv", InpSymbol, TimeToString(TimeCurrent(), TIME_DATE));
    int file_handle = FileOpen(filename, FILE_WRITE | FILE_CSV | FILE_ANSI);
    if (file_handle == INVALID_HANDLE)
    {
        PrintFormat("ERROR: Cannot open file %s (error %d)", filename, GetLastError());
        ExpertRemove();
        return(INIT_FAILED);
    }

    FileWriteString(file_handle, "Level;Lots;MarginRequired;CumulativeLots;CumulativeMargin;PctOfBalance;VaR\n");
    Print(StringFormat("%-6s %-10s %-16s %-16s %-18s %-12s %-10s", "Level", "Lots", "Margin", "CumLots", "CumMargin", "%Balance", "VaR"));
    Print("------ ---------- ---------------- ---------------- ------------------ ------------ ----------");

    double cumulative_lots   = 0;
    double cumulative_margin = 0;

    for (int level = 0; level < InpMaxLevels; level++)
    {
        double raw_lots = InpBaseLot * MathPow(InpMultiplier, level);
        double lots = NormalizeLots(raw_lots, volume_min, volume_max, volume_step);

        if (lots <= 0) break;

        double margin_needed = 0;
        if (!OrderCalcMargin(ORDER_TYPE_BUY, InpSymbol, lots, ask, margin_needed))
            margin_needed = lots * margin_init;

        double level_var = var_1_lot * lots;
        cumulative_lots   += lots;
        cumulative_margin += margin_needed;
        double pct_balance = (balance > 0) ? (cumulative_margin / balance * 100.0) : 0;

        string log_line = StringFormat("%-6d %-10.2f %-16.2f %-16.2f %-18.2f %-12.2f %-10.2f",
            level + 1, lots, margin_needed, cumulative_lots, cumulative_margin, pct_balance, level_var);
        Print(log_line);

        string csv_line = StringFormat("%d;%.2f;%.2f;%.2f;%.2f;%.2f;%.2f\n",
            level + 1, lots, margin_needed, cumulative_lots, cumulative_margin, pct_balance, level_var);
        FileWriteString(file_handle, csv_line);
    }

    FileClose(file_handle);
    Print("");
    Print("CSV saved: ", filename);
    Print("MartingaleLotSizer EA removed from chart.");
    ExpertRemove();
    return(INIT_SUCCEEDED);
}

double NormalizeLots(double lots, double vol_min, double vol_max, double vol_step)
{
    if (vol_step > 0)
        lots = MathFloor(lots / vol_step) * vol_step;
    lots = MathMax(lots, vol_min);
    lots = MathMin(lots, vol_max);
    return NormalizeDouble(lots, (int)MathMax(0, -MathLog10(vol_step)));
}
