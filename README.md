# MQL5-ExAmples
Standalone MQL5 scripts, EAs, and Python utilities for market data management and trading automation.

# Discord Market Wizardry Community / Darwinex Zero Coupon Code
- Join my Discord community for support with my EA and indicators, and also share trading ideas at http://marketwizardry.info/
- [Darwinex Zero (Discount CODE: TYPHOON)](https://www.darwinexzero.com?fpr=qsgtk&COUPON=TYPHOON) Certify your track record without putting your capital at risk.  Demonstrate your ability to achieve long-term returns and attract investment through Darwinex from $43/month.

# Scripts

| File | Type | Description |
|------|------|-------------|
| `DownloadFullHistoryFromBroker.mq5` | Script | Downloads complete OHLC history for all broker symbols across D1/W1/MN1 timeframes |
| `ExportSymbols.mq5` | Script | Exports comprehensive symbol metadata (30+ properties, ATR, VaR) to semicolon-delimited CSV |
| `ExportMonthlyOHLC.mq5` | Script | Exports monthly OHLC bars for all stock symbols to CSV (for data integrity auditing) |
| `ExportScrub.py` | Python | Enriches ExportSymbols CSV with Yahoo Finance monthly volume data |
| `CompareChartData.py` | Python | Compares Darwinex monthly OHLC against Yahoo Finance adjusted data to detect unadjusted corporate actions |
| `Discord.mq5` | EA | Multi-sector market notification system via Discord webhooks |
| `Market At Open.mq5` | EA | Queues a market order for execution when trading opens, with ATR-based SL/TP fallback |
| `OrderLoop.mq5` | EA | Batch order placer — opens orders in a loop until a target total lot size is reached |
| `LotsTotal.mq5` | Indicator | Displays total long/short volume and per-tick P/L impact for the current symbol |

# Data Audit Workflow

To detect symbols with unadjusted corporate actions (splits, mergers, restructurings) in broker data:

1. Run `DownloadFullHistoryFromBroker.mq5` to ensure full history is cached
2. Run `ExportMonthlyOHLC.mq5` to export monthly bars to CSV
3. Copy the CSV from MT5's `MQL5/Files/` directory
4. Run `python3 CompareChartData.py MonthlyOHLC-ServerName-Date.csv`
5. Review the generated anomaly report

# Usage
This project is intended and may be freely used for education and entertainment purposes.
However, **this project is not suitable for live trading** without relevant knowledge.

# License
The project is released under [GNU GPLv3 licence](https://www.gnu.org/licenses/quick-guide-gplv3.html),
so that means the software is copyrighted, however you have the freedom to use, change or share the software
for any purpose as long as the modified version stays free. See: [GNU FAQ](https://www.gnu.org/licenses/gpl-faq.html).

You should have received a copy of the GNU General Public License along with this program
(check the [LICENSE](LICENSE) file).
If not, please read <http://www.gnu.org/licenses/>.
For simplified version, please read <https://tldrlegal.com/license/gnu-general-public-license-v3-(gpl-3)>.

## Terms of Use
By using this software, you understand and agree that we (company and author)
are not be liable or responsible for any loss or damage due to any reason.
Although every attempt has been made to assure accuracy,
we do not give any express or implied warranty as to its accuracy.
We do not accept any liability for error or omission.

You acknowledge that you are familiar with these risks
and that you are solely responsible for the outcomes of your decisions.
We accept no liability whatsoever for any direct or consequential loss arising from the use of this product.
You understand and agree that past results are not necessarily indicative of future performance.

Use of this software serves as your acknowledgement and representation that you have read and understand
these TERMS OF USE and that you agree to be bound by such Terms of Use ("License Agreement").

# Copyright information
Copyright © 2023 - MarketWizardry.org - All Rights Reserved

# Disclaimer and Risk Warnings
Trading any financial market involves risk.
All forms of trading carry a high level of risk so you should only speculate with money you can afford to lose.
You can lose more than your initial deposit and stake.
Please ensure your chosen method matches your investment objectives,
familiarize yourself with the risks involved and if necessary seek independent advice.

NFA and CTFC Required Disclaimers:
Trading in the Foreign Exchange market as well as in Futures Market and Options or in the Stock Market
is a challenging opportunity where above average returns are available for educated and experienced investors
who are willing to take above average risk.
However, before deciding to participate in Foreign Exchange (FX) trading or in Trading Futures, Options or stocks,
you should carefully consider your investment objectives, level of experience and risk appetite.
**Do not invest money you cannot afford to lose**.

CFTC RULE 4.41 - HYPOTHETICAL OR SIMULATED PERFORMANCE RESULTS HAVE CERTAIN LIMITATIONS.
UNLIKE AN ACTUAL PERFORMANCE RECORD, SIMULATED RESULTS DO NOT REPRESENT ACTUAL TRADING.
ALSO, SINCE THE TRADES HAVE NOT BEEN EXECUTED, THE RESULTS MAY HAVE UNDER-OR-OVER COMPENSATED FOR THE IMPACT,
IF ANY, OF CERTAIN MARKET FACTORS, SUCH AS LACK OF LIQUIDITY. SIMULATED TRADING PROGRAMS IN GENERAL
ARE ALSO SUBJECT TO THE FACT THAT THEY ARE DESIGNED WITH THE BENEFIT OF HINDSIGHT.
NO REPRESENTATION IS BEING MADE THAN ANY ACCOUNT WILL OR IS LIKELY TO ACHIEVE PROFIT OR LOSSES SIMILAR TO THOSE SHOWN.
