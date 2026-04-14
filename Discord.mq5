/**=             Discord.mq5  (TyphooN's Discord EA Notification System)
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
#property version   "1.30"
double LastBullPowerHTF = -1;
double LastBearPowerHTF = -1;
double LastBullPowerLTF = -1;
double LastBearPowerLTF = -1;
datetime LastPowerNotification = 0;
const int NotificationCoolDown = 1200;
const int MaxVerifyAttempts = 10;
string g_cachedWebhookURL = "";  // O(1) lookup — resolved once in OnInit, _Symbol never changes
input string AgricultureAPIKey = "https://discord.com/api/webhooks/your_webhook_id/your_webhook_token";
input string CryptoAPIKey = "https://discord.com/api/webhooks/your_webhook_id/your_webhook_token";
input string EnergyAPIKey = "https://discord.com/api/webhooks/your_webhook_id/your_webhook_token";
input string ForexAPIKey = "https://discord.com/api/webhooks/your_webhook_id/your_webhook_token";
input string IndicesAPIKey = "https://discord.com/api/webhooks/your_webhook_id/your_webhook_token";
input string MetalAPIKey = "https://discord.com/api/webhooks/your_webhook_id/your_webhook_token";
input string StocksAPIKey = "https://discord.com/api/webhooks/your_webhook_id/your_webhook_token";
// Symbol-to-sector lookup arrays (pre-sorted for O(log n) binary search)
string EnergySymbols[]     = {"NATGAS.f", "UKOUSD", "USOUSD"};
string CryptoSymbols[]     = {"ADAUSD", "AVAXUSD", "BCHUSD", "BNBUSD", "BTCUSD", "DASHUSD",
                              "DOGEUSD", "DOTUSD", "ETHUSD", "FILUSD", "ICPUSD", "LINKUSD",
                              "LTCUSD", "MATICUSD", "SOLUSD", "TRXUSD", "UNIUSD", "VETUSD",
                              "XLMUSD", "XMRUSD", "XRPUSD"};
string MetalSymbols[]      = {"XAGUSD", "XAUUSD", "XPDUSD", "XPTUSD"};
string ForexSymbols[]      = {"AUDCAD.i", "AUDCHF.i", "AUDJPY.i", "AUDUSD.i", "CADCHF.i", "CADJPY.i", "CHFJPY.i",
                              "EURAUD.i", "EURCAD.i", "EURCHF.i", "EURGBP.i", "EURJPY.i", "EURUSD.i", "GBPAUD.i",
                              "GBPCAD.i", "GBPCHF.i", "GBPJPY.i", "GBPUSD.i", "USDCAD.i", "USDCHF.i", "USDJPY.i"};
string IndicesSymbols[]    = {"ASX200", "EUSTX50", "FRA40", "GER30", "HK50", "JPN225", "NDX100",
                              "SPN35", "SPX500", "UK100", "US2000.cash", "US30", "USDX", "USTN10.f"};
string AgricultureSymbols[]= {"COCOA.c", "COFFEE.c", "CORN.c", "SOYBEAN.c", "WHEAT.c"};
string StocksSymbols[]     = {"AAPL", "AIRF", "ALVG", "AMZN", "BABA", "BAC", "BAYGn",
                              "DBKGn", "GOOG", "IBE", "LVMH", "META", "MSFT", "NFLX",
                              "NVDA", "PFE", "RACE", "T", "TSLA", "V", "VOWG_p", "WMT", "ZM"};
int OnInit()
{
   g_cachedWebhookURL = GetWebhookURL();
   if(g_cachedWebhookURL == "")
      PrintFormat("Discord: WARNING — %s not mapped to any sector webhook, notifications disabled", _Symbol);
   return(INIT_SUCCEEDED);
}
bool IsSymbolInSorted(const string &arr[], string sym)
{
   int lo = 0, hi = ArraySize(arr) - 1;
   while(lo <= hi)
   {
      int mid = (lo + hi) / 2;
      int cmp = StringCompare(arr[mid], sym);
      if(cmp == 0) return true;
      if(cmp < 0) lo = mid + 1;
      else hi = mid - 1;
   }
   return false;
}
string GetWebhookURL()
{
   if(IsSymbolInSorted(EnergySymbols, _Symbol))      return EnergyAPIKey;
   if(IsSymbolInSorted(CryptoSymbols, _Symbol))      return CryptoAPIKey;
   if(IsSymbolInSorted(MetalSymbols, _Symbol))       return MetalAPIKey;
   if(IsSymbolInSorted(ForexSymbols, _Symbol))       return ForexAPIKey;
   if(IsSymbolInSorted(IndicesSymbols, _Symbol))     return IndicesAPIKey;
   if(IsSymbolInSorted(AgricultureSymbols, _Symbol)) return AgricultureAPIKey;
   if(IsSymbolInSorted(StocksSymbols, _Symbol))      return StocksAPIKey;
   return "";
}
bool ReadAndVerifyPower(double &bullHTF, double &bearHTF, double &bullLTF, double &bearLTF)
{
   if(!GlobalVariableCheck("GlobalBullPowerLTF") || !GlobalVariableCheck("GlobalBearPowerLTF") ||
      !GlobalVariableCheck("GlobalBullPowerHTF") || !GlobalVariableCheck("GlobalBearPowerHTF"))
      return false;
   int sleepDuration = 31337 + MathRand() % 7777;
   // Read initial values
   bullHTF = GlobalVariableGet("GlobalBullPowerHTF");
   bearHTF = GlobalVariableGet("GlobalBearPowerHTF");
   bullLTF = GlobalVariableGet("GlobalBullPowerLTF");
   bearLTF = GlobalVariableGet("GlobalBearPowerLTF");
   // Verify with 3 consecutive reads
   for(int attempt = 0; attempt < MaxVerifyAttempts; attempt++)
   {
      bool verified = true;
      for(int v = 0; v < 3; v++)
      {
         Sleep(sleepDuration);
         if(GlobalVariableGet("GlobalBullPowerHTF") != bullHTF ||
            GlobalVariableGet("GlobalBearPowerHTF") != bearHTF ||
            GlobalVariableGet("GlobalBullPowerLTF") != bullLTF ||
            GlobalVariableGet("GlobalBearPowerLTF") != bearLTF)
         {
            // Values changed, re-read and restart verification
            bullHTF = GlobalVariableGet("GlobalBullPowerHTF");
            bearHTF = GlobalVariableGet("GlobalBearPowerHTF");
            bullLTF = GlobalVariableGet("GlobalBullPowerLTF");
            bearLTF = GlobalVariableGet("GlobalBearPowerLTF");
            verified = false;
            break;
         }
      }
      double powerCalc = GlobalVariableGet("PowerCalcComplete");
      if(verified && powerCalc == 1.0)
         return true;
   }
   return false;
}
void SendPowerNotification()
{
   if(g_cachedWebhookURL == "") return;
   double bullHTF, bearHTF, bullLTF, bearLTF;
   if(!ReadAndVerifyPower(bullHTF, bearHTF, bullLTF, bearLTF))
      return;
   // Validate power values sum to 100
   if((bullHTF + bearHTF != 100) || (bullLTF + bearLTF != 100))
      return;
   // Check if values actually changed
   if(bullHTF == LastBullPowerHTF && bearHTF == LastBearPowerHTF &&
      bullLTF == LastBullPowerLTF && bearLTF == LastBearPowerLTF)
      return;
   // Update stored values
   LastBullPowerHTF = bullHTF;
   LastBearPowerHTF = bearHTF;
   LastBullPowerLTF = bullLTF;
   LastBearPowerLTF = bearLTF;
   string PowerText = StringFormat("[%s] [LTF Bull Power %.0f] [LTF Bear Power %.0f] [HTF Bull Power %.0f] [HTF Bear Power %.0f]",
                                   _Symbol, bullLTF, bearLTF, bullHTF, bearHTF);
   string json = "{\"content\":\"" + PowerText + "\"}";
   char jsonArray[];
   StringToCharArray(json, jsonArray);
   // Remove null-terminator
   int arrSize = ArraySize(jsonArray);
   if(arrSize > 0 && jsonArray[arrSize - 1] == '\0')
      ArrayResize(jsonArray, arrSize - 1);
   string headers = "Content-Type: application/json";
   uchar result[];
   string result_headers;
   int httpCode = WebRequest("POST", g_cachedWebhookURL, headers, 5000, jsonArray, result, result_headers);
   if(httpCode == -1)
   {
      int err = GetLastError();
      PrintFormat("Discord: WebRequest failed for %s (error %d) — check URL allowlist in Tools→Options→Expert Advisors", _Symbol, err);
   }
   else if(httpCode >= 400)
   {
      PrintFormat("Discord: webhook returned HTTP %d for %s", httpCode, _Symbol);
   }
   LastPowerNotification = TimeCurrent();
}
void OnTick()
{
   if(TimeCurrent() - LastPowerNotification >= NotificationCoolDown)
      SendPowerNotification();
}
