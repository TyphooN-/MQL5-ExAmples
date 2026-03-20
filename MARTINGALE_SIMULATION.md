# XNGUSD Martingale Lot Simulation — $100,000 Virtual Balance

**Symbol:** XNGUSD
**Margin per 0.10 lot:** ~$316.40
**Volume Min:** 0.10 | **Volume Step:** 0.10
**Base Lot:** 0.10 | **Multiplier:** 2x
**VaR per 0.10 lot:** $145.00

## With Max Levels Capped at 7 (Recommended Safe Limit)

| Level | Lots  | Margin     | Cum Lots | Cum Margin  | % Balance | VaR        |
|-------|-------|------------|----------|-------------|-----------|------------|
| 1     | 0.10  | $316.40    | 0.10     | $316.40     | 0.32%     | $145.00    |
| 2     | 0.20  | $632.80    | 0.30     | $949.20     | 0.95%     | $290.00    |
| 3     | 0.40  | $1,265.60  | 0.70     | $2,214.80   | 2.21%     | $580.00    |
| 4     | 0.80  | $2,531.20  | 1.50     | $4,746.00   | 4.75%     | $1,160.00  |
| 5     | 1.60  | $5,062.40  | 3.10     | $9,808.40   | 9.81%     | $2,320.00  |
| 6     | 3.20  | $10,124.80 | 6.30     | $19,933.20  | 19.93%    | $4,640.00  |
| 7     | 6.40  | $20,249.60 | 12.70    | $40,182.80  | 40.18%    | $9,280.00  |

**Totals at max level 7:** 12.70 cumulative lots, $40,182.80 margin (40.18% of balance)
**Remaining free margin:** ~$59,817.20 for floating drawdown

## Without Max Level Cap (Until Margin Exhaustion)

| Level | Lots  | Margin      | Cum Lots | Cum Margin   | % Balance | VaR         | Status      |
|-------|-------|-------------|----------|--------------|-----------|-------------|-------------|
| 1     | 0.10  | $316.40     | 0.10     | $316.40      | 0.32%     | $145.00     | Safe        |
| 2     | 0.20  | $632.80     | 0.30     | $949.20      | 0.95%     | $290.00     | Safe        |
| 3     | 0.40  | $1,265.60   | 0.70     | $2,214.80    | 2.21%     | $580.00     | Safe        |
| 4     | 0.80  | $2,531.20   | 1.50     | $4,746.00    | 4.75%     | $1,160.00   | Safe        |
| 5     | 1.60  | $5,062.40   | 3.10     | $9,808.40    | 9.81%     | $2,320.00   | Safe        |
| 6     | 3.20  | $10,124.80  | 6.30     | $19,933.20   | 19.93%    | $4,640.00   | Caution     |
| 7     | 6.40  | $20,249.60  | 12.70    | $40,182.80   | 40.18%    | $9,280.00   | Warning     |
| 8     | 12.80 | $40,499.20  | 25.50    | $80,682.00   | 80.68%    | $18,560.00  | Danger      |
| 9     | 25.60 | $80,998.40  | 51.10    | $161,680.40  | 161.68%   | $37,120.00  | IMPOSSIBLE  |

**Maximum possible depth:** 8 levels (25.50 cumulative lots)
**At level 8:** $80,682.00 margin used (80.68%), leaving only ~$19,318 for floating loss
**Level 9 requires $161,680.40 — exceeds $100,000 balance**

## Risk Notes

- **Level 7 cap recommended:** leaves 60% of balance as free margin for drawdown
- **Level 8 is technically possible** but leaves less than 20% free margin — a small adverse move triggers margin call
- **Level 9 is impossible** on a $100k account
- VaR figures are daily Value at Risk (95% confidence, 20-day lookback)
- Margin values based on live XNGUSD rates as of 2026.03.13
