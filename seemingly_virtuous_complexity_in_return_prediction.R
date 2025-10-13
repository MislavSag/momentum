library(data.table)
library(finutils)
library(ggplot2)
library(PerformanceAnalytics)



# UTILS -------------------------------------------------------------------
# Linear weights
linear_weights <- function(T) {
  w <- (T - 0:(T - 1))
  rev(w / sum(w))
}
all((12:1) / sum(12:1) == linear_weights(12))
all((20:1) / sum(20:1) == linear_weights(20))

# Weighted var
roll_weighted <- function(x, w) sum(x * w, na.rm = TRUE)
x = runif(12)
roll_weighted(x, linear_weights(12))


# INDIVIDUAL SYMBOL -------------------------------------------------------
# Import daily data
prices = qc_daily_parquet(
  file_path = "F:/lean/data/all_stocks_daily",
  min_obs = 252,
  duplicates = "fast",
  add_dv_rank = FALSE,
  add_day_of_month = FALSE,
  symbol = "spy"
)

# Parameters
T_lookback   <- 12     # months for momentum signal
lambda_decay <- 0.20   # recency emphasis (higher -> more recent weight)
L_vol        <- 12     # months for volatility estimate
sigma_star   <- 0.12   # target annualized vol
w_max        <- 1.00   # cap position size
long_only    <- FALSE  # TRUE = long/flat, FALSE = long/short

# Create monthly returns
prices[, month := data.table::yearmon(date)]
rm = prices[, .(ret = prod(1 + returns, na.rm = TRUE) - 1), by = month]

# RECENCY-WEIGHTED MOMENTUM SIGNAL
# Use only info available at t-1
rm[, ret_lag := shift(ret, 1)]
rm[, mom := frollapply(ret_lag, n = T_lookback, FUN = roll_weighted, w = linear_weights(T_lookback))]
plot(rm[, mom])

# VOLATILITY TIMING + CAPPING
# Annualized vol from lagged returns
rm[, vol := frollapply(ret_lag, n = L_vol, FUN = sd) * sqrt(12)]
# guard against zero/NA vol
rm[, vol := fifelse(is.finite(vol) & vol > 0, vol, NA_real_)]

# raw exposure and scaling to target vol
rm[, w_t := mom / vol]
# rm[, w_t  := raw_w * (sigma_star / vol)]

# cap exposure
# rm[, w_t := pmax(pmin(w_t,  w_max), -w_max)]
if (long_only) rm[, w_t := pmax(w_t, 0)]

# STRATEGY RETURNS & STATS
# Execute with one-month delay
rm[, strat_ret := shift(w_t, 1, type = "lag", fill = 0) * ret]

ann_mean <- 12 * mean(rm$strat_ret, na.rm = TRUE)
ann_vol  <- sqrt(12) * sd(rm$strat_ret,  na.rm = TRUE)
sharpe   <- ifelse(ann_vol > 0, ann_mean / ann_vol, NA_real_)

cat(sprintf("SPY Strategy — Annualized return: %5.2f%%\n", 100 * ann_mean))
cat(sprintf("SPY Strategy — Annualized vol   : %5.2f%%\n", 100 * ann_vol))
cat(sprintf("SPY Strategy — Sharpe (ann)     : %5.2f\n", sharpe))


# --- 5) PLOTS ----------------------------------------------------------------
# Equity curve
ec <- na.omit(rm[, .(month, strat_ret)])
ec[, cum := cumprod(1 + strat_ret)]

p1 <- ggplot(ec, aes(month, cum)) +
  geom_line() +
  labs(title = "SPY Recency-Weighted Momentum (Vol-Timed) — Equity Curve",
       x = NULL, y = "Cumulative wealth (=$1 start)") +
  theme_minimal()

# Position over time
p2 <- ggplot(rm, aes(month, w_t)) +
  geom_line() +
  labs(title = "SPY Position (w_t)", x = NULL, y = "Weight") +
  theme_minimal()

print(p1)
print(p2)

# Drawdowns (needs xts)
R <- xts::xts(ec$strat_ret, order.by = ec$month)
charts.PerformanceSummary(R, main = "SPY Strategy Performance")
