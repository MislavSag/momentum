library(data.table)
library(RollingWindow)
library(DescTools)


# # SET UP ------------------------------------------------------------------
# # global vars
# PATH = "F:/data/equity/us"


# PRICE DATA --------------------------------------------------------------
# Import QC daily data
prices = fread("F:/lean/data/stocks_daily.csv")
setnames(prices, gsub(" ", "_", c(tolower(colnames(prices)))))

# Remove duplicates
prices = unique(prices, by = c("symbol", "date"))

# Remove duplicates - there are same for different symbols (eg. phun and phun.1)
dups = prices[, .(symbol , n = .N),
              by = .(date, open, high, low, close, volume, adj_close,
                     symbol_first = substr(symbol, 1, 1))]
dups = dups[n > 1]
dups[, symbol_short := gsub("\\.\\d$", "", symbol)]
symbols_remove = dups[, .(symbol, n = .N),
                      by = .(date, open, high, low, close, volume, adj_close,
                             symbol_short)]
symbols_remove[n >= 2, unique(symbol)]
symbols_remove = symbols_remove[n >= 2, unique(symbol)]
symbols_remove = symbols_remove[grepl("\\.", symbols_remove)]
prices = prices[symbol %notin% symbols_remove]

# Adjust all columns
prices[, adj_rate := adj_close / close]
prices[, let(
  open = open*adj_rate,
  high = high*adj_rate,
  low = low*adj_rate
)]
setnames(prices, "close", "close_raw")
setnames(prices, "adj_close", "close")
prices[, let(adj_rate = NULL)]
setcolorder(prices, c("symbol", "date", "open", "high", "low", "close", "volume"))

# Remove observations where open, high, low, close columns are below 1e-008
# This step is opional, we need it if we will use finfeatures package
prices = prices[open > 1e-008 & high > 1e-008 & low > 1e-008 & close > 1e-008]

# Sort
setorder(prices, symbol, date)

# Calculate returns
prices[, returns := close / shift(close, 1) - 1]

# Remove missing values
prices = na.omit(prices)

# Set SPY returns as market returns
spy_ret = na.omit(prices[symbol == "spy", .(date, market_ret = returns)])
prices = spy_ret[prices, on = "date"]

# Minimal observations per symbol is 253 days
remove_symbols = prices[, .(symbol, n = .N), by = symbol][n < 253, symbol]
prices = prices[symbol %notin% remove_symbols]

# Free memory
gc()


# FILTERING ---------------------------------------------------------------
# Add label for most liquid asssets
prices[, dollar_volume_month := frollsum(close_raw * volume, 22, na.rm= TRUE), by = symbol]
calculate_liquid = function(prices, n) {
  # dt = copy(prices)
  # n = 500
  dt = copy(prices)
  setorder(dt, date, -dollar_volume_month)
  filtered = na.omit(dt)[, .(symbol = first(symbol, n)), by = date]
  col_ = paste0("liquid_", n)
  filtered[, (col_) := TRUE]
  dt = filtered[dt, on = c("date", "symbol")]
  dt[is.na(x), x := FALSE, env = list(x = col_)] # fill NA with FALSE
  return(dt)
}
prices = calculate_liquid(prices, 100)
prices = calculate_liquid(prices, 200)
prices = calculate_liquid(prices, 500)
prices = calculate_liquid(prices, 1000)

# Remove columns we don't need
prices[, dollar_volume_month := NULL]


# PREDICTORS --------------------------------------------------------------
# Rolling beta
setorder(prices, symbol, date)
prices = prices[, beta := RollingBeta(market_ret, returns, 252, na_method = "ignore"),
                by = symbol]

# Highest beta by date - 5% of symbols by highest beta
prices[, beta_rank := frank(abs(beta), ties.method = "dense", na.last = "keep"), by = date]
prices[, beta_rank_pct := beta_rank / max(beta_rank, na.rm = TRUE), by = date]
prices[, beta_rank_largest_99 := 0, by = date]
prices[beta_rank_pct > 0.99, beta_rank_largest_99 := 1, by = date]
prices[, beta_rank_largest_95 := 0, by = date]
prices[beta_rank_pct > 0.95, beta_rank_largest_95 := 1, by = date]
prices[, beta_rank_largest_90 := 0, by = date]
prices[beta_rank_pct > 0.90, beta_rank_largest_90 := 1, by = date]
setorder(prices, symbol, date)

# Momentum predictors
months_size = c(3, 6, 9, 12)
mom_vars = paste0("momentum_", months_size)
f_ = function(x, n) {
  shift(x, 21) / shift(x, n * 21) - 1
}
prices[, (mom_vars) := lapply(months_size, function(x) f_(close, x)), by = symbol]

# Dolar volume z-score
dv_cols = paste0("dollar_volume_zscore_winsorized", months_size)
f_ = function(x, y, z) RollingZscore(as.numeric(x * y), z, na_method = "ignore")
prices[, (dv_cols) := lapply(months_size, function(s)  as.vector(f_(close_raw, volume, s * 21))),
       by = symbol]
prices[, (dv_cols) := lapply(.SD, function(x) Winsorize(x, val = c(-5, 5))),
       .SDcols = dv_cols]


# REGIMES -----------------------------------------------------------------
# Check if market is in uptrend or downtrend in last 252, 125, 66 and 44 days
spy_regimes = na.omit(prices[symbol == "spy", .(date, close)])
spy_regimes[, let(
  regime_spy_up_252 = as.integer(close > shift(close, 252)),
  regime_spy_up_125 = as.integer(close > shift(close, 125)),
  regime_spy_up_66 = as.integer(close > shift(close, 66)),
  regime_spy_up_44 = as.integer(close > shift(close, 44))
)]
setnafill(spy_regimes,
          fill = 0,
          cols = c("regime_spy_up_252", "regime_spy_up_125",
                   "regime_spy_up_66", "regime_spy_up_44"))
prices = spy_regimes[, .SD, .SDcols = -"close"][prices, on = "date"]


# DISCRETENES -------------------------------------------------------------
# Momentum is a sustained increase or decrease in prices that we expect to continue. Therefore, we should filter out assets that have had large spikes up or down which are the main driver of their % change in price. We use information discreteness to capture how evenly distributed the changes in price are.
#
# We define information discreteness (ID) below:
#
#
#   It is effectively the direction of overall returns multiplied by the % of positive samples - the % of negative samples. For a strategy that uses daily bars, this would be the % of days that were negative minus the % of days positive.
#
# This lets us avoid news-driven price changes which have a much larger tendency to revert and of course, there is no expectation that we will see continued movement in price as the change was driven by the news. More on this in the PSD Momentum section.
# Measure discretenes from above
prices[, let(
  returns_pos = returns >= 0,
  returns_neg = returns < 0
)]
cols_pos = paste0("returns_pos_pct_", months_size)
prices[, (cols_pos) := lapply(months_size, function(x) frollmean(returns_pos, x * 21, na.rm = TRUE)),
       by = symbol]
cols_neg = paste0("returns_neg_pct_", months_size)
prices[, (cols_neg) := lapply(months_size, function(x) frollmean(returns_neg, x * 21, na.rm = TRUE)),
       by = symbol]
prices[, let(
  discreteness_3 = returns_neg_pct_3 - returns_pos_pct_3,
  discreteness_6 = returns_neg_pct_6 - returns_pos_pct_6,
  discreteness_9 = returns_neg_pct_9 - returns_pos_pct_9,
  discreteness_12 = returns_neg_pct_12 - returns_pos_pct_12
)]


# SAVE --------------------------------------------------------------------
# Remove columns we don't need
prices[, c("beta_rank", "beta_rank_pct", "open", "high", "low", "volume",
           "returns_pos", "returns_neg", cols_pos, cols_neg) := NULL]

# Save locally
fwrite(prices, "F:/strategies/momentum/prices.csv")
