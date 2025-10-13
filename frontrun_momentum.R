library(data.table)
library(PerformanceAnalytics)


# Prices
prices = qc_daily_parquet(
  file_path = "F:/lean/data/all_stocks_daily",
  etfs = FALSE,
  min_obs = 510,
  duplicates = "fast",
  add_dv_rank = FALSE,
  add_day_of_month = TRUE,
  market_symbol = "spy",
)
prices[, month := data.table::yearmon(date)]
prices[, etf := NULL]

# Add label for most liquid asssets
prices[, dollar_volume := close_raw * volume]
dvm = prices[, sum(dollar_volume, na.rm = TRUE), by = .(symbol, month)]
setorder(dvm, month, -V1)
prices = dvm[, head(.SD, 50), by = month] |>
  _[, .(symbol, month)] |>
  _[, dvm_50 := 1] |>
  _[prices, on = c("symbol", "month")]
prices = dvm[, head(.SD, 100), by = month] |>
  _[, .(symbol, month)] |>
  _[, dvm_100 := 1] |>
  _[prices, on = c("symbol", "month")]
prices = dvm[, head(.SD, 200), by = month] |>
  _[, .(symbol, month)] |>
  _[, dvm_200 := 1] |>
  _[prices, on = c("symbol", "month")]
prices = dvm[, head(.SD, 500), by = month] |>
  _[, .(symbol, month)] |>
  _[, dvm_500 := 1] |>
  _[prices, on = c("symbol", "month")]
prices = dvm[, head(.SD, 1000), by = month] |>
  _[, .(symbol, month)] |>
  _[, dvm_1000 := 1] |>
  _[prices, on = c("symbol", "month")]
prices = dvm[, head(.SD, 2000), by = month] |>
  _[, .(symbol, month)] |>
  _[, dvm_2000 := 1] |>
  _[prices, on = c("symbol", "month")]

# Checks
prices[, .N, by = day_of_month]

# Momentum variables
prices[, mom_intra_month := close / data.table::first(close) - 1, by = .(symbol, month)]
prices[, target := data.table::last(close) / data.table::first(close) - 1, by = .(symbol, month)]

# Downsample
dt = prices[day_of_month == 21]
dt[, target := shift(target, 1, type = "lead"), by = .(symbol)]
# dt[, mom_intra_month := shift(mom_intra_month, 1), by = symbol]
dt = na.omit(dt, cols = "target")
dt = dt[dvm_100 == TRUE]

# Remove stocks with less raw price than 1$
nrow(dt[close_raw <= 2]) / nrow(dt)
dt = dt[close_raw > 2]

# Remove stocks with volume lower than 10.000
nrow(dt[volume <= 100000]) / nrow(dt)
dt = dt[volume > 100000]

# Select stocks with biggest mom by mont
returns = dt[order(month, -mom_intra_month)] |>
  _[, head(.SD, 100), by = month] |>
  _[target < 200] |>
  _[, sum(target * (1 / length(target)), na.rm = TRUE), by = month]
returns_xts = as.xts.data.table(returns[, .(month = as.Date(zoo::as.yearmon(month)), ret = V1)])
charts.PerformanceSummary(returns_xts)
SharpeRatio.annualized(returns_xts, scale = 12)
Return.annualized(returns_xts, scale = 12)
# charts.PerformanceSummary(returns_xts["2025"])
