library(data.table)
library(PerformanceAnalytics)
library(QuantTools)
library(arrow)
library(dplyr)


# Import data
dt = fread("F:/strategies/momentum/sample_.csv")

# Create yearmonth column
dt[, month := data.table::yearmon(date)]

# Sort
setorder(dt, symbol, date)

# Upsample to monthly data
dt_month = dt[, .(close = last(close),
                  date_first = first(date),
                  date_last = last(date),
                  beta = last(beta),
                  liquid_1000 = last(liquid_1000),
                  liquid_500 = last(liquid_500),
                  liquid_200 = last(liquid_200),
                  liquid_100 = last(liquid_100),
                  beta_rank_largest_90 = last(beta_rank_largest_90),
                  beta_rank_largest_95 = last(beta_rank_largest_95),
                  beta_rank_largest_99 = last(beta_rank_largest_99),
                  regime_spy_up_44 = last(regime_spy_up_44),
                  regime_spy_up_66 = last(regime_spy_up_66),
                  regime_spy_up_125 = last(regime_spy_up_125),
                  regime_spy_up_252 = last(regime_spy_up_252),
                  momentum_3 = last(momentum_3),
                  momentum_6 = last(momentum_6),
                  momentum_9 = last(momentum_9),
                  momentum_12 = last(momentum_12),
                  dollar_volume_zscore_winsorized3 = last(dollar_volume_zscore_winsorized3),
                  dollar_volume_zscore_winsorized6 = last(dollar_volume_zscore_winsorized6),
                  dollar_volume_zscore_winsorized9 = last(dollar_volume_zscore_winsorized9),
                  dollar_volume_zscore_winsorized12 = last(dollar_volume_zscore_winsorized12),
                  discreteness_3 = last(discreteness_3),
                  discreteness_6 = last(discreteness_6),
                  discreteness_9 = last(discreteness_9),
                  discreteness_12 = last(discreteness_12)
),
by = .(symbol, month)]

# Free memory
rm(dt)
gc()

# Hour data data set
ds = open_dataset('F:/lean/data/stocks_hour.csv', format = 'csv')

# Loop over months and calculate pra
years_ = dt_month[month >= 2000, unique(as.integer(month))]

dt_month[, length(unique(symbol))]
sample_[, length(unique(symbol))]

for (y in years_[1:2]) {
  # y = years_[1]

  # Sample data for month
  dtm = dt_month[as.integer(month) == y]

  # Get hour data for same symbols and month
  symbols = dtm[, unique(symbol)]
  date_end = dtm[, max(date_last, na.rm = TRUE)]
  date_start = dtm[, min(date_first, na.rm = TRUE)]
  date_start_buffer = date_start - (365*2)
  system.time({
    sample_ = ds %>%
      select(Symbol, Date, `Adj Close`) %>%
      filter(Date >= date_start_buffer, Date <= date_end) %>%
      filter(Symbol %in% symbols) %>%
      arrange(Symbol, Date) %>%
      rename(symbol = Symbol, date = Date, close = `Adj Close`) %>%
      collect()
  })
  # user  system elapsed
  # 70.16    6.77   64.24
  setDT(sample_)

  # Calculate returns
  sample_[, returns := close / shift(close, 1) - 1, by = symbol]

  # Calculate PRA
  windows = c(7*21, 7*21*3, 7*21*6, 7*252, 7*252*2)
  cols_pra = paste0("pra_", windows)
  system.time({
    sample_[, (cols_pra) := lapply(windows, function(x) roll_percent_rank(close, x)),
            by = symbol]
  })
  # head(sample_[date > as.POSIXct("2000-01-01") & symbol == "aapl"], 30)

  # Create dummy variables for 999 percentile
  cols = paste0("pra_", windows)
  cols_above_999 = paste0("pr_above_dummy_", windows)
  sample_[, (cols_above_999) := lapply(.SD, function(x) ifelse(x > 0.999, 1, 0)), .SDcols = cols]
  cols_below_001 = paste0("pr_below_dummy_", windows)
  sample_[, (cols_below_001) := lapply(.SD, function(x) ifelse(x < 0.001, 1, 0)), .SDcols = cols]
  cols_net_1 = paste0("pr_below_dummy_net_", windows)
  sample_[, (cols_net_1) := sample_[, ..cols_above_999] - sample_[, ..cols_below_001]]

  # Create dummy variables for 99 percentile
  cols_above_99 = paste0("pr_above_dummy_99_", windows)
  sample_[, (cols_above_99) := lapply(.SD, function(x) ifelse(x > 0.99, 1, 0)), .SDcols = cols]
  cols_below_01 = paste0("pr_below_dummy_01_", windows)
  sample_[, (cols_below_01) := lapply(.SD, function(x) ifelse(x < 0.01, 1, 0)), .SDcols = cols]
  cols_net_2 = paste0("pr_below_dummy_net_0199", windows)
  sample_[, (cols_net_2) := sample_[, ..cols_above_99] - sample_[, ..cols_below_01]]

  # get risk measures
  indicators = sample_[, lapply(.SD, sum, na.rm = TRUE), .SDcols = c(
    colnames(sample_)[grep("pra_\\d+", colnames(sample_))],
    cols_above_999,
    cols_above_99,
    cols_below_001,
    cols_below_01,
    cols_net_1,
    cols_net_2
  ), by = .(date)]
  setorder(indicators, "date")

  # Merge indicators and sample_
  sample_ = sample_[, .(symbol, date, close, returns)][indicators, on = 'date']
  backtest_data = na.omit(sample_)
  backtest_data[, returns := close / shift(close) - 1]
  backtest_data = na.omit(backtest_data)
}






# Indicators
sample_[, pra_1764_above_dummy := ifelse(pra_1764 > 0.999, 1, 0)]
sample_[, pra_1764_below_dummy := ifelse(pra_1764 < 0.001, 1, 0)]
sample_[, pr_below_dummy_net := pra_1764_above_dummy - pra_1764_below_dummy]
indicators_sum = sample_[, lapply(.SD, sum, na.rm = TRUE),
                        .SDcols = c(
                          "pra_1764_above_dummy",
                          "pra_1764_below_dummy",
                          "pr_below_dummy_net"
                        ),
                        by = .(date)]

# Merge sample and indicators
sample_ = merge(sample_[, .(symbol, date, returns)], indicators_sum, by = "date")

# Filter date range
sample_ = sample_[date %between% c(as.POSIXct(date_start), as.POSIXct(date_end))]

# Portfolio performance
sample_[, weights := 1 / nrow(.SD), by = date]
sample_[, min(pra_1764_below_dummy)]
sample_[, max(pra_1764_below_dummy)]
thresh = 100
dt_portfolio = sample_[, .(symbol,
                           date,
                           returns,
                           weights = fifelse(shift(pra_1764_below_dummy, 1) > thresh, 0, weights))]
dt_portfolio = dt_portfolio[, .(portfolio_return = sum(weights * returns)), by = date]


dt_xts = as.xts.data.table(dt_portfolio[, .(date, portfolio_return)])



# Test
# charts.PerformanceSummary(dt_xts)
# cbind(
#   annualized_return = Return.annualized(dt_xts),
#   sharpe_ratio = suppressMessages(SharpeRatio(dt_xts)[1, 1]),
#   max_dd = maxDrawdown(dt_xts)
# )


# # Calculate PRA
# windows = c(7 * 22, 7 * 22 * 3, 7 * 22 * 6, 252, 252 * 2, 252 * 4)
# cols_pra = paste0("pra_", windows)
# sample_[, (cols_pra) := lapply(windows, function(x) roll_percent_rank(close, x)), by = symbol]
#
# # Crate dummy variables
# cols = paste0("pra_", windows)
# cols_above_999 = paste0("pr_above_dummy_", windows)
# sample_[, (cols_above_999) := lapply(.SD, function(x) ifelse(x > 0.999, 1, 0)), .SDcols = cols]
# cols_below_001 = paste0("pr_below_dummy_", windows)
# sample_[, (cols_below_001) := lapply(.SD, function(x) ifelse(x < 0.001, 1, 0)), .SDcols = cols]
# cols_net_1 = paste0("pr_below_dummy_net_", windows)
# sample_[, (cols_net_1) := sample_[, ..cols_above_999] - sample_[, ..cols_below_001]]
#
# cols_above_99 = paste0("pr_above_dummy_99_", windows)
# sample_[, (cols_above_99) := lapply(.SD, function(x) ifelse(x > 0.99, 1, 0)), .SDcols = cols]
# cols_below_01 = paste0("pr_below_dummy_01_", windows)
# sample_[, (cols_below_01) := lapply(.SD, function(x) ifelse(x < 0.01, 1, 0)), .SDcols = cols]
# cols_net_2 = paste0("pr_below_dummy_net_0199", windows)
# sample_[, (cols_net_2) := sample_[, ..cols_above_99] - sample_[, ..cols_below_01]]
#
# cols_above_97 = paste0("pr_above_dummy_97_", windows)
# sample_[, (cols_above_97) := lapply(.SD, function(x) ifelse(x > 0.97, 1, 0)), .SDcols = cols]
# cols_below_03 = paste0("pr_below_dummy_03_", windows)
# sample_[, (cols_below_03) := lapply(.SD, function(x) ifelse(x < 0.03, 1, 0)), .SDcols = cols]
# cols_net_3 = paste0("pr_below_dummy_net_0397", windows)
# sample_[, (cols_net_3) := sample_[, ..cols_above_97] - sample_[, ..cols_below_03]]
#
# cols_above_95 = paste0("pr_above_dummy_95_", windows)
# sample_[, (cols_above_95) := lapply(.SD, function(x) ifelse(x > 0.95, 1, 0)), .SDcols = cols]
# cols_below_05 = paste0("pr_below_dummy_05_", windows)
# sample_[, (cols_below_05) := lapply(.SD, function(x) ifelse(x < 0.05, 1, 0)), .SDcols = cols]
# cols_net_4 = paste0("pr_below_dummy_net_0595", windows)
# sample_[, (cols_net_4) := sample_[, ..cols_above_95] - sample_[, ..cols_below_05]]
#
# get risk measures
indicators_sum = sample_[, lapply(.SD, sum, na.rm = TRUE),
                        .SDcols = c(
                          colnames(sample_)[grep("pra_\\d+", colnames(sample_))],
                          cols_above_999,
                          cols_above_99,
                          cols_below_001,
                          cols_below_01,
                          cols_above_97,
                          cols_below_03,
                          cols_above_95,
                          cols_below_05,
                          cols_net_1,
                          cols_net_2,
                          cols_net_3,
                          cols_net_4
                        ),
                        by = .(time)]
setnames(indicators_sum, c("time", paste0("sum_", colnames(indicators_sum)[-1])))
indicators_sd = sample_[, lapply(.SD, sd, na.rm = TRUE),
                       .SDcols = c(
                         colnames(sample_)[grep("pra_\\d+", colnames(sample_))],
                         cols_above_999,
                         cols_above_99,
                         cols_below_001,
                         cols_below_01,
                         cols_above_97,
                         cols_below_03,
                         cols_above_95,
                         cols_below_05,
                         cols_net_1,
                         cols_net_2,
                         cols_net_3,
                         cols_net_4
                       ),
                       by = .(time)]
setnames(indicators_sd, c("time", paste0("sd_", colnames(indicators_sd)[-1])))
indicators = Reduce(function(x, y) merge(x, y, by = "time"),
                    list(indicators_sum, indicators_sd))
indicators = unique(indicators, by = c("time"))
setorder(indicators, "time")
