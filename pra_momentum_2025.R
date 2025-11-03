library(data.table)
library(PerformanceAnalytics)
library(QuantTools)
library(arrow)
library(dplyr)


# Import data
dt = fread("H:/strategies/momentum/mom_dt.csv")

# Sort
setorder(dt, symbol, date)

# Upsample to monthly data
dt_month = dt[, .(
  open       = data.table::first(open),
  close      = data.table::last(close),
  date_first = data.table::first(date),
  date_last  = data.table::last(date),
  mom        = data.table::last(close),
  adv20      = data.table::last(adv20),
  gmb        = data.table::last(GMB_22),
  ivol       = data.table::last(IVOL_22)
), by = .(symbol, month)]
setorder(dt_month, symbol, month)
dt_month[, target := close]

# Hour data data set
prices = open_dataset('C:/Users/Mislav/qc_snp/data/all_stocks_hour', format = 'parquet') |>
  dplyr::select(Symbol, Date, `Adj Close`) |>
  dplyr::arrange(Symbol, Date) |>
  dplyr::rename(symbol = Symbol, date = Date, close = `Adj Close`) |>
  collect()
setDT(prices)
prices[, returns := close / shift(close, 1) - 1, by = symbol]

# Calculate PRA
windows = c(7*21, 7*21*3, 7*21*6, 7*252, 7*252*2)
cols_pra = paste0("pra_", windows)
prices[, (cols_pra) := lapply(windows, function(x) roll_percent_rank(close, x)),
       by = symbol]

# Loop over months and calculate pra
months = dt_month[month >= 2000, month]
for (i in seq_along(months)) {
  # debug
  print(i)

  # Sample data for month
  month_ = months[i]
  dtm = dt_month[month == month_]

  # Get hour data for same symbols and month
  symbols = dtm[, unique(symbol)]
  date_end = dtm[, max(date_last, na.rm = TRUE)]
  date_start = dtm[, min(date_first, na.rm = TRUE)]
  date_start_buffer = date_start - (365*2)

  # Sample hour prices
  sample_ = prices[date %between% c(date_start_buffer, date_end)] |>
    _[symbol %chin% symbols]

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
