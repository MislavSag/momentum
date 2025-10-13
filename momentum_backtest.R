library(fastverse)
library(finutils)
library(RollingWindow)
library(DescTools)
library(ggplot2)
library(patchwork)
library(PerformanceAnalytics)
library(qlcal)
library(AzureStor)


# CONFIG ------------------------------------------------------------------
# Timezone
calendars
setCalendar("UnitedStates/NYSE")


# DATA --------------------------------------------------------------------
# Prices
prices = qc_daily_parquet(
  file_path = "F:/lean/data/all_stocks_daily",
  etfs = FALSE,
  min_obs = 510,
  duplicates = "fast",
  add_dv_rank = FALSE,
  add_day_of_month = FALSE,
  market_symbol = "spy"
)
prices[, month := data.table::yearmon(date)]


# PREDICTORS --------------------------------------------------------------
# Momentum predictors
setorder(prices, symbol, date)
months_size = 2:12
mom_vars = paste0("momentum_", months_size)
f_ = function(x, n) {
  shift(x, 21) / shift(x, n * 21) - 1
}
prices[, (mom_vars) := lapply(months_size, function(x) f_(close, x)), by = symbol]

# Momentum ensambles
weights_ = c(12:2) / sum(12:2)
prices[, momentum_average_year :=
         momentum_2 * weights_[1] +
         momentum_3 * weights_[2] +
         momentum_4 * weights_[3] +
         momentum_5 * weights_[4] +
         momentum_6 * weights_[5] +
         momentum_7 * weights_[6] +
         momentum_8 * weights_[7] +
         momentum_9 * weights_[8] +
         momentum_10 * weights_[9] +
         momentum_11 * weights_[10] +
         momentum_12 * weights_[11]
]

# Rolling sd
setorder(prices, symbol, date)
prices[, sd_roll_year := roll::roll_sd(returns, width = 252), by = symbol]
prices[, sd_roll_halfyear := roll::roll_sd(returns, width = 150), by = symbol]
prices[, sd_roll_month := roll::roll_sd(returns, width = 22), by = symbol]
weights_ = c(252, 150, 22) / sum(c(252, 150, 22))
prices[, sd_average_year :=
         sd_roll_year * weights_[3] +
         sd_roll_halfyear * weights_[2] +
         sd_roll_month * weights_[1]
]

# Standardized momentum, delete others
prices[, mom := momentum_average_year / sd_average_year]

# Keep only columns we need
remove_cols = c(mom_vars, colnames(prices)[grepl("sd_roll", colnames(prices))],
                "sd_average_year", "momentum_average_year", "etf")
prices = prices[, .SD, .SDcols = -remove_cols]


# FILTERING ---------------------------------------------------------------
# Repeat filtering
prices[, dollar_volume := close_raw * volume]
setorder(prices, symbol, month)
dt = prices[, .(
  open       = data.table::first(open),
  close      = data.table::last(close),
  close_raw  = data.table::last(close_raw),
  volume     = sum(volume, na.rm = TRUE),
  date_first = data.table::first(date),
  date_last  = data.table::last(date),
  mom        = data.table::first(mom),
  dollar_vol = sum(dollar_volume, na.rm = TRUE)
), by = .(symbol, month)]

# Remove stocks with less raw price than 1$
nrow(dt[close_raw <= 5]) / nrow(dt)
dt = dt[close_raw > 5]

# Remove stocks with volume lower than 10.000
nrow(dt[volume <= 100000]) / nrow(dt)
dt = dt[volume > 100000]

# Target variable
setorder(dt, symbol, month)
dt[, target := close / open - 1]
dt[, target := shift(target, 1, type = "lead"), by = symbol]
dt = na.omit(dt, cols = "target")

# Highest n by dollar volume
dt[, bin := dplyr::ntile(dollar_vol, 10), by = month]


# BACKTEST FUNCTION -------------------------------------------------------
# Create a backtest function
backtest = function(x, binn = NULL, n = 100) {
  back = na.omit(x)
  if (!is.null(binn)) {
    back = back[bin == binn]
  }
  setorder(back, month, -mom)
  back = back[, head(.SD, n), by = month]
  back = back[, .(strategy = sum(target * (1/length(target)))), by = month]
  back = as.xts.data.table(back[, .(zoo::as.yearmon(month), strategy)])
  back = back[back < 2]
  back
}
performance = function(back) {
  cat(
    "Sharpe", round(SharpeRatio.annualized(back, scale = 12), 2), "\n",
    "CAGR",   round(Return.annualized(back, scale = 12), 2), "\n",
    "Stdev",  round(StdDev.annualized(back, scale = 12), 2), "\n"
  )
  charts.PerformanceSummary(back)
}


# BASIC STRATEGY ----------------------------------------------------------
# Backtest
back = backtest(dt)
performance(back)


# UNIVERSES ---------------------------------------------------------------
# Results across universes
backs = lapply(1:10, function(i) backtest(dt, binn = i, 30))
backs = Reduce(function(x, y) cbind(x, y), backs)
charts.PerformanceSummary(backs)
SharpeRatio.annualized(backs, scale = 12)
as.data.table(SharpeRatio.annualized(backs, scale = 12)) |>
  melt() |>
  ggplot(aes(variable, value)) +
  geom_bar(stat = "identity")

# Lets include stocks qwith highest dollar volume!
dt_ = dt[bin == 1]
dt_[, .N, by = month][order(month)][, plot(N)]

# Performance again
back_ = backtest(dt, 1, 30)
performance(back_)
charts.PerformanceSummary(back_["2020/"])

# Save symbols for Quantconnect
qc_data = na.omit(dt)
qc_data = qc_data[bin == 1]
setorder(qc_data, month, -mom)
qc_data = qc_data[, head(.SD, 30), by = month]
qc_data[, .(strategy = sum(target * (1/length(target)))), by = month] |>
  _[, .(zoo::as.yearmon(month), strategy)] |>
  as.xts.data.table() |>
  charts.PerformanceSummary()
qc_data = qc_data[, .(symbol, date_last, month)]
qc_data[, max(date_last), by = month]
qc_data = qc_data[, .(date = max(date_last)), by = month][qc_data, on = "month"]
qc_data = qc_data[, .(symbol, date)]
qc_data[, date := lubridate::ceiling_date(date, unit = "month")]
# qc_data[, date_test := as.Date(vapply(date, advanceDate, days = 1L, bdc = "Following", FUN.VALUE = double(1L)))]
# qc_data[date_test >= as.Date("2021-01-01")]
qc_data[, date := as.character(date)]
qc_data[, symbol := gsub("\\.\\d+", "", symbol)]
bl_endp_key = storage_endpoint(Sys.getenv("BLOB-ENDPOINT-SNP"),
                               Sys.getenv("BLOB-KEY-SNP"))
cont = storage_container(bl_endp_key, "qc-backtest")
storage_write_csv(as.data.frame(qc_data), cont, "momentum.csv")


# REGIME ------------------------------------------------------------------
# Golden rule on spy




