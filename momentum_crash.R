library(fastverse)
library(finutils)
library(RollingWindow)
library(DescTools)
library(ggplot2)
library(patchwork)
library(PerformanceAnalytics)
library(qlcal)
library(AzureStor)
library(roll)
library(QuantTools)


# CONFIG ------------------------------------------------------------------
# Timezone
calendars
setCalendar("UnitedStates/NYSE")


# DATA --------------------------------------------------------------------
# Prices
prices = qc_daily_parquet(
  file_path = "C:/Users/Mislav/qc_snp/data/all_stocks_daily",
  min_obs = 510,
  duplicates = "fast",
  add_dv_rank = FALSE,
  add_day_of_month = FALSE,
  market_symbol = "spy",
  etfs = FALSE,
  market_cap_fmp_file = NULL,
  profiles_fmp_file = NULL
)
prices[, month := data.table::yearmon(date)]

# SPY
spy = qc_daily_parquet(
  file_path = "C:/Users/Mislav/qc_snp/data/all_stocks_daily",
  duplicates = "fast",
  add_dv_rank = FALSE,
  add_day_of_month = FALSE,
  symbols = "spy"
)
spy[, month := data.table::yearmon(date)]


# PREDICTORS --------------------------------------------------------------
# Rolling time series regressions (by symbol)
iv = function(dt, window = 22) {
  # Column names with window suffix
  iv_col    = paste0("IV_", window)
  ivol_col  = paste0("IVOL_", window)
  iskew_col = paste0("ISKEW_", window)
  gmb_col   = paste0("GMB_", window)
  resid_col = paste0("resid_", window)

  # 1) rolling regression
  dt[, c("b0","b_mkt","b_mom") := {
    fit = roll::roll_lm(
      # x = cbind(spy_returns, mom),
      x = spy_returns,
      y = returns,
      width = window,
      intercept = TRUE,
      online = FALSE
    )
    as.data.table(fit$coefficients)
  }, by = symbol]

  # 2) residuals
  # dt[, (resid_col) := returns - (b0 + b_mkt * spy_returns + b_mom * mom)]
  dt[, (resid_col) := returns - (b0 + b_mkt * spy_returns)]

  # 3) IV / IVOL / ISKEW
  dt[, (iv_col) := frollmean(get(resid_col)^2, n = window), by = symbol]
  dt[, (ivol_col) := sqrt(get(iv_col))]
  dt[, m3 := frollmean(get(resid_col)^3, n = window), by = symbol]
  dt[, (iskew_col) := fifelse(get(iv_col) > 0, m3 / (get(iv_col)^(3/2)), NA_real_)]

  # 4) Good minus Bad variance (GMB)
  dt[, IVp := frollmean((get(resid_col)^2) * (get(resid_col) > 0), n = window), by = symbol]
  dt[, IVm := frollmean((get(resid_col)^2) * (get(resid_col) < 0), n = window), by = symbol]
  dt[, (gmb_col) := fifelse(get(iv_col) > 0, (IVp - IVm) / get(iv_col), NA_real_)]

  # Momentum predictors
  months_size = 2:12
  mom_vars = paste0("momentum_", months_size)
  f_ = function(x, n) {
    shift(x, 21) / shift(x, n * 21) - 1
  }
  dt[, (mom_vars) := lapply(months_size, function(x) f_(get(resid_col), x)), by = symbol]

  # Momentum ensambles
  weights_ = c(12:2) / sum(12:2)
  dt[, momentum_average_year :=
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

  # Clean up temps
  dt[, c("b0","b_mkt", "m3","IVp","IVm") := NULL]
  dt[, paste0("momentum_", 2:12) := NULL]
  setorder(dt, symbol, date)

  return(dt)
}
setorder(prices, symbol, date)
iv(prices)
# iv(prices, 66)

prices[, mom := momentum_average_year / (IVOL_22 * sqrt(252))]
prices[symbol == "aapl"]

# Keep only columns we need
remove_cols = c(colnames(prices)[grepl("sd_roll", colnames(prices))],
                "momentum_average_year", "inv_vehicle", "b_mom")
prices = prices[, .SD, .SDcols = -remove_cols]


# UNIVERSE ----------------------------------------------------------------
# ADV20 in USD
setorder(prices, symbol, date)
prices[, dollar_vol := close_raw * volume]
prices[, adv20 := frollmean(dollar_vol, 22), by = symbol]

# Coarse universe filtering
# # 1) etf
# dt = prices[spy == 1]
# 1) dv
dt = prices[close_raw > 2 & adv20 > 1e6]

# Cap very big or very low values
hist(dt[, mom])
hist(dt[mom %between% c(-200, 200), mom])
paste0(nrow(dt[mom > 200]) / nrow(dt) * 100, "%")
paste0(nrow(dt[mom < -200]) / nrow(dt) * 100, "%")
dt = dt[mom %between% c(-200, 200)]

# Remove NA values
dt = na.omit(dt, cols = c("mom", "IV_22"))

# Descriptive
summary(dt[, mom])
summary(dt[, GMB_22])
cor(dt[, .(mom, IV_22, GMB_22)])
dt[, .N, by = date]


# MOMENTUM SORTING --------------------------------------------------------
# Convert to monthly and set index to stocks we would hold long
dtm = dt[, .(mom = data.table::last(mom)), by = .(symbol, month)]
dtm[, mom_rank := frankv(mom, order = -1L, ties.method = "first"), by = month]
dtm[, mom_rank := shift(mom_rank, 1L, type = "lead"), by = symbol]
dtm[, mom_rank_short := frankv(mom, order = 1L, ties.method = "first"), by = month]
dtm[, mom_rank_short := shift(mom_rank_short, 1L, type = "lead"), by = symbol]
anyDuplicated(dtm[, .(symbol, month)])

# Merge dt and dtm
# dt[, mom_rank := NULL]
dts = merge(dt, dtm[, .SD, .SDcols = -"mom"], by = c("symbol", "month"), all.x = TRUE, all.y = FALSE)

# Define target variables
setorder(dts, symbol, date)
dts[, target := shift(close, 1, type = "lead") / close - 1, by = symbol]

# Results without controling crash
back = na.omit(dts)[mom_rank <= 50]
portfolio = back[, .(strategy = sum(target * (1/length(target)), na.rm = TRUE)), by = date]
setorder(portfolio, date)
portfolio = as.xts.data.table(portfolio)
charts.PerformanceSummary(portfolio)
SharpeRatio.annualized(portfolio, scale = 252)
Return.annualized(portfolio, scale = 252)
StdDev.annualized(portfolio, scale = 252)
min(Drawdowns(portfolio))

# Results without controling crash - SHORT
back = na.omit(dts)[mom_rank_short <= 50]
portfolio = back[, .(strategy = sum(target * (1/length(target)), na.rm = TRUE)), by = date]
setorder(portfolio, date)
portfolio = as.xts.data.table(portfolio)
charts.PerformanceSummary(portfolio)
SharpeRatio.annualized(portfolio, scale = 252)
Return.annualized(portfolio, scale = 252)
StdDev.annualized(portfolio, scale = 252)
min(Drawdowns(portfolio))

# Results acroos bins
na.omit(dts) |>
  _[, bin := dplyr::ntile(mom_rank, 20), by = month] |>
  _[, mean(target), by = bin] |>
  _[order(bin)] |>
  ggplot(aes(bin, V1)) +
  geom_bar(stat = "identity")
na.omit(dts) |>
  _[, bin := dplyr::ntile(mom_rank, 20), by = month] |>
  _[, median(target), by = bin] |>
  _[order(bin)] |>
  ggplot(aes(bin, V1)) +
  geom_bar(stat = "identity")

# Results with positive GBM
dts[, hist(GMB_22)]
dts[, hist(ISKEW_22)]
dts[IVOL_22 < 0.2, hist(IVOL_22)]
back = na.omit(dts) |>
  _[GMB_22 < -0.1] |>
  _[mom_rank <= 50]
portfolio = back[, .(strategy = sum(target * (1/length(target)), na.rm = TRUE)), by = date]
setorder(portfolio, date)
portfolio = as.xts.data.table(portfolio)
charts.PerformanceSummary(portfolio)
SharpeRatio.annualized(portfolio, scale = 252)
Return.annualized(portfolio, scale = 252)
StdDev.annualized(portfolio, scale = 252)
min(Drawdowns(portfolio))

# Check portfolios
back[month == 2025, unique(symbol)]



# AGGREGATE RISK FOR EVERY DAY --------------------------------------------
#
risk = dts[, .(iv_22_agg = mean(IV_22, na.rm = TRUE)), by = date]
setorder(risk, date)
risk = risk[iv_22_agg < 0.1]
risk[, iv_agg_fast := TTR::EMA(iv_22_agg, 50)]
risk[, iv_agg_slow := TTR::EMA(iv_22_agg, 20)]
risk = na.omit(risk)
plot(as.xts.data.table(risk))
summary(as.xts.data.table(risk))
spy_risk = risk[spy, on = "date"]
spy_risk[, target := shift(close, 1, type = "lead") / close - 1]
spy_risk[, signal := (iv_agg_fast < iv_agg_slow) | iv_22_agg < 0.001]
spy_risk[, strategy := signal * target]
spy_risk = na.omit(spy_risk)
spy_risk = as.xts.data.table(spy_risk[, .(date, strategy)])
charts.PerformanceSummary(spy_risk)
SharpeRatio.annualized(spy_risk)
StdDev.annualized(spy_risk)

# MICRO CRASH INDEX -------------------------------------------------------
# --- SHOCK components (first differences, scaled by own 60d sd) ---
setorder(dt, symbol, date)
dt[, dIVOL  := IVOL_22  - shift(IVOL_22),  by=symbol]
dt[, dNSKEW := (-ISKEW_22) - shift(-ISKEW_22), by=symbol]  # negate: more neg skew = riskier
dt[, dNGMB  := (-GMB_22)  - shift(-GMB_22),  by=symbol]    # negate: lower GMB = riskier

# Standard deviations
dt[, `:=`(
  S_ivol  = dIVOL  / roll_sd(dIVOL, 60),
  S_nskew = dNSKEW / roll_sd(dNSKEW, 60),
  S_ngmb  = dNGMB  / roll_sd(dNGMB, 60)
), by = symbol]

# CS percentiles each day
dt[, P_ivol_CS  := frankv( IVOL_22,  ties.method = "first") / .N, by = date]
dt[, P_nskew_CS := frankv(-ISKEW_22, ties.method = "first") / .N, by = date]
dt[, P_ngmb_CS  := frankv(-GMB_22,   ties.method = "first") / .N, by = date]

# --- LEVEL components (time-series and cross-sectional percentiles) ---
# TS percentiles over 252d window
# scalar percentile of the *last* element within the window
roll_last_pctile <- function(x) {
  n <- sum(!is.na(x))
  if (n == 0L || is.na(x[length(x)])) return(NA_real_)
  r <- rank(x, ties.method = "average", na.last = "keep")
  r[length(x)] / n
}

# Time-series percentiles (per symbol) over a 252d lookback
dt[, P_ivol_TS  := roll_percent_rank( IVOL_22,  252) / 100, by = symbol]
dt[, P_nskew_TS := roll_percent_rank(-ISKEW_22, 252) / 100, by = symbol]
dt[, P_ngmb_TS  := roll_percent_rank(-GMB_22,   252) / 100, by = symbol]

# --- Micro CRASH index (0..~2) ---
dt[, jump_flag := as.integer(pmax(S_ivol, S_nskew, S_ngmb, na.rm=TRUE) > 2)]
dt[, CRASH_MICRO := 0.30*((P_ivol_TS + P_ivol_CS)/2) +
     0.30*((P_nskew_TS + P_ngmb_TS)/2) +
     0.30*((S_ivol + S_nskew + S_ngmb)/3) +
     0.10*jump_flag]


# MACRO CRASH INDEX -------------------------------------------------------
# daily cross-sectional aggregates
mkt = dt[, .(
  IVOL_mkt  = mean(IVOL_22,  na.rm=TRUE),
  NSKEW_mkt = mean(-ISKEW_22, na.rm=TRUE),
  NGMB_mkt  = mean(-GMB_22,   na.rm=TRUE)
), by=date]

# rolling z-scores (centered & scaled) over 252d
roll_z <- function(x, n=252){
  mu <- frollmean(x, n)
  sd_ <- frollapply(x, n, sd, align="right")
  (x - mu) / sd_
}
mkt[, `:=`(
  Z_ivol  = roll_z(IVOL_mkt),
  Z_nskew = roll_z(NSKEW_mkt),
  Z_ngmb  = roll_z(NGMB_mkt)
)]

# Macro crash index (higher = riskier)
mkt[, CRASH_MACRO := 0.5*Z_ivol + 0.3*Z_nskew + 0.2*Z_ngmb]
ggplot(mkt, aes(date, CRASH_MACRO)) +
  geom_line()

# map back to dt
dt[mkt, on="date", CRASH_MACRO := i.CRASH_MACRO]

# 0..1 percentile over 252d window
mkt[, MACRO_PCTL := roll_percent_rank(CRASH_MACRO, 252) / 100]
ggplot(mkt, aes(date, MACRO_PCTL)) +
  geom_line()

# join back to dt
dt[mkt[, .(date, MACRO_PCTL)], on = "date", MACRO_PCTL := i.MACRO_PCTL]

# Test MACRO crash on SPY
spy_crash = mkt[spy, on = c("date")]
spy_crash[, target := shift(close, 1L, type = "lead") / close - 1]
spy_crash = na.omit(spy_crash)
spy_crash[, hist(CRASH_MACRO)]
spy_crash[, cor(CRASH_MACRO, target)]
ggplot(spy_crash, aes(CRASH_MACRO, target)) +
  geom_smooth(method = "lm") +
  geom_point()
spy_crash[, signal := (CRASH_MACRO < -1)]
spy_crash[, strategy := signal * target]
strategy = as.xts.data.table(spy_crash[, .(date, strategy)])
charts.PerformanceSummary(strategy)
SharpeRatio.annualized(strategy, scale = 252)
Return.annualized(strategy, scale = 252)


# PORTFOLIO ---------------------------------------------------------------
#
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

# Target variable
setorder(dt, symbol, month)
dt[, target := close / open - 1]
dt[, target := shift(target, 1, type = "lead"), by = symbol]
dt = na.omit(dt, cols = "target")

# Highest n by dollar volume
dt[, bin := dplyr::ntile(dollar_vol, 10), by = month]


# PERFORMANCE -------------------------------------------------------------
# Returns over bins
plot_ret_over_rank = function(r) {
  dt[, .(ret = mean(target, na.rm = TRUE)), .(bin = dplyr::ntile(x, 20)), env = list(x = r)] |>
    _[order(ret)] |>
    ggplot(aes(bin, ret)) +
    geom_bar(stat = "identity")
}
plot_ret_over_rank("rank")
# plot_ret_over_rank("rank_idio")
# plot_ret_over_rank("rank_neg")
plot_ret_over_rank("rank_bundle_22")
plot_ret_over_rank("rank_bundle_66")














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




