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

# Paths
PATH = "H:/strategies/momentum"
if (!dir.exists(PATH)) dir.create(PATH)


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

# Save
fwrite(dt, file.path(PATH, "mom_dt.csv"))
