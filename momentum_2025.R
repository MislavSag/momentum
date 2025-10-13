library(fastverse)
library(finutils)
library(RollingWindow)
library(DescTools)
library(ggplot2)
library(patchwork)
library(PerformanceAnalytics)


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


# PREDICTORS --------------------------------------------------------------
# Rolling beta
setorder(prices, symbol, date)
prices = prices[, beta := RollingBeta(spy_returns, returns, 252, na_method = "ignore"),
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
months_size = 2:36
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
weights_ = c(24:2) / sum(24:2)
prices[, momentum_average_year2 :=
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
         momentum_12 * weights_[11] +
         momentum_13 * weights_[12] +
         momentum_14 * weights_[13] +
         momentum_15 * weights_[14] +
         momentum_16 * weights_[15] +
         momentum_17 * weights_[16] +
         momentum_18 * weights_[17] +
         momentum_19 * weights_[18] +
         momentum_20 * weights_[19] +
         momentum_21 * weights_[20] +
         momentum_22 * weights_[21] +
         momentum_23 * weights_[22] +
         momentum_24 * weights_[23]
]
weights_ = c(36:2) / sum(36:2)
prices[, momentum_average_year3 :=
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
         momentum_12 * weights_[11] +
         momentum_13 * weights_[12] +
         momentum_14 * weights_[13] +
         momentum_15 * weights_[14] +
         momentum_16 * weights_[15] +
         momentum_17 * weights_[16] +
         momentum_18 * weights_[17] +
         momentum_19 * weights_[18] +
         momentum_20 * weights_[19] +
         momentum_21 * weights_[20] +
         momentum_22 * weights_[21] +
         momentum_23 * weights_[22] +
         momentum_24 * weights_[23] +
         momentum_25 * weights_[24] +
         momentum_26 * weights_[25] +
         momentum_27 * weights_[26] +
         momentum_28 * weights_[27] +
         momentum_29 * weights_[28] +
         momentum_30 * weights_[29] +
         momentum_31 * weights_[30] +
         momentum_32 * weights_[31] +
         momentum_33 * weights_[32] +
         momentum_34 * weights_[33] +
         momentum_35 * weights_[34] +
         momentum_36 * weights_[35]
]

# Rolling sd
setorder(prices, symbol, month)
prices[, sd_roll_year := roll::roll_sd(returns, width = 252), by = symbol]

# Dolar volume z-score
dv_cols = paste0("dollar_volume_zscore_winsorized", months_size)
f_ = function(x, y, z) RollingZscore(as.numeric(x * y), z, na_method = "ignore")
prices[, (dv_cols) := lapply(months_size, function(s)  as.vector(f_(close_raw, volume, s * 21))),
       by = symbol]
prices[, (dv_cols) := lapply(.SD, function(x) Winsorize(x, val = c(-5, 5))),
       .SDcols = dv_cols]


# REGIMES -----------------------------------------------------------------
# Check if market is in uptrend or downtrend in last 252, 125, 66 and 44 days
spy_regimes = unique(na.omit(prices[, .(date, close = spy_returns)]))
setorder(spy_regimes, date)
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


# FILTERS -----------------------------------------------------------------
# Downsample
dt = prices[, .(
  close      = data.table::last(close),
  close_raw  = data.table::last(close_raw),
  volume     = sum(volume, na.rm = TRUE),
  date_first = data.table::first(date),
  date_last  = data.table::last(date),
  beta = last(beta),
  beta_rank_largest_90 = last(beta_rank_largest_90),
  beta_rank_largest_95 = last(beta_rank_largest_95),
  beta_rank_largest_99 = last(beta_rank_largest_99),
  regime_spy_up_44 = last(regime_spy_up_44),
  regime_spy_up_66 = last(regime_spy_up_66),
  regime_spy_up_125 = last(regime_spy_up_125),
  regime_spy_up_252 = last(regime_spy_up_252),
  momentum_2  = data.table::last(momentum_2),
  momentum_3  = data.table::last(momentum_3),
  momentum_4  = data.table::last(momentum_4),
  momentum_5  = data.table::last(momentum_5),
  momentum_6  = data.table::last(momentum_6),
  momentum_7  = data.table::last(momentum_7),
  momentum_8  = data.table::last(momentum_8),
  momentum_9  = data.table::last(momentum_9),
  momentum_10 = data.table::last(momentum_10),
  momentum_11 = data.table::last(momentum_11),
  momentum_12 = data.table::last(momentum_12),
  momentum_13 = data.table::last(momentum_13),
  momentum_14 = data.table::last(momentum_14),
  momentum_15 = data.table::last(momentum_15),
  momentum_16 = data.table::last(momentum_16),
  momentum_17 = data.table::last(momentum_17),
  momentum_18 = data.table::last(momentum_18),
  momentum_19 = data.table::last(momentum_19),
  momentum_20 = data.table::last(momentum_20),
  momentum_21 = data.table::last(momentum_21),
  momentum_22 = data.table::last(momentum_22),
  momentum_23 = data.table::last(momentum_23),
  momentum_24 = data.table::last(momentum_24),
  momentum_25 = data.table::last(momentum_25),
  momentum_26 = data.table::last(momentum_26),
  momentum_27 = data.table::last(momentum_27),
  momentum_28 = data.table::last(momentum_28),
  momentum_29 = data.table::last(momentum_29),
  momentum_30 = data.table::last(momentum_30),
  momentum_31 = data.table::last(momentum_31),
  momentum_32 = data.table::last(momentum_32),
  momentum_33 = data.table::last(momentum_33),
  momentum_34 = data.table::last(momentum_34),
  momentum_35 = data.table::last(momentum_35),
  momentum_36 = data.table::last(momentum_36),
  momentum_average_year = data.table::last(momentum_average_year),
  momentum_average_year2 = data.table::last(momentum_average_year2),
  momentum_average_year3 = data.table::last(momentum_average_year3)
), by = .(symbol, month)]

# Remove stocks with less raw price than 1$
nrow(dt[close_raw <= 2]) / nrow(dt)
dt = dt[close_raw > 2]

# Remove stocks with volume lower than 10.000
nrow(dt[volume <= 100000]) / nrow(dt)
dt = dt[volume > 100000]

# Target variable
setorder(dt, symbol, month)
dt[, target := shift(close, 1, type = "lead") / close - 1, by = symbol]
dt = na.omit(dt, cols = "target")


# WHICH MOMENTUM ----------------------------------------------------------
# Prepare data
mom_cols = colnames(dt)[grepl("mom", colnames(dt))]
dt_ = dt[, .SD, .SDcols = c("symbol", "month", "target", mom_cols)]
dt_ = melt(dt_, id.vars = c("symbol", "month", "target"))
dt_[, bin := dplyr::ntile(value, 10), by = .(month, variable)]
dt_ = na.omit(dt_)
dt_[variable == "momentum_average_year"]

# Long only
portfolios = na.omit(dt_[bin == 10]) |>
  _[target < 1] |>
  _[, sum(target * (1/length(target))), by = .(month, variable)] |>
  _[order(variable, month)] |>
  _[, .(ret_cum = Return.cumulative(V1),
        ret_anu = Return.annualized(V1, scale = 12),
        sharpe  = SharpeRatio.annualized(.SD[, .(zoo::as.yearmon(month), V1)], scale = 12)
        ), by = variable]
ggplot(portfolios[, .(sharpe, variable)], aes(variable, sharpe)) +
  geom_bar(stat = "identity")

# One year average seems very good


# DIVIDE OR NOT -----------------------------------------------------------
# Repeat filtering
dt = prices[, .(
  close      = data.table::last(close),
  close_raw  = data.table::last(close_raw),
  volume     = sum(volume, na.rm = TRUE),
  date_first = data.table::first(date),
  date_last  = data.table::last(date),
  beta = last(beta),
  sd_roll_year = data.table::last(sd_roll_year),
  mom = data.table::last(momentum_average_year)
), by = .(symbol, month)]

# Remove stocks with less raw price than 1$
nrow(dt[close_raw <= 2]) / nrow(dt)
dt = dt[close_raw > 2]

# Remove stocks with volume lower than 10.000
nrow(dt[volume <= 100000]) / nrow(dt)
dt = dt[volume > 100000]

# Target variable
setorder(dt, symbol, month)
dt[, target := shift(close, 1, type = "lead") / close - 1, by = symbol]
dt = na.omit(dt, cols = "target")

# Divide by beta and compare
dt[, mom_beta := mom / beta]
dt[, bin := dplyr::ntile(mom, 10), by = .(month)]
dt[, bin_beta := dplyr::ntile(mom_beta, 10), by = .(month)]
dt = na.omit(dt)
portfolios = dt[, .(symbol, month, target, bin)]|>
  _[target < 1] |>
  _[, sum(target * (1/length(target))), by = .(month, bin)] |>
  _[order(bin, month)] |>
  _[, .(ret_cum = Return.cumulative(V1),
        ret_anu = Return.annualized(V1, scale = 12),
        sharpe  = SharpeRatio.annualized(.SD[, .(zoo::as.yearmon(month), V1)], scale = 12)
  ), by = bin]
g1 = ggplot(portfolios[, .(sharpe, bin)], aes(bin, sharpe)) +
  geom_bar(stat = "identity")
portfolios = dt[, .(symbol, month, target, bin_beta)]|>
  _[target < 1] |>
  _[, sum(target * (1/length(target))), by = .(month, bin_beta)] |>
  _[order(bin_beta, month)] |>
  _[, .(ret_cum = Return.cumulative(V1),
        ret_anu = Return.annualized(V1, scale = 12),
        sharpe  = SharpeRatio.annualized(.SD[, .(zoo::as.yearmon(month), V1)], scale = 12)
  ), by = bin_beta]
g2 = ggplot(portfolios[, .(sharpe, bin_beta)], aes(bin_beta, sharpe)) +
  geom_bar(stat = "identity")
g1 / g2

# Divide by sd and compare
dt[, mom_beta := mom / sd_roll_year]
dt[, bin := dplyr::ntile(mom, 10), by = .(month)]
dt[, bin_beta := dplyr::ntile(mom_beta, 10), by = .(month)]
dt = na.omit(dt)
portfolios = dt[, .(symbol, month, mom, target, bin)]|>
  _[target < 1] |>
  _[, sum(target * (1/length(target))), by = .(month, bin)] |>
  _[order(bin, month)] |>
  _[, .(ret_cum = Return.cumulative(V1),
        ret_anu = Return.annualized(V1, scale = 12),
        sharpe  = SharpeRatio.annualized(.SD[, .(zoo::as.yearmon(month), V1)], scale = 12)
  ), by = bin]
g1 = ggplot(portfolios[, .(sharpe, bin)], aes(bin, sharpe)) +
  geom_bar(stat = "identity")
portfolios = dt[, .(symbol, month, mom, target, bin_beta)]|>
  _[target < 1] |>
  _[, sum(target * (1/length(target))), by = .(month, bin_beta)] |>
  _[order(bin_beta, month)] |>
  _[, .(ret_cum = Return.cumulative(V1),
        ret_anu = Return.annualized(V1, scale = 12),
        sharpe  = SharpeRatio.annualized(.SD[, .(zoo::as.yearmon(month), V1)], scale = 12)
  ), by = bin_beta]
g2 = ggplot(portfolios[, .(sharpe, bin_beta)], aes(bin_beta, sharpe)) +
  geom_bar(stat = "identity")
g1 / g2

# Divide by sd



# HIGH BETA OR ALL --------------------------------------------------------
dt = prices[, .(
  close      = data.table::last(close),
  close_raw  = data.table::last(close_raw),
  volume     = sum(volume, na.rm = TRUE),
  date_first = data.table::first(date),
  date_last  = data.table::last(date),
  sd_roll_year = data.table::last(sd_roll_year),
  mom = data.table::last(momentum_average_year),
  beta_rank_largest_90 = last(beta_rank_largest_90),
  beta_rank_largest_95 = last(beta_rank_largest_95),
  beta_rank_largest_99 = last(beta_rank_largest_99)
), by = .(symbol, month)]

# Remove stocks with less raw price than 1$
nrow(dt[close_raw <= 2]) / nrow(dt)
dt = dt[close_raw > 2]

# Remove stocks with volume lower than 10.000
nrow(dt[volume <= 100000]) / nrow(dt)
dt = dt[volume > 100000]

# Target variable
setorder(dt, symbol, month)
dt[, target := shift(close, 1, type = "lead") / close - 1, by = symbol]
dt = na.omit(dt, cols = "target")

# Divide by sd and compare
dt[, mom_sd := mom / sd_roll_year]
g1 = na.omit(dt) |>
  _[beta_rank_largest_90  == 0] |>
  _[, bin := dplyr::ntile(mom, 10), by = .(month)] |>
  _[, .(symbol, month, mom_sd, target, bin)]|>
  _[target < 1] |>
  _[, sum(target * (1/length(target))), by = .(month, bin)] |>
  _[order(bin, month)] |>
  _[, .(ret_cum = Return.cumulative(V1),
        ret_anu = Return.annualized(V1, scale = 12),
        sharpe  = SharpeRatio.annualized(.SD[, .(zoo::as.yearmon(month), V1)], scale = 12)
  ), by = bin] |>
  ggplot(aes(bin, sharpe)) +
  geom_bar(stat = "identity")
g2 = na.omit(dt) |>
  _[beta_rank_largest_95  == 0] |>
  _[, bin := dplyr::ntile(mom, 10), by = .(month)] |>
  _[, .(symbol, month, mom_sd, target, bin)]|>
  _[target < 1] |>
  _[, sum(target * (1/length(target))), by = .(month, bin)] |>
  _[order(bin, month)] |>
  _[, .(ret_cum = Return.cumulative(V1),
        ret_anu = Return.annualized(V1, scale = 12),
        sharpe  = SharpeRatio.annualized(.SD[, .(zoo::as.yearmon(month), V1)], scale = 12)
  ), by = bin] |>
  ggplot(aes(bin, sharpe)) +
  geom_bar(stat = "identity")
g3 = na.omit(dt) |>
  _[beta_rank_largest_99  == 0] |>
  _[, bin := dplyr::ntile(mom, 10), by = .(month)] |>
  _[, .(symbol, month, mom_sd, target, bin)]|>
  _[target < 1] |>
  _[, sum(target * (1/length(target))), by = .(month, bin)] |>
  _[order(bin, month)] |>
  _[, .(ret_cum = Return.cumulative(V1),
        ret_anu = Return.annualized(V1, scale = 12),
        sharpe  = SharpeRatio.annualized(.SD[, .(zoo::as.yearmon(month), V1)], scale = 12)
  ), by = bin] |>
  ggplot(aes(bin, sharpe)) +
  geom_bar(stat = "identity")
g4 = na.omit(dt) |>
  _[, bin := dplyr::ntile(mom, 10), by = .(month)] |>
  _[, .(symbol, month, mom_sd, target, bin)]|>
  _[target < 1] |>
  _[, sum(target * (1/length(target))), by = .(month, bin)] |>
  _[order(bin, month)] |>
  _[, .(ret_cum = Return.cumulative(V1),
        ret_anu = Return.annualized(V1, scale = 12),
        sharpe  = SharpeRatio.annualized(.SD[, .(zoo::as.yearmon(month), V1)], scale = 12)
  ), by = bin] |>
  ggplot(aes(bin, sharpe)) +
  geom_bar(stat = "identity")
g1 / g2 / g3 / g4

# Don't see nothing here!

