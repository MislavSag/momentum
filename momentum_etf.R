library(data.table)
library(PerformanceAnalytics)
library(roll)
library(ggplot2)
library(patchwork)
library(foreach)


# Import data
dt = fread("F:/strategies/momentum/prices.csv")

# Create yearmonth column
dt[, month := data.table::yearmon(date)]

# Sort
setorder(dt, symbol, date)

# Upsample to monthly data
dt_month = dt[, .(close = last(close),
                  beta = last(beta),
                  momentum_3 = last(momentum_3),
                  momentum_6 = last(momentum_6),
                  momentum_9 = last(momentum_9),
                  momentum_12 = last(momentum_12),
                  momentum_average = last(momentum_average),
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

# Calculate next month return
dt_month[, return := shift(close, -1, typ = "shift") / close - 1, by = symbol]

# Define ETF symbols we want to potentially invest in
symbols = c(
  'bal', 'corn', 'wood', 'cow', 'cow', 'jjg', 'soyb', 'sgg', 'weat', 'soyb',
  'nib',  'jo', 'soyb', 'weat', 'uso', 'uhn', 'bno', 'uso', 'ung', 'uga',
  'gld', 'cper', 'pall', 'slv', 'fxa', 'fxb', 'fxe', 'fxa', 'fxf', 'fxc',
  'fxm', 'uup', 'qqq', 'ewj', 'spy', 'iwm', 'dia', 'vxx', 'eem', 'vwo', "tlt"
)
dt_month = dt_month[symbol %in% symbols]

# Define parameters
beta_adjustment = c(TRUE, FALSE)
momentun_vars = colnames(dt_month)[grepl("momentum", colnames(dt_month))]
dvzw_thresholds = c(2:5, NA)
discreteness_log = c(TRUE, FALSE)
n = c(10, 20, 30, 50, 100)
params = expand.grid(
  momentun_vars = momentun_vars,
  dvzw_thresholds = dvzw_thresholds,
  discreteness_log = discreteness_log,
  n = n,
  stringsAsFactors = FALSE
)
setDT(params)

# Calculate momentum for all coarse universes
cluster = parallel::makeCluster(8, type = "PSOCK")
doParallel::registerDoParallel(cl = cluster)
foreach::getDoParRegistered()
foreach::getDoParWorkers()

start_time = Sys.time()
res = foreach(i=1:nrow(params),
              .combine='rbind',
              .packages = c("data.table", "xts", "PerformanceAnalytics")) %dopar% {
                # i = 5
                # print(i)
                # parameters
                momentum_ = params$momentun_vars[i]
                dvzw_ = params$dvzw_thresholds[i]
                discreteness_ = params$discreteness_log[i]
                n_ = params$n[i]

                dt_mom = na.omit(dt_month, cols = c(momentum_, "return"))
                if (!is.na(dvzw_)) {
                  if (momentum_ == "momentum_average") {
                    dt_mom = dt_mom[get(paste0("dollar_volume_zscore_winsorized", 12)) < dvzw_]
                  } else {
                    dt_mom = dt_mom[get(paste0("dollar_volume_zscore_winsorized", gsub("momentum_", "", momentum_))) < dvzw_]
                  }
                }
                if (discreteness_) {
                  if (momentum_ == "momentum_average") {
                    dt_mom = dt_mom[get(paste0("discreteness_", 12)) < 0]
                  } else {
                    dt_mom = dt_mom[get(paste0("discreteness_", gsub("momentum_", "", momentum_))) < 0]
                  }
                }
                setorderv(dt_mom, c("month", momentum_), order = c(1, -1))
                dt_mom = dt_mom[, first(.SD, n_), by = month]
                dt_mom[, weights := 1 / length(close), by = month]
                dt_mom[, all(weights == first(weights))] # test if all weights are the same
                dt_portfolio = dt_mom[, .(portfolio_return = sum(weights * return)), by = month]
                dt_xts = as.xts.data.table(dt_portfolio[, .(as.Date(zoo::as.yearmon(month)),
                                                            portfolio_return)])
                cbind(
                  annualized_return = Return.annualized(dt_xts),
                  sharpe_ratio = suppressMessages(SharpeRatio(dt_xts)[1, 1]),
                  max_dd = maxDrawdown(dt_xts)
                )
              }
end_time = Sys.time()
print(end_time - start_time)
# Time difference of 56.194 mins
# Stop cluster
parallel::stopCluster(cluster)

# Merge results and parameters
res = cbind.data.frame(params, res)
setDT(res)

# Order by sharpe ratio
setorder(res, -sharpe_ratio)

# Inspect results
head(res)
