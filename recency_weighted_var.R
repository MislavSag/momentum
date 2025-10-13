# Build weights (oldest -> newest)
build_weights <- function(n,
                          scheme = c("triangular", "half_life", "exp_lambda", "custom"),
                          half_life = 6, lambda = 0.2, custom_w = NULL,
                          normalize = TRUE) {
  scheme <- match.arg(scheme)
  stopifnot(n >= 1)

  w <- switch(scheme,
              triangular = seq_len(n),                       # 1..n
              half_life  = { beta <- 2^(-1/half_life); beta^(n - seq_len(n)) },
              exp_lambda = { beta <- exp(-lambda);           beta^(n - seq_len(n)) },
              custom     = {
                stopifnot(length(custom_w) == n)
                custom_w
              }
  )
  if (normalize) w <- w / sum(w)
  w
}
