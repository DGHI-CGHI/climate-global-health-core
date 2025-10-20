################################################################################
# Script:   wbgt_smoothing_schema_diagnostics.R
# Purpose:  Compare three hourly W B G T smoothing schemas (98/1/1, 80/10/10,
#           50/25/25) by examining their effects on daily statistics.
#
# What this script does
# ---------------------
# 1) Loads daily, geography-level WBGT summaries (Parquet) produced by your
#    ERA5 aggregation pipeline:
#       E:/data/gridded/era5land/<geog-name>/gridded_output_parquet-<date>/daily/
#    The dataset must contain the daily max/min/mean under three smoothing
#    choices, e.g.:
#       wbgt_max98, wbgt_min98, wbgt_mean98
#       wbgt_max80, wbgt_min80, wbgt_mean80
#       wbgt_max50, wbgt_min50, wbgt_mean50
#    plus: localdate, year, GISJOIN (geography key).
#
# 2) Builds pairwise deltas between schemas (98-50, 80-50, 98-80) for each of:
#    daily max, daily min, and daily mean.
#
# 3) Produces multiple summaries to understand the magnitude and shape of
#    differences:
#    . Global summary across all dates/places: mean/median/sd, RMSE vs "50",
#      quantiles, and correlations by metric (max/min/mean).
#    . Monthly delta summary (statewide): average and RMSE of the pairwise
#      deltas for each month.
#    . Spatial delta summary (by GISJOIN): average deltas across time to
#      identify locations most sensitive to the smoothing choice.
#    . Correlations between schemas for each metric (global).
#    . Threshold-exceedance disagreement: for chosen WBGT thresholds, how often
#      do schema classifications (hot/not) disagree?
#    . "Top schema" ranking: which schema most often yields the *largest* daily
#      max for a given place/day.
#
# 4) Practical-significance analysis:
#    Computes, for a grid of thresholds ?? in [0.1, 4.0] °C, the fraction of days
#    where |schema A ??? schema B| ??? ?? for daily *max* WBGT (98 vs 50, 80 vs 50,
#    98 vs 80). Produces:
#    . A tidy data.table of (delta, pair, fraction).
#    . Two base R visualizations:
#        - Line plot of fraction vs ?? for each pair.
#        - Barplot at ?? = 2 °C (editable).
#
# Inputs (expected columns)
# -------------------------
# . localdate  (Date or "YYYY-MM-DD" string; auto-converted if needed)
# . year       (integer or coercible)
# . GISJOIN    (character geography key)
# . wbgt_max98, wbgt_min98, wbgt_mean98
# . wbgt_max80, wbgt_min80, wbgt_mean80
# . wbgt_max50, wbgt_min50, wbgt_mean50
#
# Outputs
# -------
# . Printed tables to console:
#     global_summary, monthly_delta_summary, spatial_delta_summary,
#     cor_summary, exceedance_summary, rank_summary
# . Optional CSV exports (commented fwrite() lines near the end)
# . Two base-R figures drawn to the active device:
#     - Fraction vs ?? multi-line plot
#     - Barplot at ?? = 2 °C
#
# How to run (example)
# --------------------
#   library(data.table); library(arrow)
#   source("wbgt_smoothing_schema_diagnostics.R")
#   # Ensure the path in open_dataset() points to your daily parquet root
#
# Key assumptions & notes
# -----------------------
# . Daily parquet has already been computed from hourly data for your chosen
#   geography (e.g., blockgroup, tract, county) and contains the three schema
#   variants (98, 80, 50) of daily max/min/mean WBGT.
# . Missing values are handled with na.rm=TRUE; all-NA vectors yield NA
#   summaries where appropriate.
# . The script treats "50" as the baseline for RMSE and correlation comparisons.
# . Thresholds for "hot day" classification (THRESH_MAX, THRESH_MEAN) are
#   editable in the code.
#
# Dependencies
# ------------
# . R packages: data.table, arrow (and jj for j.date/lmonth if present)
# . Base R graphics only (no ggplot2), so it works in simple R terminals.
#
# Performance tips
# ----------------
# . arrow::open_dataset() lazily scans Parquet; collect() pulls the entire
#   daily table into memory once at the top. If the daily folder is very large,
#   consider pre-filtering by year/month/path partitioning before collect().
#
# Contact
# -------
# . If you modify the schema names (e.g., different suffixes than 98/80/50),
#   update `cols_needed` and the delta calculations accordingly.
################################################################################


library(data.table)
library(arrow)

daily_dt = collect(open_dataset("E:/data/gridded/era5land/blockgroup-output/gridded_output_parquet-202510/daily/"))


# # Ensure types
if (!inherits(daily_dt$localdate, "Date")) {
  # Accepts "YYYY-MM-DD" strings
  suppressWarnings(daily_dt[, localdate := j.date(localdate)])
}
if (!is.integer(daily_dt$year)) {
  suppressWarnings(daily_dt[, year := as.integer(year)])
}

# Keep one clean NA mask per stat to avoid silent all-NA groups later
cols_needed <- c(
  paste0("wbgt_max", c("98","80","50")),
  paste0("wbgt_min", c("98","80","50")),
  paste0("wbgt_mean",c("98","80","50"))
)
missing <- setdiff(cols_needed, names(daily_dt))
if (length(missing)) stop("Missing expected columns: ", paste(missing, collapse=", "))

# Helper: safe quantiles (handles all-NA)
qfun <- function(x, probs = c(0.05, 0.25, 0.5, 0.75, 0.95)) {
  if (all(is.na(x))) return(rep(NA_real_, length(probs)))
  as.numeric(quantile(x, probs = probs, na.rm = TRUE, type = 7))
}
rmse <- function(err) sqrt(mean(err^2, na.rm = TRUE))

# ------------------------------------------------------------------------------
# 1) Pairwise deltas (98-50, 80-50, 98-80) for max/min/mean
# ------------------------------------------------------------------------------
daily_dt[, `:=`(
  d_max_98_50  = wbgt_max98  - wbgt_max50,
  d_min_98_50  = wbgt_min98  - wbgt_min50,
  d_mean_98_50 = wbgt_mean98 - wbgt_mean50,
  
  d_max_80_50  = wbgt_max80  - wbgt_max50,
  d_min_80_50  = wbgt_min80  - wbgt_min50,
  d_mean_80_50 = wbgt_mean80 - wbgt_mean50,
  
  d_max_98_80  = wbgt_max98  - wbgt_max80,
  d_min_98_80  = wbgt_min98  - wbgt_min80,
  d_mean_98_80 = wbgt_mean98 - wbgt_mean80
)]

# ------------------------------------------------------------------------------
# 2) Global summaries (across all dates/places)
# ------------------------------------------------------------------------------
global_summary <- {
  # For each metric, produce mean/median/sd, RMSE vs 50, and correlation with 50
  f <- function(x_schema, x_base) {
    err <- x_schema - x_base
    c(
      mean   = mean(err, na.rm=TRUE),
      median = median(err, na.rm=TRUE),
      sd     = sd(err, na.rm=TRUE),
      rmse   = rmse(err),
      q05    = qfun(err, .05),
      q25    = qfun(err, .25),
      q50    = qfun(err, .50),
      q75    = qfun(err, .75),
      q95    = qfun(err, .95),
      cor    = suppressWarnings(cor(x_schema, x_base, use="complete.obs"))
    )
  }
  rbind(
    # max
    data.table(metric="max",  compare="98 vs 50", t(f(daily_dt$wbgt_max98,  daily_dt$wbgt_max50))),
    data.table(metric="max",  compare="80 vs 50", t(f(daily_dt$wbgt_max80,  daily_dt$wbgt_max50))),
    data.table(metric="max",  compare="98 vs 80", t(f(daily_dt$wbgt_max98,  daily_dt$wbgt_max80))),
    # min
    data.table(metric="min",  compare="98 vs 50", t(f(daily_dt$wbgt_min98,  daily_dt$wbgt_min50))),
    data.table(metric="min",  compare="80 vs 50", t(f(daily_dt$wbgt_min80,  daily_dt$wbgt_min50))),
    data.table(metric="min",  compare="98 vs 80", t(f(daily_dt$wbgt_min98,  daily_dt$wbgt_min80))),
    # mean
    data.table(metric="mean", compare="98 vs 50", t(f(daily_dt$wbgt_mean98, daily_dt$wbgt_mean50))),
    data.table(metric="mean", compare="80 vs 50", t(f(daily_dt$wbgt_mean80, daily_dt$wbgt_mean50))),
    data.table(metric="mean", compare="98 vs 80", t(f(daily_dt$wbgt_mean98, daily_dt$wbgt_mean80)))
  )
}
global_summary[]  # print

# ------------------------------------------------------------------------------
# 3) Monthly summaries (by month, statewide) of deltas
# ------------------------------------------------------------------------------
daily_dt[, month := jj::lmonth(localdate)]

monthly_delta_summary <- daily_dt[
  , .(
    n = .N,
    
    # Max deltas
    max_98_50_mean  = mean(d_max_98_50, na.rm=TRUE),
    max_98_50_rmse  = rmse(d_max_98_50),
    max_80_50_mean  = mean(d_max_80_50, na.rm=TRUE),
    max_80_50_rmse  = rmse(d_max_80_50),
    max_98_80_mean  = mean(d_max_98_80, na.rm=TRUE),
    max_98_80_rmse  = rmse(d_max_98_80),
    
    # Min deltas
    min_98_50_mean  = mean(d_min_98_50, na.rm=TRUE),
    min_98_50_rmse  = rmse(d_min_98_50),
    min_80_50_mean  = mean(d_min_80_50, na.rm=TRUE),
    min_80_50_rmse  = rmse(d_min_80_50),
    min_98_80_mean  = mean(d_min_98_80, na.rm=TRUE),
    min_98_80_rmse  = rmse(d_min_98_80),
    
    # Mean deltas
    mean_98_50_mean = mean(d_mean_98_50, na.rm=TRUE),
    mean_98_50_rmse = rmse(d_mean_98_50),
    mean_80_50_mean = mean(d_mean_80_50, na.rm=TRUE),
    mean_80_50_rmse = rmse(d_mean_80_50),
    mean_98_80_mean = mean(d_mean_98_80, na.rm=TRUE),
    mean_98_80_rmse = rmse(d_mean_98_80)
  ),
  by = .(year, month)
][order(year, month)]

monthly_delta_summary[]  # print

# ------------------------------------------------------------------------------
# 4) Spatial summaries (by GISJOIN): average deltas across time
#    Useful to rank locations most sensitive to smoothing choice
# ------------------------------------------------------------------------------
spatial_delta_summary <- daily_dt[
  , .(
    n_days = .N,
    max_98_50 = mean(d_max_98_50,  na.rm=TRUE),
    max_80_50 = mean(d_max_80_50,  na.rm=TRUE),
    max_98_80 = mean(d_max_98_80,  na.rm=TRUE),
    
    min_98_50 = mean(d_min_98_50,  na.rm=TRUE),
    min_80_50 = mean(d_min_80_50,  na.rm=TRUE),
    min_98_80 = mean(d_min_98_80,  na.rm=TRUE),
    
    mean_98_50 = mean(d_mean_98_50, na.rm=TRUE),
    mean_80_50 = mean(d_mean_80_50, na.rm=TRUE),
    mean_98_80 = mean(d_mean_98_80, na.rm=TRUE)
  ),
  by = GISJOIN
][order(-abs(mean_98_50))]

# Top/bottom locations by absolute change in daily mean
spatial_top_change <- head(spatial_delta_summary[order(-abs(mean_98_50))], 50)

# ------------------------------------------------------------------------------
# 5) Correlations between schemas, per metric, globally
# ------------------------------------------------------------------------------
cor_summary <- data.table(
  metric = c("max","max","max","min","min","min","mean","mean","mean"),
  pair   = c("98-50","80-50","98-80","98-50","80-50","98-80","98-50","80-50","98-80"),
  cor    = c(
    suppressWarnings(cor(daily_dt$wbgt_max98,  daily_dt$wbgt_max50,  use="complete.obs")),
    suppressWarnings(cor(daily_dt$wbgt_max80,  daily_dt$wbgt_max50,  use="complete.obs")),
    suppressWarnings(cor(daily_dt$wbgt_max98,  daily_dt$wbgt_max80,  use="complete.obs")),
    
    suppressWarnings(cor(daily_dt$wbgt_min98,  daily_dt$wbgt_min50,  use="complete.obs")),
    suppressWarnings(cor(daily_dt$wbgt_min80,  daily_dt$wbgt_min50,  use="complete.obs")),
    suppressWarnings(cor(daily_dt$wbgt_min98,  daily_dt$wbgt_min80,  use="complete.obs")),
    
    suppressWarnings(cor(daily_dt$wbgt_mean98, daily_dt$wbgt_mean50, use="complete.obs")),
    suppressWarnings(cor(daily_dt$wbgt_mean80, daily_dt$wbgt_mean50, use="complete.obs")),
    suppressWarnings(cor(daily_dt$wbgt_mean98, daily_dt$wbgt_mean80, use="complete.obs"))
  )
)

cor_summary[]  # print

# ------------------------------------------------------------------------------
# 6) Threshold exceedance agreement
#    Compare classification differences (e.g., hot day by daily max ??? T)
# ------------------------------------------------------------------------------
THRESH_MAX  <- c(28, 30, 32)  # °C WBGT max thresholds (edit as needed)
THRESH_MEAN <- c(24, 26, 28)  # °C WBGT daily mean thresholds (edit as needed)

exceedance_summary <- rbindlist(lapply(THRESH_MAX, function(Tmax) {
  # Flags for daily max
  daily_dt[, `:=`(
    flag_max_98 = (wbgt_max98  >= Tmax),
    flag_max_80 = (wbgt_max80  >= Tmax),
    flag_max_50 = (wbgt_max50  >= Tmax)
  )]
  
  data.table(
    metric = "max",
    threshold = Tmax,
    total_days = daily_dt[!is.na(flag_max_98) & !is.na(flag_max_50), .N],
    # Disagreement rates vs 50
    disagree_98_50 = daily_dt[flag_max_98 != flag_max_50, .N] / .N,
    disagree_80_50 = daily_dt[flag_max_80 != flag_max_50, .N] / .N,
    disagree_98_80 = daily_dt[flag_max_98 != flag_max_80, .N] / .N
  )
}), fill = TRUE)

exceedance_summary <- rbind(
  exceedance_summary,
  rbindlist(lapply(THRESH_MEAN, function(Tm) {
    daily_dt[, `:=`(
      flag_mean_98 = (wbgt_mean98 >= Tm),
      flag_mean_80 = (wbgt_mean80 >= Tm),
      flag_mean_50 = (wbgt_mean50 >= Tm)
    )]
    data.table(
      metric = "mean",
      threshold = Tm,
      total_days = daily_dt[!is.na(flag_mean_98) & !is.na(flag_mean_50), .N],
      disagree_98_50 = daily_dt[flag_mean_98 != flag_mean_50, .N] / .N,
      disagree_80_50 = daily_dt[flag_mean_80 != flag_mean_50, .N] / .N,
      disagree_98_80 = daily_dt[flag_mean_98 != flag_mean_80, .N] / .N
    )
  }), fill = TRUE)
)

exceedance_summary[]  # print

# (Optional) GISJOIN-level disagreement rates for a chosen threshold
chosen_Tmax <- 32
hotday_disagree_by_place <- daily_dt[
  , {
    f98 = wbgt_max98 >= chosen_Tmax
    f50 = wbgt_max50 >= chosen_Tmax
    .(rate_disagree_98_50 = mean(f98 != f50, na.rm = TRUE), n = .N)
  },
  by = GISJOIN
][order(-rate_disagree_98_50)]

# ------------------------------------------------------------------------------
# 7) Ranking consistency: which schema yields the largest daily max?
# ------------------------------------------------------------------------------
# For each (GISJOIN, localdate), find which schema gives the highest daily max.
daily_dt[
  , top_schema_max := {
    # vectorized pmax + tie-breaker (prefer larger weight ??? less smoothing label)
    m <- cbind(wbgt_max98, wbgt_max80, wbgt_max50)
    idx <- max.col(m, ties.method = "first")  # 1=98, 2=80, 3=50
    c("98","80","50")[idx]
  }
  , by = .(GISJOIN, month)
]

rank_summary <- daily_dt[
  , .(count = .N), by = top_schema_max
][, prop := count / sum(count) ][order(-count)]

# Also compute how often any pair differs by ??? delta (practical significance)
DELTA <- 2  # °C - adjust to your tolerance
practical_diff_max <- daily_dt[
  , .(
    frac_98_50_ge = mean(abs(wbgt_max98 - wbgt_max50) >= DELTA, na.rm = TRUE),
    frac_80_50_ge = mean(abs(wbgt_max80 - wbgt_max50) >= DELTA, na.rm = TRUE),
    frac_98_80_ge = mean(abs(wbgt_max98 - wbgt_max80) >= DELTA, na.rm = TRUE)
  )
]

# ------------------------------------------------------------------------------
# 8) Optional: save tables
# ------------------------------------------------------------------------------
# fwrite(global_summary,          "global_summary.csv")
# fwrite(monthly_delta_summary,   "monthly_delta_summary.csv")
# fwrite(spatial_delta_summary,   "spatial_delta_summary.csv")
# fwrite(cor_summary,             "cor_summary.csv")
# fwrite(exceedance_summary,      "exceedance_summary.csv")
# fwrite(rank_summary,            "rank_summary.csv")
# fwrite(hotday_disagree_by_place,"hotday_disagree_by_place.csv")

# Print a compact dashboard
cat("\n=== GLOBAL SUMMARY (deltas; RMSE, cor) ===\n")
print(global_summary)

cat("\n=== MONTHLY DELTA SUMMARY (means & RMSE) ===\n")
print(monthly_delta_summary)

cat("\n=== CORRELATIONS BETWEEN SCHEMAS ===\n")
print(cor_summary)

cat("\n=== THRESHOLD EXCEEDANCE DISAGREEMENT ===\n")
print(exceedance_summary)

cat("\n=== RANKING: SCHEMA PRODUCING HIGHEST DAILY MAX ===\n")
print(rank_summary)


#################################################################
#################################################################
#################################################################
# Safety check
req <- c("wbgt_max98","wbgt_max80","wbgt_max50")
miss <- setdiff(req, names(daily_dt))
if (length(miss)) stop("daily_dt is missing columns: ", paste(miss, collapse=", "))

# --- Compute practical difference fractions across many deltas ----------
deltas <- seq(0.1, 4.0, by = 0.1)

# Precompute absolute pairwise diffs (vectors)
diffs <- list(
  `98 vs 50` = abs(daily_dt$wbgt_max98 - daily_dt$wbgt_max50),
  `80 vs 50` = abs(daily_dt$wbgt_max80 - daily_dt$wbgt_max50),
  `98 vs 80` = abs(daily_dt$wbgt_max98 - daily_dt$wbgt_max80)
)

# For each pair, compute fraction(|??| >= delta) for every delta
frac_list <- lapply(names(diffs), function(pair_name) {
  v <- diffs[[pair_name]]
  # Handle all-NA edge case gracefully
  if (all(is.na(v))) {
    data.table(delta = deltas, pair = pair_name, fraction = NA_real_)
  } else {
    # Vectorized over deltas via sapply
    fr <- sapply(deltas, function(d) mean(v >= d, na.rm = TRUE))
    data.table(delta = deltas, pair = pair_name, fraction = as.numeric(fr))
  }
})

practical_diff_grid <- rbindlist(frac_list)
setorder(practical_diff_grid, pair, delta)

# Peek
cat("\nTidy practical-difference table (head):\n")
print(head(practical_diff_grid), row.names = FALSE)

# If you specifically want the ??=2.0 cut:
delta_target <- 2.0
at2 <- practical_diff_grid[abs(delta - delta_target) < 1e-9]
cat("\n=== PRACTICAL MAX DIFFERENCE (|??| ??? ", delta_target, " °C) ===\n", sep = "")
print(at2)

# --- Visualization 1: multi-line plot (base R) --------------------------
# Wide matrix by pair for matplot
wide <- dcast(practical_diff_grid, delta ~ pair, value.var = "fraction")
mat <- as.matrix(wide[, -1])

op <- par(no.readonly = TRUE)
par(mar = c(4.2, 4.2, 2.5, 0.5))
matplot(
  x = wide$delta, y = mat, type = "l", lty = 1, lwd = 2,
  xlab = expression(Delta~"(°C) threshold"),
  ylab = "Fraction of days with |??|max ??? threshold",
  main = "Practical-significance (daily max WBGT) across thresholds"
)
grid(col = "gray80"); box()
legend("topright", inset = 0.02, bty = "n", lty = 1, lwd = 2,
       legend = colnames(mat), col = seq_len(ncol(mat)))

# --- Visualization 2: barplot at ??=2 (base R) ---------------------------
par(mar = c(5, 4, 2.5, 0.5))
bp_vals <- setNames(at2$fraction, at2$pair)
barplot(
  height = bp_vals,
  ylim = c(0, 1),
  ylab = "Fraction (|??|max ??? 2°C)",
  main = expression("Practical-significance at " * Delta * " = 2 °C"),
  las = 1
)
abline(h = pretty(c(0,1)), col = "gray90", lwd = 1)

# restore par
par(op)
