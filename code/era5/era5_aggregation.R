#!/usr/bin/env Rscript
################################################################################
# ERA5 Climate Data Processing Script (CLI, multi-variable; custom lat/lon cols)
# Author: You
# Last Updated: 2025-10-17
#
# PURPOSE
#   - Build grid???geography weights (exactextractr) from a reference ERA5 grid
#   - Compute weighted hourly statistics for user-specified variables
#   - Aggregate to daily statistics using local dates per GISJOIN
#
# INPUT (processed ERA5 parquet layout)
#   <data-dir>/<year>/*.parquet
#   Must include:
#     - <lat-col>, <lon-col>  (scaled integer indices, e.g., ×10)
#     - validTime             (UNIX seconds or Arrow timestamp)
#     - user-specified columns via --vars (e.g., wbgt_scaled10_degC, ta_scaled10_degC)
#
# OUTPUT (under --out)
#   <out>/<geog-name>/gridded_output_parquet-<date-version>/hourly/<year>/*.parquet
#   <out>/<geog-name>/gridded_output_parquet-<date-version>/daily/<year>/*.parquet
#   <out>/<geog-name>/gridded_output_parquet-<date-version>/grid_to_geograhpy_dt-<geog>-<date>.Rds
################################################################################

suppressPackageStartupMessages({
  library(data.table)
  library(dplyr)
  library(sf)
  library(terra)
  library(arrow)
  library(exactextractr)
  library(lubridate)
  library(lutz)
  library(future)
  library(future.apply)
})

#---------------------------#
# Utilities / CLI parsing   #
#---------------------------#

die <- function(...) { cat(paste0(..., "\n")); quit(save="no", status = 1) }

parse_vars <- function(txt) {
  # txt example:
  # "name=wbgt,col=wbgt_scaled10_degC,scale=10,unit=degC;name=ta,col=ta_scaled10_degC,scale=10,unit=degC"
  if (is.null(txt) || !nzchar(txt)) die("Missing --vars. Use --help for format.")
  specs <- unlist(strsplit(txt, ";"))
  specs <- trimws(specs)
  out <- lapply(specs, function(s) {
    parts <- unlist(strsplit(s, ","))
    parts <- trimws(parts)
    kv <- strsplit(parts, "=", fixed = TRUE)
    kv <- lapply(kv, function(z) if (length(z) == 2) z else c(z[1], NA_character_))
    lst <- setNames(lapply(kv, `[[`, 2), sapply(kv, `[[`, 1))
    if (is.null(lst$name) || is.null(lst$col))
      die("Each --vars spec must include name=... and col=...")
    lst$scale  <- if (is.null(lst$scale)  || !nzchar(lst$scale))  "1" else lst$scale
    lst$offset <- if (is.null(lst$offset) || !nzchar(lst$offset)) "0" else lst$offset
    lst$unit   <- if (is.null(lst$unit)) "" else lst$unit
    lst
  })
  dt <- rbindlist(lapply(out, as.data.table), fill = TRUE)
  dt[, scale := as.numeric(scale)]
  dt[, offset := as.numeric(offset)]
  if (any(!is.finite(dt$scale)))  die("Non-numeric scale in --vars.")
  if (any(!is.finite(dt$offset))) die("Non-numeric offset in --vars.")
  if (any(duplicated(dt$name)))   die("Duplicate variable 'name' values in --vars.")
  dt[]
}

parse_cli <- function() {
  args <- commandArgs(trailingOnly = TRUE)
  if (length(args) == 0 || any(args %in% c("-h", "--help"))) {
    cat("
Usage:
  Rscript era5_census_cli.R \\
    --out=DIR \\
    --data-dir=DIR \\
    --geog-shp=/path/your_geographies.shp \\
    --geog-name=blockgroup \\
    --years=2025,2024 \\
    --months=1,2,3 \\
    --lat-col=lat_idx_x10 --lon-col=lon_idx_x10 [--lat-scale=10 --lon-scale=10] \\
    --vars=\"name=wbgt,col=wbgt_scaled10_degC,scale=10,unit=degC;name=ta,col=ta_scaled10_degC,scale=10,unit=degC\" \\
    [--state-fips=37] [--date-version=202510] \\
    [--workers=4] [--ref-month=5] \\
    [--build-grid] [--no-hourly] [--no-daily]

Required:
  --out           Root output directory
  --data-dir      Directory with processed ERA5 parquet (<data-dir>/<year>/*.parquet)
  --geog-shp      Path to shapefile; must have GISJOIN or GEOID*/STATEFP*
  --geog-name     Short tag for geography (e.g., blockgroup, county)
  --years         Comma list (e.g., 2025 or 2019,2020)
  --vars          Semicolon list of variable specs:
                  name=ALIAS,col=COLUMN[,scale=10][,offset=0][,unit=degC]
  --lat-col       Scaled latitude index column name (e.g., lat_idx_x10)
  --lon-col       Scaled longitude index column name (e.g., lon_idx_x10)

Optional:
  --months        Comma list in 1..12 (default: 1..12)
  --lat-scale     Divisor to convert --lat-col to degrees (default: 10)
  --lon-scale     Divisor to convert --lon-col to degrees (default: 10)
  --state-fips    2-digit FIPS filter for shapefile (e.g., 37)
  --date-version  Tag for outputs (default: YYYYMM of today)
  --workers       Parallel workers (default: 4)
  --ref-month     Month (1..12) used to build reference grid (default: 5)
  --build-grid    Force (re)build of grid weights
  --no-hourly     Skip hourly stage
  --no-daily      Skip daily stage
\n")
    quit(save="no")
  }
  
  get_opt <- function(key, default=NULL) {
    hit <- grep(paste0("^", key, "="), args, value = TRUE)
    if (length(hit)) sub(paste0("^", key, "="), "", hit[1]) else default
  }
  has_flag <- function(flag) any(args == flag)
  
  out_dir      <- normalizePath(get_opt("--out"), mustWork = FALSE)
  data_dir     <- normalizePath(get_opt("--data-dir"), mustWork = TRUE)
  geog_shp     <- normalizePath(get_opt("--geog-shp"), mustWork = TRUE)
  geog_name    <- get_opt("--geog-name")
  years_txt    <- get_opt("--years")
  months_txt   <- get_opt("--months")
  state_fips   <- get_opt("--state-fips")
  date_version <- get_opt("--date-version")
  workers      <- as.integer(get_opt("--workers", "4"))
  ref_month    <- as.integer(get_opt("--ref-month", "5"))
  vars_txt     <- get_opt("--vars")
  
  lat_col      <- get_opt("--lat-col")
  lon_col      <- get_opt("--lon-col")
  lat_scale    <- as.numeric(get_opt("--lat-scale", "10"))
  lon_scale    <- as.numeric(get_opt("--lon-scale", "10"))
  
  if (is.na(workers) || workers < 1) workers <- 4
  if (is.na(ref_month) || ref_month < 1 || ref_month > 12) ref_month <- 5
  if (is.na(lat_scale) || lat_scale <= 0) lat_scale <- 10
  if (is.na(lon_scale) || lon_scale <= 0) lon_scale <- 10
  
  if (is.null(out_dir) || out_dir == "" ||
      is.null(data_dir) || is.null(geog_shp) || is.null(geog_name) ||
      is.null(lat_col)  || is.null(lon_col))
    die("Missing required flags. Use --help.")
  
  years  <- if (!is.null(years_txt)) as.integer(unlist(strsplit(years_txt, ","))) else die("Provide --years.")
  months <- if (!is.null(months_txt)) as.integer(unlist(strsplit(months_txt, ","))) else 1:12
  if (is.null(date_version) || date_version == "") date_version <- format(Sys.Date(), "%Y%m")
  
  vars <- parse_vars(vars_txt)
  
  list(
    out_dir = out_dir,
    data_dir = data_dir,
    geog_shp = geog_shp,
    geog_name = geog_name,
    years = years,
    months = months,
    state_fips = state_fips,
    date_version = date_version,
    workers = workers,
    ref_month = ref_month,
    vars = vars,
    lat_col = lat_col,
    lon_col = lon_col,
    lat_scale = lat_scale,
    lon_scale = lon_scale,
    do_build_grid = has_flag("--build-grid"),
    do_hourly = !has_flag("--no-hourly"),
    do_daily  = !has_flag("--no-daily")
  )
}

ensure_dirs <- function(...) {
  lapply(list(...), function(p) dir.create(p, recursive = TRUE, showWarnings = FALSE))
}

#----------------------------------------------#
# Simple stats helpers                          #
#----------------------------------------------#

stat_weighted_mean <- function(v, w) {
  w[is.na(w)] <- 0
  s <- sum(w, na.rm = TRUE)
  if (s <= 0) mean(v, na.rm = TRUE) else sum(v * w, na.rm = TRUE) / s
}

#----------------------------------------------#
# Core: build grid ??? geography weights         #
#----------------------------------------------#

build_grid_weights <- function(cfg) {
  message("[grid] Reading geography: ", cfg$geog_shp)
  g <- st_read(cfg$geog_shp, quiet = TRUE) |> st_transform(4326)
  
  # Ensure GISJOIN
  if (!("GISJOIN" %in% names(g))) {
    geoid_col <- intersect(c("GEOID","GEOID20","GEOID10"), names(g))[1]
    if (is.na(geoid_col)) die("Shapefile lacks GISJOIN and GEOID* fields.")
    g$GISJOIN <- paste0("G", g[[geoid_col]])
  }
  
  if (!is.null(cfg$state_fips)) {
    sf_col <- intersect(c("STATEFP","STATEFP20","STATEFP10"), names(g))[1]
    if (is.na(sf_col)) die("STATEFP column not found to filter by --state-fips.")
    g <- g[g[[sf_col]] == cfg$state_fips, ]
    message("[grid] Filtered to STATEFP=", cfg$state_fips, " ??? ", nrow(g), " features")
  }
  
  # Build a light reference grid using the FIRST variable column (for value placeholder)
  ref_year  <- cfg$years[1]
  first_col <- cfg$vars$col[1]
  
  ds <- open_dataset(file.path(cfg$data_dir, ref_year), format = "parquet")
  ref <- ds %>%
    filter(month == cfg$ref_month) %>%
    select(all_of(c(cfg$lat_col, cfg$lon_col, first_col))) %>%
    distinct() %>%
    mutate(
      lat = !!as.name(cfg$lat_col) / cfg$lat_scale,
      lon = !!as.name(cfg$lon_col) / cfg$lon_scale,
      val = !!as.name(first_col)
    ) %>%
    select(lat, lon, val) %>%
    collect()
  setDT(ref)
  if (nrow(ref) == 0) die("[grid] No reference rows found; check --data-dir, --ref-month, column names, and scales.")
  
  ref[, lat := round(lat, 4)]
  ref[, lon := round(lon, 4)]
  
  r <- rast(ref[, .(lon, lat, val)], type = "xyz", crs = "EPSG:4326")
  r <- raster::raster(r)  # for exactextractr
  
  message("[grid] exact_extract.")
  ee <- exact_extract(r, g, include_xy = TRUE, include_cols = "GISJOIN")
  
  grid_dt <- rbindlist(lapply(seq_along(ee), function(i) {
    data.table(
      lon = ee[[i]]$x,
      lat = ee[[i]]$y,
      value = ee[[i]]$value,
      weight = ee[[i]]$coverage_fraction,
      GISJOIN = ee[[i]]$GISJOIN
    )
  }), fill = TRUE)
  grid_dt <- grid_dt[!is.na(lon)]
  
  # Timezone per point (cached in weights)
  pts_sf <- st_as_sf(grid_dt, coords = c("lon","lat"), crs = 4326, remove = FALSE)
  pts_sf$tz <- lutz::tz_lookup(pts_sf, method = "fast", warn = FALSE)
  grid_dt <- as.data.table(pts_sf); grid_dt$geometry <- NULL
  
  out_rds <- file.path(cfg$out_dir, cfg$geog_name,
                       sprintf("gridded_output_parquet-%s", cfg$date_version),
                       sprintf("grid_to_geograhpy_dt-%s-%s.Rds", cfg$geog_name, cfg$date_version))
  dir.create(dirname(out_rds), recursive = TRUE, showWarnings = FALSE)
  saveRDS(grid_dt, out_rds)
  message("[grid] Saved weights: ", out_rds)
  out_rds
}

#----------------------------------------------#
# Hourly stats (multi-variable)                 #
#----------------------------------------------#

compute_hourly_stats_multi <- function(dtm, var_defs) {
  dtm[, group := .GRP, by = .(validTime, GISJOIN)]
  stats_list <- lapply(seq_len(nrow(var_defs)), function(i) {
    alias <- var_defs$name[i]
    tmp <- dtm[, .(
      mean   = stat_weighted_mean(get(alias), weight),
      min    = suppressWarnings(min(get(alias), na.rm = TRUE)),
      max    = suppressWarnings(max(get(alias), na.rm = TRUE)),
      median = suppressWarnings(median(get(alias), na.rm = TRUE))
    ), by = group]
    setnames(tmp, c("group","mean","min","max","median"),
             c("group",
               paste0(alias,"_mean"),
               paste0(alias,"_min"),
               paste0(alias,"_max"),
               paste0(alias,"_median")))
    tmp
  })
  res <- stats_list[[1]]
  if (length(stats_list) > 1) for (k in 2:length(stats_list)) res <- merge(res, stats_list[[k]], by = "group", all = TRUE)
  res[]
}

process_hourly <- function(cfg, weights_rds) {
  message("[hourly] Using weights: ", weights_rds)
  w <- readRDS(weights_rds)
  setDT(w)
  setkey(w, lat, lon)
  
  out_hourly_root <- file.path(cfg$out_dir, cfg$geog_name,
                               sprintf("gridded_output_parquet-%s", cfg$date_version),
                               "hourly")
  ensure_dirs(out_hourly_root)
  
  plan(multisession, workers = cfg$workers)
  on.exit(plan(sequential), add = TRUE)
  
  invisible(lapply(cfg$years, function(yr) {
    message("[hourly] Year ", yr)
    ds_year <- open_dataset(file.path(cfg$data_dir, yr), format="parquet")
    
    lapply(cfg$months, function(mm) {
      message(sprintf("  . Month %02d", mm))
      # define the window (UTC) then convert to integer seconds
      month_start <- as.POSIXct(sprintf("%d-%02d-01 00:00:00", yr, mm), tz = "UTC")
      next_month  <- ifelse(mm == 12, 1, mm + 1)
      next_year   <- yr + as.integer(mm == 12)
      month_end   <- as.POSIXct(sprintf("%d-%02d-01 00:00:00", next_year, next_month), tz = "UTC")
      
      ms <- as.integer(month_start)  # seconds since epoch
      me <- as.integer(month_end)
      
      needed_cols <- unique(c(cfg$lat_col, cfg$lon_col, "validTime", cfg$vars$col))
      
      dat <- ds_year %>%
        select(all_of(needed_cols)) %>%
        filter(
          validTime >= !!arrow::scalar(ms),
          validTime <  !!arrow::scalar(me)
        ) %>%
        collect()
      
      dat <- as.data.table(dat)
      if (!nrow(dat)) { message("    (no rows)"); return(invisible(NULL)) }
      
      # Convert indices ??? lat/lon using provided scales
      dat[, `:=`(
        lat = get(cfg$lat_col) / cfg$lat_scale,
        lon = get(cfg$lon_col) / cfg$lon_scale
      )]
      dat[, `:=`(lat = round(lat, 4), lon = round(lon, 4))]
      setkey(dat, lat, lon)
      
      # Apply per-variable scale/offset and rename to alias
      for (i in seq_len(nrow(cfg$vars))) {
        alias <- cfg$vars$name[i]; col <- cfg$vars$col[i]
        sc    <- cfg$vars$scale[i]; off <- cfg$vars$offset[i]
        if (!(col %in% names(dat))) die(sprintf("Column '%s' not found in parquet for %d-%02d.", col, yr, mm))
        dat[, (alias) := get(col)/sc + off]
      }
      
      dat <- dat[, c("lat","lon","validTime", cfg$vars$name), with = FALSE]
      
      # Merge weights (Cartesian allowed)
      dtm <- merge(dat, w, by = c("lat","lon"), all.y = TRUE, allow.cartesian = TRUE)
      rm(dat); gc()
      
      # Fill single-hour gaps by neighbor average per grid, for each variable
      setorder(dtm, lat, lon, validTime)
      for (alias in cfg$vars$name) {
        dtm[, (alias) := {
          x <- get(alias)
          na_idx <- which(is.na(x))
          if (length(na_idx)) {
            lag1 <- data.table::shift(x, 1); lead1 <- data.table::shift(x, -1)
            fill <- na_idx[!is.na(lag1[na_idx]) & !is.na(lead1[na_idx])]
            x[fill] <- (lag1[fill] + lead1[fill]) / 2
          }
          x
        }, by = .(lat, lon)]
      }
      
      # Stats per (validTime, GISJOIN)
      res_stats <- compute_hourly_stats_multi(dtm, cfg$vars)
      res <- merge(res_stats, dtm[, .(group, validTime, GISJOIN, tz)], by = "group", all.x = TRUE)
      res[, group := NULL]
      res <- unique(res)
      rm(dtm, res_stats); gc()
      
      # Add local time fields by tz
      res[, validTime := as.POSIXct(validTime, origin="1970-01-01", tz="UTC")]
      tz_lookup <- unique(res[, .(validTime, tz)])
      tz_lookup[, c("localhour","localdate") := {
        lt <- with_tz(validTime, tzone = tz[1])
        .(hour(lt), as.character(as_date(lt)))
      }, by = tz]
      res <- res[tz_lookup, on = .(validTime, tz), nomatch = 0]
      
      # Output
      out_y <- file.path(out_hourly_root, yr)
      ensure_dirs(out_y)
      out_file <- file.path(out_y, sprintf("hourly_%d_%02d_%s.parquet", yr, mm, cfg$geog_name))
      write_parquet(res, out_file)
      message("    ??? ", out_file)
      invisible(NULL)
    })
  }))
}

# ---- Time window filter helper for Arrow datasets ----
# Ensures both sides of the comparison are the SAME type.
# Supports: timestamp[us/ms/s], int64 seconds, int32/64 seconds.
arrow_time_window <- function(ds, time_col = "validTime",
                              start_posixct, end_posixct) {
  stopifnot(inherits(start_posixct, "POSIXct"), inherits(end_posixct, "POSIXct"))
  
  sc <- schema(ds)
  tfield <- sc[[time_col]]
  if (is.null(tfield)) stop("Column '", time_col, "' not found.")
  
  # Normalize literals
  start_ts <- start_posixct
  end_ts   <- end_posixct
  
  # Decide based on Arrow type of the time column
  ttype <- tfield$type$ToString()
  
  # Helper to build the filtered query
  do_filter <- function(.ds, colname, lo, hi) {
    .ds %>%
      dplyr::filter(
        !!rlang::sym(colname) >= !!arrow::scalar(lo),
        !!rlang::sym(colname) <  !!arrow::scalar(hi)
      )
  }
  
  if (grepl("^timestamp\\[us", ttype)) {
    # Column already timestamp[us]; compare to POSIXct directly
    ds %>% do_filter(time_col, start_ts, end_ts)
    
  } else if (grepl("^timestamp\\[ms", ttype)) {
    # Cast to timestamp[s] or compare to POSIXct (Arrow will upcast)
    ds %>%
      dplyr::mutate(!!time_col := arrow::cast(!!rlang::sym(time_col),
                                              arrow::timestamp("us", tz = "UTC"))) %>%
      do_filter(time_col, start_ts, end_ts)
    
  } else if (grepl("^timestamp\\[s", ttype)) {
    ds %>% do_filter(time_col, start_ts, end_ts)
    
  } else if (grepl("^int(32|64)$", ttype)) {
    # Epoch seconds in integer; compare as integers (int64 safest)
    ms <- as.integer(as.numeric(start_ts))  # seconds
    me <- as.integer(as.numeric(end_ts))
    # force int64 scalars so Arrow doesn't try int32
    ds %>%
      dplyr::mutate(!!time_col := arrow::cast(!!rlang::sym(time_col), arrow::int64())) %>%
      dplyr::filter(
        !!rlang::sym(time_col) >= !!arrow::scalar(bit64::as.integer64(ms)),
        !!rlang::sym(time_col) <  !!arrow::scalar(bit64::as.integer64(me))
      )
  } else {
    stop("Unsupported time column type for filtering: ", ttype)
  }
}




#----------------------------------------------#
# Daily stats (multi-variable)                  #
#----------------------------------------------#

dailystats_by_month <- function(yr, mm, parquet_root_hourly, out_root_daily, geog_name, var_defs) {
  out_y <- file.path(out_root_daily, yr)
  ensure_dirs(out_y)
  out_file <- file.path(out_y, sprintf("%d_%02d-%s-dailystats.parquet", yr, mm, geog_name))
  
  ds_hourly <- open_dataset(parquet_root_hourly, format = "parquet")
  
  month_start <- ymd_hms(sprintf("%04d-%02d-01 00:00:00", yr, mm), tz="UTC")
  next_month  <- if (mm == 12) 1 else (mm + 1)
  next_year   <- if (mm == 12) (yr + 1) else yr
  month_end   <- ymd_hms(sprintf("%04d-%02d-01 00:00:00", next_year, next_month), tz="UTC")
  
  mean_cols <- paste0(var_defs$name, "_mean")
  needed <- unique(c("GISJOIN","tz","validTime","localdate", mean_cols))
  
  dt <- ds_hourly %>%
    dplyr::select(dplyr::all_of(needed)) %>%
    arrow_time_window(
      start_posixct = month_start,
      end_posixct   = month_end
    ) %>%
    collect()

  if (!nrow(dt)) { message("  [daily] no rows for ", yr, "-", mm); return(invisible(NULL)) }
  
  setDT(dt)
  by_keys <- c("localdate","GISJOIN")
  out_dt <- unique(dt[, ..by_keys])
  
  for (alias in var_defs$name) {
    colm <- paste0(alias, "_mean")
    if (!(colm %in% names(dt))) next
    agg <- dt[, .(
      val_max = round(max(get(colm), na.rm = TRUE), 1),
      val_min = round(min(get(colm), na.rm = TRUE), 1),
      val_mean= round(mean(get(colm), na.rm = TRUE), 1)
    ), by = by_keys]
    setnames(agg, c(by_keys, paste0(alias, c("_max","_min","_mean"))))
    out_dt <- merge(out_dt, agg, by = by_keys, all.x = TRUE)
  }
  
  write_parquet(out_dt, out_file)
  message("  [daily] ??? ", out_file)
  invisible(NULL)
}

process_daily <- function(cfg) {
  parquet_root_hourly <- file.path(cfg$out_dir, cfg$geog_name,
                                   sprintf("gridded_output_parquet-%s", cfg$date_version),
                                   "hourly")
  out_root_daily <- file.path(cfg$out_dir, cfg$geog_name,
                              sprintf("gridded_output_parquet-%s", cfg$date_version),
                              "daily")
  ensure_dirs(out_root_daily)
  
  lapply(cfg$years, function(yr) {
    message("[daily] Year ", yr)
    lapply(cfg$months, function(mm) dailystats_by_month(yr, mm, parquet_root_hourly, out_root_daily, cfg$geog_name, cfg$vars))
  })
  invisible(NULL)
}

#---------------------------#
# Main                      #
#---------------------------#

main <- function() {
  cfg <- parse_cli()
  ensure_dirs(cfg$out_dir)
  
  weights_rds <- file.path(cfg$out_dir, cfg$geog_name,
                           sprintf("gridded_output_parquet-%s", cfg$date_version),
                           sprintf("grid_to_geograhpy_dt-%s-%s.Rds", cfg$geog_name, cfg$date_version))
  
  if (cfg$do_build_grid || !file.exists(weights_rds)) {
    weights_rds <- build_grid_weights(cfg)
  } else {
    message("[grid] Using existing weights: ", weights_rds)
  }
  
  if (cfg$do_hourly) process_hourly(cfg, weights_rds)
  if (cfg$do_daily)  process_daily(cfg)
  
  message("[done]")
}

if (sys.nframe() == 0) main()
