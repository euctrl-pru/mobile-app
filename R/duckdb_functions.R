# File: R/read_selected_years_duck_tbl.R
suppressPackageStartupMessages({
  library(DBI)
  library(duckdb)
  library(dplyr)
  library(glue)
})

# Read ONLY the requested years from a Hive-partitioned layout:
# <base_dir>/YEAR=YYYY/data_*.parquet
read_selected_years_duck_tbl <- function(con = con,
                                         mydataframe,
                                         years = 2018:lubridate::year(lubridate::today()),
                                         subpattern = "YEAR=*/data_*.parquet") {
  # --- Validate inputs ---
  if (missing(years) || length(years) == 0L) stop("`years` must be a non-empty numeric vector, e.g. c(2024, 2025).")
  if (!is.numeric(years)) stop("`years` must be numeric (e.g., c(2024, 2025)).")
  years <- as.integer(years)
  
  base_dir <- here(archive_dir_raw, mydataframe)
  
  # --- Build glob & SQL ---
  glob <- normalizePath(file.path(base_dir, subpattern), winslash = "/", mustWork = FALSE)
  years_sql <- paste(years, collapse = ", ")
  sql_txt <- glue("
    SELECT
      CAST(YEAR AS INTEGER) AS YEAR,  -- optional: ensure integer
      * EXCLUDE (YEAR)
    FROM parquet_scan('{glob}', hive_partitioning=true)
    WHERE YEAR IN ({years_sql})
  ")
  
  # --- Lazy DuckDB table ---
  dplyr::tbl(con, dplyr::sql(sql_txt))
}

# tbl: lazy DuckDB table that includes a YEAR column (e.g., from hive_partitioning)
# df: local data.frame/tibble/duckplyr_df containing a YEAR column
save_partitions_from_local_df <- function(con,
                                          df,
                                          mydataframe,
                                          years,
                                          filename = "data_0.parquet",
                                          clean_partition_dir = TRUE) {
  print(paste(format(now(), "%H:%M:%S")))
  stopifnot(!is.null(con), inherits(con, "duckdb_connection"))


  # Normalize base path once
  base_dir <- here(archive_dir_raw, mydataframe)
  base_posix <- normalizePath(base_dir, winslash = "/", mustWork = FALSE)
  
  # Register local df as a DuckDB relation (zero-copy); use a unique name
  rel_name <- sprintf("__df_reg_%d__", as.integer(Sys.time()))
  duckdb::duckdb_register(con, rel_name, as.data.frame(df))
  on.exit(try(duckdb::duckdb_unregister(con, rel_name), silent = TRUE), add = TRUE)
  
  for (yr in years) {
    part_dir <- file.path(base_posix, paste0("YEAR=", yr))
    if (!dir.exists(part_dir)) dir.create(part_dir, recursive = TRUE)
    if (clean_partition_dir) {
      old <- list.files(part_dir, pattern = "\\.parquet$", full.names = TRUE)
      if (length(old)) unlink(old, force = TRUE)
    }
    out_path <- normalizePath(file.path(part_dir, filename), winslash = "/", mustWork = FALSE)
    
    sql_txt <- sprintf(
      "COPY (SELECT * FROM %s WHERE YEAR = %d)
       TO '%s' (FORMAT PARQUET, OVERWRITE_OR_IGNORE TRUE);",
      rel_name, yr, out_path
    )
    DBI::dbExecute(con, sql_txt)
  }
  
  invisible(TRUE)
  print(paste(format(now(), "%H:%M:%S")))
  
}

# --- Example ---
# con <- DBI::dbConnect(duckdb::duckdb())
# save_partitions_from_local_df(con,
#                               df = df_mod,                    # local duckplyr_df
#                               base_dir = here::here(archive_dir_raw),
#                               years = c(2024, 2025))          # or NULL to auto-detect
# DBI::dbDisconnect(con, shutdown = TRUE)

save_partitions_single_copy <- function(con, df, mydataframe, years, year_col = "YEAR") {
  message(paste(format(now(), "%H:%M:%S")))
  
  stopifnot(is.character(year_col), length(year_col) == 1)
  if (!(year_col %in% names(df))) {
    stop(sprintf("Column '%s' not found in `df`.", year_col))
  }
  
  # years <- as.integer(years)
  if (!length(years)) stop("`years` must be a non-empty numeric vector.")
  
  # ---- base output dir ----
  base_dir <- here::here(archive_dir_raw, mydataframe)
  fs::dir_create(base_dir, recurse = TRUE)  # ensure it exists
  base_posix <- normalizePath(base_dir, winslash = "/", mustWork = TRUE)
  
  # ---- remove target partitions first (ensures overwrite semantics) ----
  # DuckDB uses Hive-style partition dirs: <base>/<year_col>=<value>
  # Coerce years to character to match folder names consistently
  years_chr <- as.character(years)
  part_dirs <- file.path(base_posix, paste0(year_col, "=", years_chr))

  # Delete only the partitions we’re about to rewrite
  for (p in part_dirs) {
    if (fs::dir_exists(p)) fs::dir_delete(p)
  }  
  # Zero-copy registration (fast)
  rel <- sprintf("__df_reg_%d__", as.integer(Sys.time()))
  duckdb::duckdb_register(con, rel, as.data.frame(df))
  on.exit(try(duckdb::duckdb_unregister(con, rel), silent = TRUE), add = TRUE)

  
  years_sql <- paste(years, collapse = ", ")
  sql_txt <- glue::glue_sql("
    COPY (
      SELECT *
      FROM {`rel`}
      WHERE {`year_col`} IN ({years*})
    )
    TO {base_posix}
    (FORMAT PARQUET, PARTITION_BY ({`year_col`}), OVERWRITE_OR_IGNORE TRUE);
  ", .con = con)
  
  DBI::dbExecute(con, sql_txt)
  message(format(lubridate::now(), "%H:%M:%S"))
  invisible(TRUE)
}


read_partitioned_parquet_duckdb <- function(con,
                                            mydataframe,
                                            years = NULL,
                                            subpattern = NULL,
                                            collect = FALSE, 
                                            year_col = "YEAR"
                                            ) {
  stopifnot(inherits(con, "duckdb_connection"))
  stopifnot(is.character(year_col), length(year_col) == 1)
  
  # Build the glob (default pattern depends on the chosen partition column)
  if (is.null(subpattern)) {
    subpattern <- sprintf("%s=*/data_*.parquet", year_col)
  }
  base_dir <- here::here(archive_dir_raw, mydataframe)
  glob <- normalizePath(file.path(base_dir, subpattern), winslash = "/", mustWork = FALSE)
  

  # Base scan
  sql_base <- glue::glue_sql(
    "SELECT * FROM parquet_scan({glob}, hive_partitioning=true)",
    .con = con
  )
  
  # Optional year filter (cast because partition columns may be read as TEXT)
  if (!is.null(years) && length(years) > 0) {
    yrs <- as.integer(years)
    if (!length(yrs)) stop("`years` must be a non-empty numeric vector.")
    sql_base <- glue::glue_sql(
      "{sql_base} WHERE CAST({`year_col`} AS INTEGER) IN ({yrs*})",
      .con = con
    )
  }
  
  tbl <- dplyr::tbl(con, dplyr::sql(sql_base))
  if (isTRUE(collect)) dplyr::collect(tbl) else tbl
}

# --- Examples ---
# Local tibble, only 2024 & 2025:
# df <- read_partitioned_parquet_duckdb(con, base_dir = here::here(archive_dir_raw), years = c(2024, 2025), collect = TRUE)
# Lazy table, only 2025:
# t  <- read_partitioned_parquet_duckdb(con, base_dir = here::here(archive_dir_raw), years = 2025, collect = FALSE)


