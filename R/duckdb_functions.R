# file: R/duckplyr_owned_io.R

# Minimal, deterministic control over DuckDB/duckplyr lifecycle so temp files
# can be cleaned after you're done manipulating lazy relations.

suppressPackageStartupMessages({
  requireNamespace("DBI")
  requireNamespace("duckdb")
})

# Open a user-owned DuckDB connection and set a private spill directory.
# Why: you control when file handles are released.
# - spill_dir: directory for DuckDB temp files
# - dbdir: DuckDB database location (":memory:" by default)
# Returns: DBI connection with attribute 'spill_dir'
duck_open <- function(spill_dir = file.path(tempdir(), "duckplyr_spill"),
                      dbdir = ":memory:") {
  if (!dir.exists(spill_dir)) dir.create(spill_dir, recursive = TRUE, showWarnings = FALSE)
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = dbdir)
  DBI::dbExecute(con, sprintf(
    "SET temp_directory='%s'",
    normalizePath(spill_dir, winslash = "/", mustWork = FALSE)
  ))
  attr(con, "spill_dir") <- spill_dir
  con
}

# Ingest a Parquet file via DuckDB using the provided connection.
# - lazy = FALSE (default) materializes eagerly (prudence = "lavish") to mimic
#   read_parquet_duckdb() convenience and avoid translation issues with %in%.
# - lazy = TRUE returns a duckplyr lazy relation so you can dplyr-manipulate
#   and collect later.
# Returns: duckplyr relation (lazy) or a tibble (eager)
duck_ingest_parquet <- function(con, parquet_path, lazy = FALSE) {
  if (!inherits(con, "duckdb_connection")) stop("'con' must be a DuckDB connection from duck_open().")
  if (!requireNamespace("duckplyr", quietly = TRUE)) stop("Package 'duckplyr' is required.")
  stopifnot(length(parquet_path) == 1, nzchar(parquet_path))
  p <- normalizePath(parquet_path, winslash = "/", mustWork = TRUE)
  sql <- sprintf("SELECT * FROM read_parquet('%s')", p)
  prudence <- if (isTRUE(lazy)) "stingy" else "lavish"
  duckplyr::read_sql_duckdb(sql, prudence = prudence, con = con)
}

# Close the connection and optionally clean spill files.
# - clean: if TRUE, deletes common DuckDB spill file types in spill_dir
# Returns: TRUE invisibly on success
duck_close <- function(con, clean = TRUE, recursive = TRUE) {
  if (!inherits(con, "duckdb_connection")) return(invisible(TRUE))
  spill_dir <- attr(con, "spill_dir", exact = TRUE)
  # Disconnect first to release file handles
  try(DBI::dbDisconnect(con, shutdown = TRUE), silent = TRUE)
  gc()
  if (isTRUE(clean) && !is.null(spill_dir)) {
    duck_clean_spills(spill_dir = spill_dir, recursive = recursive)
  }
  invisible(TRUE)
}

# Delete spill files under a directory.
# Matches .tmp/.wal/.duckdb; adjust pattern if needed.
duck_clean_spills <- function(spill_dir = file.path(tempdir(), "duckplyr_spill"), recursive = TRUE) {
  if (!dir.exists(spill_dir)) return(invisible(character()))
  paths <- list.files(
    spill_dir,
    pattern = "\\.(tmp|wal|duckdb)$",
    full.names = TRUE,
    recursive = recursive
  )
  if (length(paths)) unlink(paths, force = TRUE, recursive = FALSE)
  invisible(paths)
}

# Inspect active DuckDB temporary files (when connection is open).
# Returns empty tibble if the table function isn't available.
duck_list_active_tempfiles <- function(con) {
  if (!requireNamespace("duckplyr", quietly = TRUE)) return(tibble::tibble())
  out <- try(duckplyr::read_sql_duckdb("FROM duckdb_temporary_files()", con = con), silent = TRUE)
  if (inherits(out, "try-error")) tibble::tibble() else out
}

# ----------------------
# Example usage pattern:
# ----------------------
# library(here)
# con <- duck_open(spill_dir = file.path(tempdir(), "duckplyr_spill"))
# df_base <- duck_ingest_parquet(con, here::here(archive_dir_raw, myparquetfile), lazy = TRUE)
#
# -- Option A: inline small vectors and then compute to keep working lazily
# df_alldays <- df_base |>%
#   dplyr::filter(AO_ID %in% dbplyr::local(list_ao$AO_ID)) |>%
#   compute(prudence = "lavish")
#
# -- Option B: large vector -> copy to DuckDB and semi_join (most robust)
# ao_ids_tbl <- duck_copy_vec(con, unique(list_ao$AO_ID), name = "ao_ids", col = "AO_ID")
# df_alldays <- df_base |>% dplyr::semi_join(ao_ids_tbl, by = "AO_ID")
#
# df_result <- df_alldays |>%
#   dplyr::mutate(x = some_col * 2) |>%
#   dplyr::collect()
# duck_close(con, clean = TRUE)

# ----------------------
# Utilities for large vector joins
# ----------------------
# Copy an R vector into DuckDB as a temporary table for robust joins.
# Why: avoids inlining huge IN lists and odd translation issues.
duck_copy_vec <- function(con, x, name = "vec_tmp", col = "value") {
  if (!inherits(con, "duckdb_connection")) stop("'con' must be a DuckDB connection from duck_open().")
  df <- tibble::tibble(!!rlang::sym(col) := x)
  dplyr::copy_to(con, df, name = name, temporary = TRUE, overwrite = TRUE)
}

# Convenience: filter 'tbl' where key column is in vector 'x' by creating a temp table + semi_join.
# Returns a lazy relation.
duck_filter_in <- function(tbl, con, key = "AO_ID", x, name = "vec_tmp") {
  vt <- duck_copy_vec(con, x, name = name, col = key)
  dplyr::semi_join(tbl, vt, by = key)
}

