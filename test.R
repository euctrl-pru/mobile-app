## ---------------------------
##
## Script name: R-Studio Setup Automation
##
## Purpose of script: Automating the setup of R-Studio for the Aviation Intelligence Unit.
##
## Date Created: 20 February 2023
##
## Author(s): Quinten Goens (EUROCONTROL, ESGD/AIU/OPS)
##
## Email(s): Quinten.Goens@eurocontrol.int
##
## ---------------------------
##
## Notes: This setup procedure is based on the work of E. Spinielli. See here:
## https://github.com/euctrl-pru/portal/wiki/Tools-Installation-and-Setup-%28For-R%29
##
##
## ---------------------------
options(warn = -1) # Disable warnings

#' Check if a version is up-to-date.
#'
#' Compares the individual components of the given version and reference version to determine
#' if the given version is up-to-date.
#'
#' @param VERSION A character string representing the version to check.
#' @param REF_VERSION A character string representing the reference version to compare against.
#' @return A logical value indicating whether the given version is up-to-date (TRUE) or not (FALSE).
#'
#' @examples
#' versionIsUptodate("1.2.3", "1.2.2")
#' # Returns TRUE
#' versionIsUptodate("1.2.3", "1.2.4")
#' # Returns FALSE
#'
versionIsUptodate <- function(VERSION, REF_VERSION) {
  # Split the versions into individual components
  VERSION_COMPONENTS <- as.numeric(strsplit(VERSION, "\\.")[[1]])
  REF_VERSION_COMPONENTS <- as.numeric(strsplit(REF_VERSION, "\\.")[[1]])

  # Compare the components of each VERSION
  for (i in seq_along(VERSION_COMPONENTS)) {
    if (VERSION_COMPONENTS[i] > REF_VERSION_COMPONENTS[i]) {
      return(TRUE)
    } else if (VERSION_COMPONENTS[i] < REF_VERSION_COMPONENTS[i]) {
      return(FALSE)
    }
  }
  # If all components are equal, the versions are the same
  return(TRUE)
}

#' Check required installations
#'
#' Checks if the required installations for the program are available and up-to-date, namely R, RStudio,
#' Git, and Rtools. If any of the required installations are not available or not up-to-date, the function
#' will display an error message and abort the configuration process.
#'
#' @return A logical value indicating whether all required installations are available and up-to-date (TRUE)
#' or not (FALSE).
#'
#' @examples
#' checkInstallations()
#'
checkInstallations <- function() {
  # Reference version
  REF_V_RSTUDIO <- "2022.12.0.353"
  REF_V_GIT <- "2.37.1.1" # Remove windows tag
  REF_V_R <- "4.2.1"
  REF_V_RTOOLS <- "42"

  message("\n\n[Checking required installations...]\n")

  # Checking Rtools
  RTOOLS_PATH <- Sys.which("make")
  RTOOLS_AVAILABLE <- as.logical(RTOOLS_PATH != "")
  if (RTOOLS_AVAILABLE) {
    RTOOLS_VERSION <-
      as.integer(sub(".*Rtools(\\d+).*", "\\1", RTOOLS_PATH))
    RTOOLS_UPTODATE <-
      (RTOOLS_VERSION < 100 & RTOOLS_VERSION >= as.integer(REF_V_RTOOLS))
  } else{
    RTOOLS_UPTODATE <- FALSE
  }

  # Checking R
  R_VERSION <-
    paste0(sessionInfo()[1]$R.version$major,
           ".",
           sessionInfo()[1]$R.version$minor)
  R_AVAILABLE <- TRUE # Otherwise you wouldn't be able to run this
  R_UPTODATE <- versionIsUptodate(R_VERSION, REF_V_R)

  # Checking Git
  GIT_DEFAULT_DIR <- "C:/Program Files/Git/bin/"
  GIT_AVAILABLE <- file.exists(paste0(GIT_DEFAULT_DIR, "git.exe"))
  if (GIT_AVAILABLE) {
    GIT_CMD <- c("git", "--version")
    GIT_VERSION_OUTPUT <- system2(GIT_CMD, stdout = TRUE)
    GIT_VERSION_OUTPUT <-
      gsub("windows.", "", gsub("git version ", "", GIT_VERSION_OUTPUT))
    GIT_UPTODATE <- versionIsUptodate(GIT_VERSION_OUTPUT, REF_V_GIT)
  } else{
    GIT_UPTODATE <- FALSE
  }

  # Get the RStudio version
  RSTUDIO_VERSION <- as.character(rstudioapi::versionInfo()$version)
  RSTUDIO_AVAILABLE <-
    TRUE # Otherwise you wouldn't be able to run this
  RSTUDIO_UPTODATE <-
    versionIsUptodate(RSTUDIO_VERSION, REF_V_RSTUDIO)

  if (RSTUDIO_UPTODATE &
      GIT_UPTODATE & RTOOLS_UPTODATE & R_UPTODATE) {
    message("\n\n[All software installed and up to date! Thank you.]\n")
    return(TRUE)
  } else{
    CANCELLED_MESSAGE <-
      "You do not have the (up-to-date) programs installed. Please go to https://eurocontrol.service-now.com/ and make an IT request to install the following software:"

    if (!RSTUDIO_UPTODATE | !RSTUDIO_AVAILABLE) {
      CANCELLED_MESSAGE <-
        paste0(CANCELLED_MESSAGE,
               '\n',
               "- Rstudio 2022.12.0.353 or newer if available.")
    }
    if (!GIT_UPTODATE | !GIT_AVAILABLE) {
      CANCELLED_MESSAGE <-
        paste0(CANCELLED_MESSAGE,
               '\n',
               "- Git (x64) 2.39.1 or newer if available.")
    }
    if (!RTOOLS_UPTODATE | !RTOOLS_AVAILABLE) {
      CANCELLED_MESSAGE <-
        paste0(CANCELLED_MESSAGE,
               '\n',
               "- Rtools (x64) 4.2 or newer if available.")
    }
    if (!R_UPTODATE | !R_AVAILABLE) {
      CANCELLED_MESSAGE <-
        paste0(CANCELLED_MESSAGE,
               '\n',
               "- R for Windows (x64) 4.2.1 or newer if available.")
    }
    message(
      "\n\n[All software is NOT installed or NOT up to date! Please see error message. Aborting configuration]\n"
    )

    CANCELLED_MESSAGE <- paste0(CANCELLED_MESSAGE,
                                '\n\n',
                                'Retry the configuration once all programs are up-to-date or installed. Aborting configuration now.',
                                '\n\n',
                                'For more information, contact Quinten.Goens@eurocontrol.int (ESGD/AIU/OPS).'
    )
    .rs.showErrorMessage(title = "Aborting configuration: Not all programs are installed.", message = CANCELLED_MESSAGE)
    return(FALSE)
  }
  #- Optional : Oracle Instant Client Basic and SDK (x64) 12.2.0.1.0 or newer if available.
}

#' setProxyPassword - Configures the proxy settings with the user's EUROCONTROL
#' password.
#'
#' This function sets the proxy string as an environment variable for HTTP and
#' HTTPS using the EUROCONTROL proxy server.
#'
#' It prompts the user to enter their password twice and checks for consistency
#' before encoding and setting it in the proxy URL.The proxy configuration is
#' persisted for all future sessions.
#'
#' @return None.
#' @raise None.
#' @examples
#' setProxyPassword()
setProxyPassword <- function() {
  message("\n\n[Welcome to AIU R-autoconfigure for proxy settings...]\n")
  message("\n\n[You will be prompted to enter your password twice...]\n")

  pwd1 <- "Placeholder 1"
  pwd2 <- "Placeholder 2"

  pwd_request <-
    "Please enter your password to enable or update the proxy configuration:"
  pwd_request_full <- pwd_request
  pwd_confirmation <-
    "Please confirm your password by typing it again:"

  while (pwd1 != pwd2) {
    pwd1 <- .rs.askForPassword(pwd_request_full)
    pwd2 <- .rs.askForPassword(pwd_confirmation)

    if (pwd1 != pwd2) {
      pwd_request_full <-
        paste("The passwords you entered do not match, please try again.",
              pwd_request)
    }
  }

  pwd_pc_encoded <- URLencode(pwd1, reserved = TRUE)
  user <- Sys.info()[["user"]]
  proxy_url <-
    paste0("http://",
           user,
           ":",
           pwd_pc_encoded,
           "@proxy.eurocontrol.int:9512")

  message("\n\n[Setting the proxy string as an environment variable for HTTP and HTTPS...]\n")

  # Persisting for all future sessions
  system(sprintf("setx HTTP_PROXY %s", proxy_url))
  system(sprintf("setx HTTPS_PROXY %s", proxy_url))

  # Enabling just for this session so that I can install packages through the proxy
  Sys.setenv(HTTP_PROXY = proxy_url, envir = new.env())
  Sys.setenv(HTTPS_PROXY = proxy_url, envir = new.env())

  message("\n\n[Proxy configuration complete! You can now install packages using CRAN.]\n")
}

setProxyPassword()


#' setEnvironment - Configures the R programming environment with various
#' settings and packages.
#'
#' This function sets various environment variables and installs several packages
#' that are commonly used in the R programming language.It sets the R_LIBS_USER
#' environment variable and creates a corresponding folder, adds the R bin to the
#' PATH environment variable, unsets the BINPREF environment variable, and sets
#' the OCI_LIB64 environment variable.
#'
#' It installs the TinyTex package and adds it to the PATH environment variable,
#' as well as installs several standard packages, including tidyverse, rvest, and
#' here.
#'
#' It also installs several custom packages, including pruatlas and trrrj, which
#' are developed by the AIU team.
#' Finally, it provides messages to the user throughout the process to indicate
#' the progress of the configuration.
#'
#' @return None.
#' @raise None.
#' @examples
#' setEnvironment()
setEnvironment <- function(quiet = FALSE)
{
  message("\n\n[Welcome to AIU R-autoconfigure for the programming environment]\n")

  # Get a list of packages that are loaded at startup
  startup_pkgs <- c(".GlobalEnv",
                    "tools:rstudio",
                    "package:stats",
                    "package:graphics",
                    "package:grDevices",
                    "package:utils",
                    "package:datasets",
                    "package:methods",
                    "Autoloads",
                    "package:base")

  # Get a list of all attached packages
  attached_pkgs <- search()

  # Remove packages that are not loaded at startup
  detach_pkgs <- attached_pkgs[!attached_pkgs %in% startup_pkgs]

  # Detach all packages that are not loaded at startup
  sapply(detach_pkgs, detach, character.only = TRUE, unload = TRUE)

  # Remove environment variables
  #rm(list=ls()) # messes up rstudio version somehow / need to find better alternative.

  # Requirements (Installing in the wrong location, I'll clean this up later)

  # Check if rstudioapi package is installed
  if (!require("rstudioapi")) {
    install.packages("rstudioapi")
    library("rstudioapi")
  } else {
    library("rstudioapi")
  }

  if (!require("httr")) {
    install.packages("httr")
    library("httr")
  } else {
    library("httr")
  }


  # Checking installation status
  if (!checkInstallations()){
    return(NULL)
  }

  # Set R_LIBS_USER and create folder
  message("\n\n[Setting R_LIBS_USER environment variable...]\n")
  USER = Sys.info()[["user"]]
  R_VERSION_MAJOR <- sessionInfo()[1]$R.version$major
  R_VERSION_MINOR <- sessionInfo()[1]$R.version$minor
  R_LIBS_USER = sprintf(
    "C:\\Users\\%s\\dev\\R\\win-library\\R-%s.%s",
    USER,
    R_VERSION_MAJOR,
    R_VERSION_MINOR
  )
  R_LIBS_USER_PATH = file.path(R_LIBS_USER)
  if (!dir.exists(R_LIBS_USER_PATH)) {
    message(sprintf("\n\n[Creating the following directory: %s]\n", R_LIBS_USER_PATH))
    dir.create(R_LIBS_USER_PATH, recursive = TRUE)
  }

  # Persisting for all future sessions
  system(sprintf("setx R_LIBS_USER %s", R_LIBS_USER))

  # Add the R bin to path (for using R in git bash)
  message("\n\n[Adding R_BIN environment variable to Path...]\n")
  R_BIN <-
    sprintf("C:\\Program Files\\R\\R-%s.%s\\bin",
            R_VERSION_MAJOR,
            R_VERSION_MINOR)

  # Persisting for all future sessions
  system(sprintf('setx PATH "%%PATH%%;%s"', R_BIN))

  # Unset BINPREF
  message("\n\n[Unsetting BINPREF environment variable to Path...]\n")
  system('setx BINPREF ""')
  Sys.unsetenv("BINPREF")

  # Set OCI_LIB64
  message("\n\n[Adding OCI_LIB64 environment variable to Path...]\n")
  system("setx OCI_LIB64 C:\\PROGRA~1\\INSTAN~1")

  # Add Rtools to path
  message("\n\n[Adding Rtools environment variable to Path...]\n")
  RTOOLS_PATH <- Sys.which("make")
  if (RTOOLS_PATH == "") {
    RTOOLS_AVAILABLE <- FALSE
    message <-
      "Rtools has not been detected. Please go to https://eurocontrol.service-now.com/ and make an IT request to install the latest Rtools.\n\nThe installation will continue without configuring Rtools. Rerun once installed to configure Rtools."
    .rs.showErrorMessage(title = "Warning: Rtools is not installed.", message =
                           message)
  } else {
    RTOOLS_AVAILABLE <- TRUE
    RTOOLS_PATH <-
      gsub(
        "/",
        "\\\\",
        gsub(
          "usr/bin",
          "x86_64-w64-mingw32.static.posix/bin",
          file.path(dirname(RTOOLS_PATH))
        )
      )

    system(sprintf('setx PATH "%%PATH%%;%s;%s"',
                   R_BIN,
                   RTOOLS_PATH))
  }

  # Install TinyTex & TexLive
  if (RTOOLS_AVAILABLE) {
    message("\n\n[Installing TinyTex package...]\n")
    install.packages("tinytex",
                     type = "binary",
                     quiet = quiet,
                     repos = "https://cran.r-project.org/",
                     Ncpus = parallel::detectCores(),
                     INSTALL_opts = c('--no-lock'),
                     lib=R_LIBS_USER)

    TINYTEX_URL <- "https://yihui.org/tinytex/TinyTeX.zip"

    USER = Sys.info()[["user"]]
    ZIP_PATH <- file.path(sprintf("C:\\Users\\%s\\Downloads", USER))

    # Download the zip file to the Downloads folder & unzip
    message("\n\n[Downloading TinyTex LaTeX zip...]\n")
    download.file(TINYTEX_URL, destfile = file.path(ZIP_PATH, "TinyTex.zip"))

    ZIP_PATH <-
      file.path(sprintf("C:\\Users\\%s\\Downloads\\TinyTex.zip", USER))
    EXTRACT_PATH <- file.path(sprintf("C:\\Users\\%s\\dev\\", USER))
    message("\n\n[Decompressing TinyTex LaTeX zip...]\n")
    unzip(zipfile = ZIP_PATH, exdir = EXTRACT_PATH)

    # Add TinyTex to path
    message("\n\n[Adding TinyTex environment variable to Path...]\n")
    TINYTEX_PATH <-
      file.path(sprintf("C:\\Users\\%s\\dev\\TinyTeX\\bin\\win32", USER))
    system(sprintf(
      'setx PATH "%%PATH%%;%s;%s;%s"',
      R_BIN,
      RTOOLS_PATH,
      TINYTEX_PATH
    ))
  } else{
    message("\n\n[Installation of TinyTex failed because RTOOLS not available...]\n")
  }

  # Installing latest Quarto version

  USER <- Sys.info()[["user"]]
  ZIP_PATH <- file.path(sprintf("C:\\Users\\%s\\Downloads", USER))

  owner <- "quarto-dev"
  repo <- "quarto-cli"
  url <- sprintf("https://api.github.com/repos/%s/%s/releases/latest", owner, repo)

  # Make the GET request using httr
  response <- GET(url, add_headers(Accept = "application/vnd.github.v3+json"))

  # Extract the latest release version from the response using httr
  latest_version <- content(response)$tag_name

  cat(paste("The latest version of", owner, "/", repo, "is", latest_version))

  # Download the latest release zip file to the Downloads folder
  QUARTO_URL <- sprintf("https://github.com/%s/%s/releases/download/%s/quarto-%s-win.zip", owner, repo, latest_version, sub("^v", "", latest_version))
  message("\n\n[Downloading Quarto zip...]\n")
  download.file(QUARTO_URL, destfile = file.path(ZIP_PATH, "quarto.zip"))

  EXTRACT_PATH <- file.path(sprintf("C:\\Users\\%s\\dev\\quarto-%s", USER, latest_version), fsep='\\')
  message("\n\n[Decompressing Quarto zip...]\n")
  unzip(zipfile = file.path(ZIP_PATH, "quarto.zip", fsep='\\'), exdir = EXTRACT_PATH)

  # Add Quarto to PATH
  message("\n\n[Adding Quarto environment variable to Path...]\n")
  QUARTO_PATH <- file.path(EXTRACT_PATH, "bin", fsep = '\\')
  system(sprintf(
    'setx PATH "%%PATH%%;%s;%s;%s;%s"',
    R_BIN,
    RTOOLS_PATH,
    TINYTEX_PATH,
    QUARTO_PATH
  ))

  # Set RENV
  message("\n\n[Setting RENV environment variable...]\n")
  RENV_PATHS_ROOT <- sprintf("C:\\Users\\%s\\dev\\renv", USER)
  system(sprintf("setx RENV_PATHS_ROOT %s", RENV_PATHS_ROOT))

  RENV_PATHS_ROOT = file.path(RENV_PATHS_ROOT)
  if (!dir.exists(RENV_PATHS_ROOT)) {
    message(sprintf("\n\n[Creating the following directory: %s]\n", RENV_PATHS_ROOT))
    dir.create(RENV_PATHS_ROOT, recursive = TRUE)
  }

  # Custom ROracle installation
  message("\n\n[Installing ROracle (customized version by AIU)...]\n")
  CRO_ORIG <-
    "\\\\sky.corp.eurocontrol.int\\DFSRoot\\Public\\Haren\\db_connectivity\\ROracle"
  CRO_DEST <- R_LIBS_USER
  file.copy(CRO_ORIG, CRO_DEST, recursive = TRUE)

  # Install packages

  message("\n\n[Installing standard packages...]\n")

  detach("package:rstudioapi",unload=TRUE)
  options(install.packages.restart = FALSE)

  packages <- c(
    "tidyverse",
    "tidyr",
    "rvest",
    "sf",
    "pak",
    "datapasta",
    "devtools",
    "reticulate",
    "remotes",
    "here",
    "usethis"
  )

  # Filter out packages that are already installed
  packages_to_install <- packages[!(packages %in% installed.packages()[, "Package"])]

  # Install missing packages
  if (length(packages_to_install) > 0) {
    install.packages(
      packages_to_install,
      lib = R_LIBS_USER,
      type = "binary",
      quiet = quiet,
      repos = "https://cran.r-project.org/",
      Ncpus = parallel::detectCores(),
      INSTALL_opts = c('--no-lock'),
      restart = FALSE
    )
  }

  # # Update all installed packages
  # update.packages(
  #   ask = FALSE,
  #   checkBuilt = TRUE,
  #   lib = R_LIBS_USER,
  #   type = "binary",
  #   repos = "https://cran.r-project.org/",
  #   Ncpus = parallel::detectCores(),
  #   quiet = quiet,
  #   INSTALL_opts = c('--no-lock'),
  #   restart = FALSE
  # )

  # Loading these because devtools needs this and cannot read them yet from R_LIBS_USER
  library(withr, lib.loc = R_LIBS_USER)
  library(ps, lib.loc = R_LIBS_USER)
  library(usethis, lib.loc = R_LIBS_USER)
  library(devtools, lib.loc = R_LIBS_USER)
  library(rprojroot, lib.loc = R_LIBS_USER)

  message("\n\n[Installing addinexamples plugin...]\n")
  with_libpaths(
    new = R_LIBS_USER,
    devtools::install_github(
      "rstudio/addinexamples",
      upgrade = "always",
      type = "binary",
      quiet = quiet
    )
  )

  message("\n\n[Installing latest plotly...]\n")
  with_libpaths(
    new = R_LIBS_USER,
    devtools::install_github(
      "plotly/plotly",
      upgrade = "always",
      type = "binary",
      quiet = quiet
    )
  )

  message("\n\n[Installing pruatlas (AIU package)...]\n")
  with_libpaths(
    new = R_LIBS_USER,
    devtools::install_github(
      "euctrl-pru/pruatlas",
      upgrade = "always",
      type = "binary",
      quiet = quiet
    )
  )

  message("\n\n[Installing trrrj (AIU package)...]\n")
  with_libpaths(
    new = R_LIBS_USER,
    devtools::install_github(
      "euctrl-pru/trrrj",
      upgrade = "always",
      type = "binary",
      quiet = quiet
    )
  )

  # Setting blank slate
  library(usethis, lib.loc = R_LIBS_USER)
  library(rappdirs, lib.loc = R_LIBS_USER)
  library(jsonlite, lib.loc = R_LIBS_USER)
  message("\n\n[Setting blank slate...]\n")
  usethis::use_blank_slate(scope = c("user", "project"))

  # Make old folder (where packages were stored in the past) not retrievable by R to avoid conflicts
  # We don't delete in case we want to restore it. Added datetime in case there is already an archive.
  current_datetime <- Sys.time()
  datetime_string <- gsub("[-: ]", "_", as.character(current_datetime))
  file.rename(paste0("C:\\Users\\",USER,"\\AppData\\Local\\R\\"),paste0("C:\\Users\\",USER,"\\AppData\\Local\\R_archive_",datetime_string,"\\"))

  # End config
  message("\n\n[Configuration is complete, please restart R for the new configuration to take effect.]\n")
  message(
    "\n\n[In case you would still encounter issues: Contact Quinten.Goens@eurocontrol.int.]"
  )
}

setEnvironment()
