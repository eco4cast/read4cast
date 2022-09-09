

#' read a forecast file (csv or netcdf)
#' @param file_in path or URL to forecast to be read in
#' @param target_variables vector of valid variables being predicted
#' @param reps_col name of the replicates column (for ensemble forecasts)
#' @param quiet logical, default TRUE (show download progress?)
#' @param s3 Optionally, provide an S3 bucket (from `arrow::s3_bucket`).
#' In this case, `file_in` is interpreted as the object key on the bucket. 
#' @param ... additional options (currently ignored)
#' Reads in a valid forecast as a data.frame() in EFI format.
#' csv files are simply read in using `readr::read_csv`.
#' This utility  exists mostly to provide a single function that can
#' handle both csv and netcdf formats, as both are valid choices in EFI standard.
#' @export

read_forecast <- function(file_in, 
                          target_variables = c("oxygen", 
                                               "temperature", 
                                               "chla",
                                               "richness",
                                               "abundance", 
                                               "nee",
                                               "le", 
                                               "vswc",
                                               "gcc_90",
                                               "ixodes_scapularis",
                                               "amblyomma_americanum"),
                          reps_col = "ensemble",
                          quiet = TRUE,
                          s3 = NULL,
                          ...){
  
  if(!is.null(s3)) {
    if(grepl("[.]nc", file_in)){ #if file is nc
      dest <- tempfile(fileext=".nc")
      download_file(file_in, s3, dest)
      out <- read_forecast_nc(dest, )
      unlink(dest)
    } else {
      out <- read_arrow(file_in, s3)
    }
  } else if(any(vapply(c("[.]csv", "[.]csv\\.gz"), grepl, logical(1), file_in))){  
    # if file is csv zip file
    out <- readr::read_csv(file_in, guess_max = 1e6, lazy = FALSE, show_col_types = FALSE) 
    
    
  } else if(grepl("[.]nc", file_in)){ #if file is nc
    out <- read_forecast_nc(file_in, target_variables, reps_col, quiet = quiet)
  }
  
  out
}

read_arrow <- function(key, s3, ...) {
  
  requireNamespace("arrow", quietly = TRUE)
  if (grepl("\\.csv\\.gz$", key)) {
    obj <- arrow::CompressedInputStream$create(s3$OpenInputStream(key))
    out <- arrow::read_csv_arrow(obj, ...)
  } else if (grepl("\\.csv$", key)) {
    obj <- s3$path(key)
    out <- arrow::read_csv_arrow(obj, ...) 
  } else if (grepl("\\.parquet$", key)) {
    obj <- s3$path(key)
    out <- arrow::read_parquet(obj) 
  } else {
    stop("file format not recognized")
  }
  out
}


#GENERALIZATION: Specific target variables
read_forecast_nc <- function(file_in,
                             target_variables = c("oxygen", 
                                                  "temperature", 
                                                  "chla",
                                                  "richness",
                                                  "abundance", 
                                                  "nee",
                                                  "le", 
                                                  "vswc",
                                                  "gcc_90",
                                                  "ixodes_scapularis",
                                                  "amblyomma_americanum"),
                             reps_col = "ensemble",
                             quiet = TRUE)
{
  
  requireNamespace("dplyr", quietly = TRUE)
  requireNamespace("ncdf4", quietly = TRUE)
  requireNamespace("lubridate", quietly = TRUE)
  
  
  if(!file.exists(file_in)) {
    ## If URL is passed instead
    path <- tempfile(basename(file_in), fileext = tools::file_ext(file_in))
    utils::download.file(file_in, path, quiet = quiet)
    on.exit(unlink(path))
    file_in <- path
  }
  
  nc <- ncdf4::nc_open(file_in)
  if("time" %in% names(nc$dim)){
    time_nc <- as.integer(ncdf4::ncvar_get(nc, "time"))
    t_string <- strsplit(ncdf4::ncatt_get(nc, varid = "time", "units")$value, " ")[[1]]
    tustr<-strsplit(ncdf4::ncatt_get(nc, varid = "time", "units")$value, " ")
  }else{
    time_nc <- as.integer(ncdf4::ncvar_get(nc, "datetime"))
    t_string <- strsplit(ncdf4::ncatt_get(nc, varid = "datetime", "units")$value, " ")[[1]] 
    tustr<-strsplit(ncdf4::ncatt_get(nc, varid = "datetime", "units")$value, " ")
  }
  global_attributes <- ncdf4::ncatt_get(nc, varid = 0)
  if("start_time" %in% names(global_attributes)){
    reference_datetime <- global_attributes$start_time
  }else if("reference_datetime" %in% names(global_attributes)){
    reference_datetime <- global_attributes$reference_datetime
  }else{
    reference_datetime <- NULL
  }
  if(t_string[1] == "days"){
    time_nc <-lubridate::as_date(time_nc,origin=unlist(tustr)[3])
    if(is.null(reference_datetime)){
      reference_datetime <- lubridate::as_date(time_nc[1])
    }else{
      reference_datetime <- lubridate::as_date(reference_datetime)
    }
  }else{
    tustr <- lubridate::as_datetime(tustr[[1]][3])
    time_nc <- as.POSIXct.numeric(time_nc, origin = tustr)
    if(is.null(reference_datetime)){
      reference_datetime <- lubridate::as_datetime(time_nc[1])
    }else{
      reference_datetime <- lubridate::as_datetime(reference_datetime)
    }
  } 
  targets <- names(nc$var)[which(names(nc$var) %in% target_variables)]
  
  ncdf4::nc_close(nc)
  
  nc_tidy <- tidync::tidync(file_in)
  df <- nc_tidy %>% tidync::hyper_tibble(select_var = targets[1])
  
  if("time" %in% names(df)){
    df <- df %>%
      rename(time = datetime)
  }
  
  if(length(targets) > 1){
    for(i in 2:length(targets)){
      new_df <- nc_tidy %>% tidync::hyper_tibble(select_var = targets[i]) %>% 
        dplyr::select(targets[i]) 
      df <- dplyr::bind_cols(df, new_df)
    }
  }
  
  time_tibble <- dplyr::tibble(datetime = unique(df$datetime),
                               new_value = time_nc)
  
  df <- df %>% 
    dplyr::left_join(time_tibble, by = "datetime") %>% 
    dplyr::mutate(datetime = new_value) %>% 
    dplyr::select(-new_value)
  
  if("site" %in% names(df)){
    nc <- ncdf4::nc_open(file_in)
    #GENERALIZATION:  Hack because ticks didn't make siteID unique in Round 1
    if(("ixodes_scapularis" %in% names(nc$var) | "amblyomma_americanum" %in% names(nc$var)) & "plotID" %in% names(nc$var)){
      site_id <- ncdf4::ncvar_get(nc, "plotID")
    }else{
      if("siteID" %in% names(nc$var)){
        site_id <- ncdf4::ncvar_get(nc, "siteID")  
      }else{
        site_id <- ncdf4::ncvar_get(nc, "site_id")
      }
    }
    ncdf4::nc_close(nc)
    
    site_tibble  <- dplyr::tibble(site = unique(df$site),
                                  new_value = as.vector(site_id))
    df <- df %>% 
      dplyr::left_join(site_tibble, by = "site") %>% 
      dplyr::mutate(site = new_value) %>% 
      dplyr::select(-new_value) 
  }
  
  if("depth" %in% names(df)){
    nc <- ncdf4::nc_open(file_in)
    depth <- ncdf4::ncvar_get(nc, "depth")
    ncdf4::nc_close(nc)
    
    depth_tibble  <- dplyr::tibble(depth = unique(df$depth),
                                   new_value = as.vector(depth)) 
    df <- df %>% 
      dplyr::left_join(depth_tibble, by = "depth") %>% 
      dplyr::mutate(time = new_value) %>% 
      dplyr::select(-new_value)
  }
  
  df <- df %>% 
    dplyr::rename(site_id = site) %>% 
    tidyr::pivot_longer(dplyr::any_of(targets), names_to = "variable", values_to = "predicted")
  
  if("start_time" %in% names(global_attributes)){
    df <- df %>% 
      dplyr::mutate(reference_datetime = start_time)
  }else  if("reference_datetime" %in% names(global_attributes)){
    df <- df %>% 
      dplyr::mutate(reference_datetime = reference_datetime)
  }
  
  if(!("family" %in% nc$dim)){
    df <- df |> 
      mutate(family = "ensemble")
  }
  
  out <- df %>% 
    dplyr::select(dplyr::any_of(c("datetime", "reference_datetime", "site_id","depth","ensemble","family", "parameter", 
                                  "forecast","data_assimilation", "variable", "predicted")))
  
  out
}

utils::globalVariables("new_value", package="read4cast")


#' @importFrom readr read_csv
#' @importFrom dplyr `%>%`
NULL
