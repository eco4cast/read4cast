
test_that("ncdf by URL", {
  
  fc <- 
    paste0("https://data.ecoforecast.org/forecasts/",
           "terrestrial_30min/terrestrial_30min-2022-01-01-hist30min.nc")
  df <- read_forecast(fc)
  expect_s3_class(df, "tbl_df")
  
})
test_that("csv.gz by URL", {
    
  fc <- paste0("https://data.ecoforecast.org/neon4cast-forecasts/aquatics/",
               "aquatics-2020-09-01-EFInull.csv.gz")
  df <- read_forecast(fc)
  expect_s3_class(df, "tbl_df")
  
})

test_that("csv.gz by S3", {
  
  s3 <- arrow::s3_bucket("neon4cast-forecasts/aquatics", 
                  endpoint_override = "data.ecoforecast.org", 
                  anonymous = TRUE)
  key <- "aquatics-2020-09-01-EFInull.csv.gz"

  df <- read_forecast(key, s3=s3)
  expect_s3_class(df, "tbl_df")
  
})

test_that("nc by S3", {
  ## ncdf versions are converted to csv due to pivoting
  s3 <- arrow::s3_bucket("forecasts", 
                         endpoint_override = "data.ecoforecast.org", 
                         anonymous = TRUE)
  key <- "terrestrial_30min/terrestrial_30min-2022-01-01-hist30min.nc"
  
  df <- read_forecast(key, s3=s3)
  expect_s3_class(df, "tbl_df")
  
})