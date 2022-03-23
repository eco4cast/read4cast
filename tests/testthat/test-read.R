
test_that("ncdf by URL", {
  
  fc <- 
    paste0("https://data.ecoforecast.org/forecasts/",
           "terrestrial_30min/terrestrial_30min-2022-01-01-hist30min.nc")
  df <- read_forecast(fc)
  expect_s3_class(df, "tbl_df")
  
})
test_that("csv.gz by URL", {
    
  fc <- paste0("https://data.ecoforecast.org/forecasts/aquatics/",
               "aquatics-2020-09-01-EFInull.csv.gz")
  df <- read_forecast(fc)
  expect_s3_class(df, "tbl_df")
  
})