#' download an arbitrary file from S3
#' 
#' Note: file must be small enough to fit in RAM with this method.
#' @param dest file name or connection where file should be written locally
#' @param key the file name (key) to be used in the bucket (string, can include path)
#' @param s3 an arrow FileSystem object (usually S3 Filesystem)
#' @export
#' @examplesIf interactive()
#' s3 <- arrow::s3_bucket("neon4cast-forecasts", 
#'                        endpoint_override = "data.ecoforecast.org", 
#'                        anonymous=TRUE)
#' example <- tail(s3$ls("phenology"), 1)
#' dest <- tempfile()
#' download_file(example, s3, dest)
download_file <- function(key, s3, dest = basename(key)) {
  ## Assumes full object fits into RAM
  raw <- get_raw(key, s3)
  writeBin(raw, dest)
}
get_raw <- function(key, s3) {
  x <- s3$OpenInputFile(key)
  raw <- x$Read()$data()
  x$close()
  raw
}

#' upload a file from local disk to an S3 bucket
#'
#' Note: file must be small enough to fit in RAM with this method.
#' @param file file name or connection of file to be uploaded
#' @inheritParams download_file
#' @export
upload_file <- function(file, key = file, s3) {
  x <- s3$OpenOutputStream(key)
  raw <- readBin(file)
  x$write(raw)
  x$close()
}




get_object <- function(key, s3, fun = readr::read_csv, ...) {
  raw <- get_raw(key, s3)
  fun(raw, ...)
}



#' upload an object to an arrow filesystem (such as remote S3 bucket)
#' @param obj an R object that can be serialized to disk by `fun` (e.g. data.frame)
#' @param key the file name (key) to be used in the bucket (string, can include path)
#' @param s3 an arrow FileSystem object (usually S3 Filesystem, see arrow::s3_bucket)
#' @param fun a function that can serialize obj (to an anonymous file).
#' @param ... additional arguments to `fun`
put_object <- function(obj, key, s3, fun = readr::write_csv, ...) {
  x <- s3$OpenOutputStream(key)
  raw <- serialize_raw(obj, fun, ...)
  # can possibly call multiple times if necessary to append chunks
  x$write(raw)
  x$close()
}

serialize_raw <- function(object,
                          fun = readr::write_csv,
                          ...) {
  
  zzz <- file(open="w+b")
  on.exit(close(zzz))
  
  ## Serialize to anonymous file
  fun(object, zzz, ...)
  readBin(zzz, "raw", seek(zzz)) #overestimate with maximum desired chunk size
  
}





## Draft methods that can work in chunks for larger-than-ram data:

## Optional chunked version, for large data
serialize_raw_chunked <- function(obj,
                                  fun = readr::write_csv,
                                  chunk_size = 1000000L,
                                  iter = 0L,
                                  ...) {
  
  
  zzz <- file(open="w+b")
  on.exit(close(zzz))
  seek(zzz, as.integer(iter * chunk_size)) # start from iter
  
  ## Serialize to anonymous file
  fun(obj, zzz, ...)
  raw <- readBin(zzz, "raw", chunk_size) #overestimate with maxiumum desired chunk size
  
  
  ## check if we have multiple parts
  pos <- seek(zzz)
  
  if(pos >= chunk_size * (iter+1)){
    message(paste("returning first", chunk_size, "bytes\n",
                  "increase iter number by 1 and repeat call"))
  }
  
  raw
  
}

put_object_chunks <- function(obj, key, s3, fun = readr::write_csv, ...) {
  x <- s3$OpenOutputStream(key)
  raw <- serialize_raw_chunked(obj, fun, ...)
  # can possibly call multiple times if necessary to append chunks
  x$write(raw)
  x$close()
}
