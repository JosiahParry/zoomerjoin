multi_by_validate <- function(a, b, by) {
  # first pass to handle dplyr::join_by() call
  if (inherits(by, "dplyr_join_by")) {
    if (any(by$condition != "==")) {
      stop("Inequality joins are not supported.")
    }
    new_by <- by$y
    names(new_by) <- by$x
    by <- new_by
  }

  if (is.null(by)) {
    by_a <- intersect(names(a), names(b))
    by_b <- by_a
    stopifnot("Can't Determine columns to match on" = length(by_a)!=0)
  } else {
    if (!is.null(names(by))) {
      by_a <- names(by)
      by_b <- by
    } else {
      by_a <- by
      by_b <- by
    }
    stopifnot(by_a %in% names(a))
    stopifnot(by_b %in% names(b))
  }
  return(list(
    by_a,
    by_b
  ))
}


# ` @importFrom stats pnorm
euclidean_join <- function(a, b, by_a, by_b, block_by_a = NULL, block_by_b = NULL, n_bands = 30, band_width = 10, threshold = 1.0, r = .5, progress = FALSE) {
  stopifnot("'radius' must be greater than 0" = threshold > 0)
  stopifnot("There should be no NA's in by_a[1]" = !anyNA(a[[by_a[1]]]))
  stopifnot("There should be no NA's in by_a[2]" = !anyNA(a[[by_a[2]]]))
  stopifnot("There should be no NA's in by_b[1]" = !anyNA(b[[by_b[1]]]))
  stopifnot("There should be no NA's in by_b[2]" = !anyNA(b[[by_b[2]]]))

  thresh_prob <- euclidean_probability(threshold, n_bands, band_width, r)
  if (thresh_prob < .95) {
    str <- paste0(
      "A pair of records at the threshold (", threshold,
      ") have only a ", round(thresh_prob * 100), "% chance of being compared.\n",
      "Please consider changing `n_bands` and `band_width`, and `r`."
    )

    warning(str)
  }

  match_table <- rust_p_norm_join(
    a_mat = as.matrix(dplyr::select(a, dplyr::all_of(by_a))),
    b_mat = as.matrix(dplyr::select(b, dplyr::all_of(by_b))),
    radius = threshold,
    band_width = band_width,
    n_bands = n_bands,
    r = r,
    progress = progress,
    seed = round(runif(1, 0, 2^32))
  )

  return(list(match_table = match_table))
}
