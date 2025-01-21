#' Core Fuzzy Joining Logic
#'
#' @param a the first dataframe to join
#' @param b the second dataframe to join
#' @param by columns on which to join
#' @param join_func the joining function responsible for performing the join
#' @param mode the dplyr-style type of join you want to perform
#' @param block_on any columns to block (perform exact matching on) (where supported)
#' @param ... Other parameters to be passed to the joining function
#'
#' @importFrom dplyr pull %>%
fuzzy_join_core <- function(a, b, by, join_func, mode, block_by = NULL, similarity_column = NULL, ...) {
  a <- tibble::as_tibble(a)
  b <- tibble::as_tibble(b)

  by <- multi_by_validate(a, b, by)
  by_a <- by[[1]]
  by_b <- by[[2]]

  if (!is.null(block_by)) {
      block_by <- multi_by_validate(a, b, block_by)
      block_by_a <- block_by[[1]]
      block_by_b <- block_by[[2]]
  } else {
      block_by_a <- NULL
      block_by_b <- NULL
  }

  match_result <- join_func(
                           a=a, b=b,
                           by_a = by_a,by_b = by_b,
                           block_by_a = block_by_a ,block_by_b = block_by_b,
                           ...)

  match_table <- match_result[['match_table']]
  similarities <- match_result[['similarities']]

  # Rename Columns in Both Tables
  names_in_both <- intersect(names(a), names(b))
  names(a)[names(a) %in% names_in_both] <- paste0(names(a)[names(a) %in% names_in_both], ".x")
  names(b)[names(b) %in% names_in_both] <- paste0(names(b)[names(b) %in% names_in_both], ".y")

  matches <- dplyr::bind_cols(a[match_table[, 1], ], b[match_table[, 2], ])

  if (!is.null(similarity_column)) {
    matches[, similarity_column] <- similarities
  }

  # No need to look for rows that don't match
  if (mode == "inner") {
    return(matches)
  }

  switch(mode,
    "left" = {
      not_matched_a <- collapse::`%!iin%`(seq_len(nrow(a)), match_table[, 1])
      matches <- dplyr::bind_rows(matches, a[not_matched_a, ])
    },
    "right" = {
      not_matched_b <- collapse::`%!iin%`(seq_len(nrow(b)), match_table[, 2])
      matches <- dplyr::bind_rows(matches, b[not_matched_b, ])
    },
    "full" = {
      not_matched_a <- collapse::`%!iin%`(seq_len(nrow(a)), match_table[, 1])
      not_matched_b <- collapse::`%!iin%`(seq_len(nrow(b)), match_table[, 2])
      matches <- dplyr::bind_rows(matches, a[not_matched_a, ], b[not_matched_b, ])
    },
    "anti" = {
      not_matched_a <- collapse::`%!iin%`(seq_len(nrow(a)), match_table[, 1])
      not_matched_b <- collapse::`%!iin%`(seq_len(nrow(b)), match_table[, 2])
      matches <- dplyr::bind_rows(a[not_matched_a, ], b[not_matched_b, ])
    }
  )

  matches
}

#' @importFrom dplyr pull %>%
jaccard_join <- function(a, b, by_a, by_b, block_by_a, block_by_b, n_gram_width, n_bands,
                          band_width, threshold, progress = FALSE, a_salt = NULL, b_salt = NULL,
                         clean = FALSE) {

  stopifnot("'threshold' must be of length 1" = length(threshold) == 1)
  stopifnot("'threshold' must be between 0 and 1" = threshold <= 1 & threshold >= 0)

  stopifnot("'by_a' must be of length 1" = length(by_a) == 1)
  stopifnot("'by_b' must be of length 1" = length(by_b) == 1)

  stopifnot("'n_bands' must be greater than 0" = n_bands > 0)
  stopifnot("'n_bands' must be length than 1" = length(n_bands) == 1)

  stopifnot("'band_width' must be greater than 0" = band_width > 0)
  stopifnot("'band_width' must be length than 1" = length(band_width) == 1)

  stopifnot("'n_gram_width' must be greater than 0" = n_gram_width > 0)
  stopifnot("'n_gram_width' must be length than 1" = length(n_gram_width) == 1)

  thresh_prob <- jaccard_probability(threshold, n_bands, band_width)

  if (thresh_prob < .95) {
    str <- paste0(
      "A pair of records at the threshold (", threshold,
      ") have only a ", round(thresh_prob * 100), "% chance of being compared.\n",
      "Please consider changing `n_bands` and `band_width`."
    )
    warning(str)
  }

  stopifnot("'by' vectors must have length 1" = length(by_a) == 1)
  stopifnot("'by' vectors must have length 1" = length(by_b) == 1)

  stopifnot("There should be no NA's in by_a" = !anyNA(a[[by_a]]))
  stopifnot("There should be no NA's in by_b" = !anyNA(b[[by_b]]))

  # Clean strings that are matched on
  if (clean) {
    a_col <- tolower(gsub("[[:punct:] ]", "", dplyr::pull(a, by_a)))
    b_col <- tolower(gsub("[[:punct:] ]", "", dplyr::pull(b, by_b)))

    if (!is.null(block_by_a) && !is.null(block_by_b)) {
      a_salt_col <- tidyr::unite(a, "block_by_a", dplyr::all_of(block_by_a)) %>%
        dplyr::pull("block_by_a")
      b_salt_col <- tidyr::unite(b, "block_by_b", dplyr::all_of(block_by_b)) %>%
        dplyr::pull("block_by_b")

      a_salt_col <- tolower(gsub("[[:punct:] ]", "", a_salt_col))
      b_salt_col <- tolower(gsub("[[:punct:] ]", "", b_salt_col))
    }
  } else {
    a_col <- dplyr::pull(a, by_a)
    b_col <- dplyr::pull(b, by_b)

    if (!is.null(block_by_a) && !is.null(block_by_b)) {
      a_salt_col <- tidyr::unite(a, "block_by_a", dplyr::all_of(block_by_a)) %>%
        dplyr::pull("block_by_a")

      b_salt_col <- tidyr::unite(b, "block_by_b", dplyr::all_of(block_by_b)) %>%
        dplyr::pull("block_by_b")
    }
  }

  if (is.null(block_by_a) || is.null(block_by_b)) {
    match_table <- rust_jaccard_join(
      a_col, b_col,
      n_gram_width, n_bands, band_width, threshold,
      progress,
      seed = 1
    )
  } else {
    match_table <- rust_salted_jaccard_join(
      a_col, b_col,
      a_salt_col, b_salt_col,
      n_gram_width, n_bands, band_width, threshold,
      progress,
      seed = round(runif(1, 0, 2^64))
    )
  }

  similarities <- jaccard_similarity(
      pull(a[match_table[, 1], ], by_a),
      pull(b[match_table[, 2], ], by_b),
       n_gram_width
     )

  return(list(
              match_table = match_table,
              similarities = similarities
    ))
}



