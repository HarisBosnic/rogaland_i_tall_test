rm(list = ls())

library(httr2)
library(jsonlite)
library(rjstat)
library(dplyr)
library(stringr)

try(Sys.setlocale("LC_CTYPE", "nb_NO.UTF-8"), silent = TRUE)

ssb_hent_med_fylke_generisk <- function(
    table_id,
    contents_code,
    aar = 2006:2025,
    start_kode = "K-0301",
    slutt_kode = "K-5636",
    regioner = NULL,
    extra_dims = list(),
    dim_region   = "Region",
    dim_tid      = "Tid",
    dim_contents = "ContentsCode",
    region_codelist = "agg_KommSummer",
    region_output   = "aggregated",
    max_cells = 300000,
    chunk_years = TRUE
) {
  
  agg_url <- sprintf(
    "https://data.ssb.no/api/pxwebapi/v2/codeLists/%s?lang=no",
    region_codelist
  )
  
  agg <- httr2::request(agg_url) |>
    httr2::req_perform() |>
    httr2::resp_body_string() |>
    jsonlite::fromJSON()
  
  aggmap <- agg$values |>
    dplyr::select(code, label) |>
    dplyr::rename(
      k_kode       = code,
      region_label = label
    )
  
  komm_koder <- aggmap$k_kode
  
  if (is.null(regioner)) {
    i1 <- match(start_kode, komm_koder)
    i2 <- match(slutt_kode, komm_koder)
    
    if (is.na(i1) | is.na(i2)) stop("Start- eller slutkode finnes ikke i agg_KommSummer.")
    if (i1 > i2) stop("start_kode kommer etter slutt_kode — snu dem?")
    
    region_spec <- sprintf("[range(%s,%s)]", start_kode, slutt_kode)
    region_mode <- "range"
  } else {
    regioner <- intersect(regioner, komm_koder)
    if (length(regioner) == 0) stop("Ingen gyldige region-koder i 'regioner'.")
    region_spec <- regioner
    region_mode <- "list"
  }
  
  fylke_navn_2024 <- c(
    "03" = "Oslo",
    "11" = "Rogaland",
    "15" = "Møre og Romsdal",
    "18" = "Nordland",
    "31" = "Østfold",
    "32" = "Akershus",
    "33" = "Buskerud",
    "34" = "Innlandet",
    "39" = "Vestfold",
    "40" = "Telemark",
    "42" = "Agder",
    "46" = "Vestland",
    "50" = "Trøndelag",
    "55" = "Troms",
    "56" = "Finnmark"
  )
  
  calc_cells <- function(years_vec) {
    n_contents <- length(contents_code)
    n_years    <- length(years_vec)
    n_region   <- if (region_mode == "range") {
      i1 <- match(start_kode, komm_koder)
      i2 <- match(slutt_kode, komm_koder)
      (i2 - i1 + 1)
    } else {
      length(region_spec)
    }
    n_extra <- if (length(extra_dims) == 0) 1 else prod(vapply(extra_dims, length, 1L))
    n_contents * n_region * n_years * n_extra
  }
  
  split_years <- function(years_vec) {
    years_vec <- as.character(years_vec)
    if (!chunk_years) return(list(years_vec))
    
    chunks <- list()
    cur <- character(0)
    
    for (y in years_vec) {
      test <- c(cur, y)
      if (length(cur) > 0 && calc_cells(test) > max_cells) {
        chunks[[length(chunks) + 1]] <- cur
        cur <- y
      } else {
        cur <- test
      }
    }
    
    if (length(cur) > 0) chunks[[length(chunks) + 1]] <- cur
    chunks
  }
  
  fetch_one <- function(years_vec) {
    base_url <- sprintf("https://data.ssb.no/api/pxwebapi/v2/tables/%s/data", table_id)
    
    q <- list()
    q[["lang"]] <- "no"
    q[[sprintf("valueCodes[%s]", dim_contents)]] <- paste(contents_code, collapse = ",")
    q[[sprintf("valueCodes[%s]", dim_tid)]]      <- paste(years_vec, collapse = ",")
    q[[sprintf("codelist[%s]", dim_region)]]     <- region_codelist
    q[[sprintf("outputValues[%s]", dim_region)]] <- region_output
    
    if (region_mode == "range") {
      q[[sprintf("valueCodes[%s]", dim_region)]] <- region_spec
    } else {
      q[[sprintf("valueCodes[%s]", dim_region)]] <- paste(region_spec, collapse = ",")
    }
    
    if (length(extra_dims) > 0) {
      for (nm in names(extra_dims)) {
        q[[sprintf("valueCodes[%s]", nm)]] <- paste(extra_dims[[nm]], collapse = ",")
      }
    }
    
    resp <- httr2::request(base_url) |>
      httr2::req_url_query(!!!q) |>
      httr2::req_perform()
    
    js <- rjstat::fromJSONstat(httr2::resp_body_string(resp))
    df <- if (is.data.frame(js)) js else js[[1]]
    
    df |>
      dplyr::left_join(aggmap, by = c("region" = "region_label")) |>
      dplyr::mutate(
        kommune_nr = stringr::str_sub(k_kode, 3, 6),
        fylkesnr   = stringr::str_sub(kommune_nr, 1, 2),
        fylke      = unname(fylke_navn_2024[fylkesnr])
      )
  }
  
  year_chunks <- split_years(aar)
  dplyr::bind_rows(lapply(year_chunks, fetch_one))
}

# ---- Tabell 06265 ----
df_raw <- ssb_hent_med_fylke_generisk(
  table_id = "06265",
  contents_code = "Boliger",
  aar = 2006:2025,
  start_kode = "K-3101",
  slutt_kode = "K-5636",
  extra_dims = list(
    BygnType = c("01", "02", "03", "04", "05", "999")
  )
)

df_bolig <- df_raw |>
  dplyr::filter(!is.na(fylke)) |>
  dplyr::rename(
    aar      = `år`,
    bygnType = bygningstype
  ) |>
  dplyr::mutate(
    aar   = as.character(aar),
    value = as.numeric(value)
  )

available_years  <- sort(unique(df_bolig$aar))
latest_year      <- as.character(max(as.integer(available_years), na.rm = TRUE))
available_fylker <- sort(unique(df_bolig$fylke))

kommuner_rogaland <- df_bolig |>
  dplyr::filter(fylke == "Rogaland") |>
  dplyr::distinct(region) |>
  dplyr::arrange(region) |>
  dplyr::pull(region)

bygn_levels <- c(
  "Enebolig",
  "Boligblokk",
  "Rekkehus, kjedehus og andre småhus",
  "Tomannsbolig",
  "Andre bygningstyper",
  "Bygning for bofellesskap"
)

# ---- Tabell 05940 ----
df_05940 <- ssb_hent_med_fylke_generisk(
  table_id = "05940",
  contents_code = c("Igangsatte", "Fullforte", "BruksarealIgang", "BruksarealFullfort"),
  aar = 2000:2024,
  start_kode = "K-3101",
  slutt_kode = "K-5636",
  extra_dims = list(
    Byggeareal = c(
      "111","112","113","121","122","123","124","131","133","135","136",
      "141","142","143","144","145","146","151","152","159","000"
    )
  ),
  max_cells = 300000,
  chunk_years = TRUE
)

df_ferdig <- df_05940 |>
  dplyr::rename(
    aar      = `år`,
    contents = statistikkvariabel,
    bygtype  = bygningstype
  ) |>
  dplyr::mutate(
    aar   = as.character(aar),
    value = as.numeric(value)
  )

available_years_ferdig  <- sort(unique(df_ferdig$aar))
available_fylker_ferdig <- sort(unique(df_ferdig$fylke))

kommuner_rogaland_ferdig <- df_ferdig |>
  dplyr::filter(
    fylke == "Rogaland",
    !is.na(kommune_nr),
    nchar(kommune_nr) == 4
  ) |>
  dplyr::distinct(region) |>
  dplyr::arrange(region) |>
  dplyr::pull(region)

contents_choices_ferdig <- sort(unique(df_ferdig$contents))
bygtype_choices_ferdig  <- sort(unique(df_ferdig$bygtype))


# ---- Lagre til CSV (raw_data) ----

dir.create("raw_data", showWarnings = FALSE, recursive = TRUE)

readr::write_csv(df_bolig, "raw_data/df_bolig.csv")
readr::write_csv(df_ferdig, "raw_data/df_ferdig.csv")



