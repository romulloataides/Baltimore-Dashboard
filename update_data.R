library(tidyverse)
library(sf)
library(jsonlite)
library(tidycensus)
library(httr)

print("Starting Baltimore Health Data Pipeline...")

normalize_name <- function(x) {
  x %>%
    replace_na("") %>%
    str_replace_all("&", " and ") %>%
    str_replace_all("[^[:alnum:]]+", " ") %>%
    str_squish() %>%
    str_to_lower()
}

# 1. SETUP: Authenticate Census API
census_key <- Sys.getenv("CENSUS_API_KEY")
if (identical(census_key, "")) {
  stop("CENSUS_API_KEY is not set.")
}
census_api_key(census_key)

# 2. EXTRACT: Fetch Spatial Boundaries
print("Fetching neighborhood boundaries...")

nsa_url <- paste0(
  "https://egis.baltimorecity.gov/egis/rest/services/",
  "Housing/dmxBoundaries/MapServer/1/query?",
  "where=1%3D1&outFields=Name&returnGeometry=true&f=geojson"
)

response <- GET(
  nsa_url,
  add_headers(
    `User-Agent` = "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    Accept = "application/geo+json, application/json;q=0.9, */*;q=0.8"
  ),
  timeout(60)
)

stop_for_status(response)

content_type <- headers(response)[["content-type"]]
boundary_text <- content(response, "text", encoding = "UTF-8")

if (is.null(content_type)) {
  content_type <- "unknown"
}

if (
  !str_detect(str_to_lower(content_type), "json") ||
  !str_detect(boundary_text, '"type"\\s*:\\s*"FeatureCollection"')
) {
  stop(
    paste0(
      "Boundary endpoint did not return valid GeoJSON. Content-Type: ",
      content_type,
      ". Response starts with: ",
      substr(boundary_text, 1, 200)
    )
  )
}

writeLines(boundary_text, "nsa_boundaries.geojson", useBytes = TRUE)

nsa_boundaries <- st_read("nsa_boundaries.geojson", quiet = TRUE) %>%
  st_transform(crs = 4326) %>%
  select(Neighborhood = Name, geometry) %>%
  mutate(
    Neighborhood = trimws(Neighborhood),
    Neighborhood_key = normalize_name(Neighborhood)
  )

boundary_lookup <- nsa_boundaries %>%
  st_drop_geometry() %>%
  distinct(Neighborhood, Neighborhood_key)

# 3. EXTRACT & TRANSFORM: Historical 311 Environmental Hazards
print("Fetching historical 311 data...")

fetch_arcgis_layer <- function(service_url, out_fields) {
  meta_response <- GET(service_url, query = list(f = "json"), timeout(60))
  stop_for_status(meta_response)

  meta <- fromJSON(
    content(meta_response, "text", encoding = "UTF-8"),
    simplifyVector = FALSE
  )

  if (!is.null(meta$error)) {
    stop(
      paste(
        "ArcGIS metadata request failed for",
        service_url,
        ":",
        meta$error$message
      )
    )
  }

  page_size <- meta$maxRecordCount
  if (is.null(page_size) || page_size <= 0) {
    page_size <- 2000L
  }
  page_size <- min(as.integer(page_size), 2000L)

  offset <- 0L
  batches <- list()

  repeat {
    response <- GET(
      service_url,
      query = list(
        where = "1=1",
        outFields = paste(out_fields, collapse = ","),
        returnGeometry = "false",
        orderByFields = "RowID ASC",
        resultOffset = offset,
        resultRecordCount = page_size,
        f = "json"
      ),
      timeout(120)
    )
    stop_for_status(response)

    payload <- fromJSON(
      content(response, "text", encoding = "UTF-8"),
      simplifyVector = FALSE
    )

    if (!is.null(payload$error)) {
      stop(
        paste(
          "ArcGIS query failed for",
          service_url,
          ":",
          payload$error$message
        )
      )
    }

    features <- payload$features
    if (is.null(features) || length(features) == 0) {
      break
    }

    batch <- purrr::map_dfr(features, function(feature) {
      attr <- feature$attributes
      if (is.null(attr)) {
        attr <- list()
      }

      tibble(
        SRType = if (is.null(attr$SRType)) NA_character_ else as.character(attr$SRType),
        SRStatus = if (is.null(attr$SRStatus)) NA_character_ else as.character(attr$SRStatus),
        CreatedDate = if (is.null(attr$CreatedDate)) NA_real_ else as.numeric(attr$CreatedDate),
        Neighborhood = if (is.null(attr$Neighborhood)) NA_character_ else as.character(attr$Neighborhood)
      )
    })

    if (nrow(batch) == 0) {
      break
    }

    batches[[length(batches) + 1L]] <- batch

    if (!isTRUE(payload$exceededTransferLimit)) {
      break
    }

    offset <- offset + nrow(batch)
  }

  dplyr::bind_rows(batches)
}

three11_sources <- c(
  "2016" = "https://services1.arcgis.com/UWYHeuuJISiGmgXx/ArcGIS/rest/services/311_Customer_Service_Requests_Yearly/FeatureServer/6",
  "2017" = "https://services1.arcgis.com/UWYHeuuJISiGmgXx/ArcGIS/rest/services/311_Customer_Service_Requests_Yearly/FeatureServer/5",
  "2018" = "https://services1.arcgis.com/UWYHeuuJISiGmgXx/ArcGIS/rest/services/311_Customer_Service_Requests_Yearly/FeatureServer/4",
  "2019" = "https://services1.arcgis.com/UWYHeuuJISiGmgXx/ArcGIS/rest/services/311_Customer_Service_Requests_Yearly/FeatureServer/3",
  "2020" = "https://services1.arcgis.com/UWYHeuuJISiGmgXx/ArcGIS/rest/services/311_Customer_Service_Requests_Yearly/FeatureServer/2",
  "2021" = "https://services1.arcgis.com/UWYHeuuJISiGmgXx/ArcGIS/rest/services/311_Customer_Service_Requests_Yearly/FeatureServer/1",
  "2022" = "https://services1.arcgis.com/UWYHeuuJISiGmgXx/ArcGIS/rest/services/311_Customer_Service_Requests_Yearly/FeatureServer/0",
  "2023" = "https://services1.arcgis.com/UWYHeuuJISiGmgXx/ArcGIS/rest/services/311_Customer_Service_Requests_2023/FeatureServer/0"
)

raw_311 <- purrr::imap_dfr(three11_sources, function(service_url, year_label) {
  print(paste("  Downloading 311 records for", year_label, "..."))
  fetch_arcgis_layer(
    service_url,
    out_fields = c("SRType", "SRStatus", "CreatedDate", "Neighborhood")
  )
})

required_cols <- c("SRType", "SRStatus", "CreatedDate", "Neighborhood")
missing_cols <- setdiff(required_cols, names(raw_311))

if (length(missing_cols) > 0) {
  stop(
    paste(
      "311 data is missing required columns:",
      paste(missing_cols, collapse = ", ")
    )
  )
}

nsa_311_clean <- raw_311 %>%
  transmute(
    SRType = SRType,
    SRStatus = SRStatus,
    CreatedDate = as.numeric(CreatedDate),
    Neighborhood_raw = str_squish(Neighborhood)
  ) %>%
  filter(!is.na(CreatedDate), !is.na(Neighborhood_raw), Neighborhood_raw != "") %>%
  mutate(
    Year = lubridate::year(lubridate::as_datetime(CreatedDate / 1000, tz = "UTC")),
    Neighborhood_key = normalize_name(Neighborhood_raw)
  ) %>%
  left_join(boundary_lookup, by = "Neighborhood_key") %>%
  filter(
    !is.na(Neighborhood),
    Year >= 2016,
    Year <= 2023,
    SRStatus != "Closed (Duplicate)"
  ) %>%
  mutate(
    Category = case_when(
      str_detect(str_to_lower(SRType), "rat|rodent") ~ "rt",
      str_detect(str_to_lower(SRType), "trash|dumping") ~ "dp",
      str_detect(str_to_lower(SRType), "water|sewer") ~ "ws",
      TRUE ~ "other"
    )
  ) %>%
  filter(Category != "other") %>%
  select(Neighborhood, Neighborhood_key, Category, Year)

nsa_311_yearly <- nsa_311_clean %>%
  group_by(Neighborhood, Neighborhood_key, Category, Year) %>%
  summarize(Total = n(), .groups = "drop")

total_hazards <- nsa_311_yearly %>%
  group_by(Neighborhood, Neighborhood_key, Year) %>%
  summarize(Total = sum(Total), .groups = "drop") %>%
  mutate(Category = "hz")

all_311_yearly <- bind_rows(nsa_311_yearly, total_hazards)

complete_grid <- tidyr::crossing(
  boundary_lookup,
  Category = c("rt", "dp", "ws", "hz"),
  Year = 2016:2023
)

nsa_311_arrays <- complete_grid %>%
  left_join(all_311_yearly, by = c("Neighborhood", "Neighborhood_key", "Category", "Year")) %>%
  mutate(Total = replace_na(Total, 0)) %>%
  arrange(Neighborhood, Category, Year) %>%
  group_by(Neighborhood, Neighborhood_key, Category) %>%
  summarize(yearly_array = list(as.numeric(Total)), .groups = "drop") %>%
  pivot_wider(names_from = Category, values_from = yearly_array)

# 4. EXTRACT & TRANSFORM: ACS Demographic Baselines
print("Fetching and processing Census data...")

acs_vars <- c(
  Poverty = "B17001_002",
  Total_Pop = "B17001_001"
)

baltimore_acs <- get_acs(
  geography = "tract",
  variables = acs_vars,
  state = "MD",
  county = "Baltimore city",
  year = 2022,
  geometry = TRUE,
  output = "wide"
) %>%
  st_transform(crs = 4326) %>%
  mutate(
    pv = round((PovertyE / Total_PopE) * 100, 1),
    pv = replace_na(pv, 0)
  ) %>%
  select(GEOID, pv, geometry)

nsa_acs_summary <- st_intersection(
  nsa_boundaries %>% select(Neighborhood, Neighborhood_key),
  baltimore_acs
) %>%
  mutate(intersect_area = st_area(geometry)) %>%
  st_drop_geometry() %>%
  group_by(Neighborhood, Neighborhood_key) %>%
  summarize(
    pv = round(weighted.mean(pv, as.numeric(intersect_area), na.rm = TRUE), 1),
    .groups = "drop"
  )

# 5. MERGE: Combine Data
base_data_path <- "data.json"

if (file.exists(base_data_path)) {
  current_data_raw <- read_json(base_data_path, simplifyVector = FALSE)

  if (
    !is.list(current_data_raw) ||
    is.null(names(current_data_raw)) ||
    any(names(current_data_raw) == "")
  ) {
    stop(
      paste(
        "data.json must be a named object keyed by Neighborhood.",
        "Example: {\"Canton\": {\"hi\": 84, \"le\": 80, ...}}"
      )
    )
  }

  current_data <- tibble(
    Neighborhood = names(current_data_raw),
    data = unname(current_data_raw)
  ) %>%
    unnest_wider(data) %>%
    mutate(Neighborhood_key = normalize_name(Neighborhood))
} else {
  stop("Missing initial data.json to use as a base.")
}

final_dashboard_data <- current_data %>%
  select(-any_of(c("pv", "rt", "dp", "ws", "hz"))) %>%
  left_join(
    nsa_acs_summary %>% select(Neighborhood_key, pv),
    by = "Neighborhood_key"
  ) %>%
  left_join(
    nsa_311_arrays %>% select(Neighborhood_key, rt, dp, ws, hz),
    by = "Neighborhood_key"
  ) %>%
  mutate(pv = replace_na(pv, 0))

array_cols <- intersect(c("rt", "dp", "ws", "hz"), names(final_dashboard_data))

if (length(array_cols) > 0) {
  final_dashboard_data <- final_dashboard_data %>%
    mutate(across(all_of(array_cols), ~ map(., function(x) {
      if (is.null(x) || all(is.na(x))) rep(0, 8) else as.numeric(x)
    })))
}

# 6. LOAD: Write to JSON
print("Formatting and writing output...")

json_source <- final_dashboard_data %>%
  select(-Neighborhood_key)

json_ready_data <- split(json_source, json_source$Neighborhood) %>%
  map(~ .x %>% select(-Neighborhood) %>% as.list() %>% map(~ .x[[1]]))

write_json(json_ready_data, "data.json", auto_unbox = TRUE, pretty = TRUE)

print("Pipeline Complete! Longitudinal data.json updated.")
