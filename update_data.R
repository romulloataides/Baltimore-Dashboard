library(tidyverse)
library(sf)
library(jsonlite)
library(tidycensus)
library(httr)

print("Starting Baltimore Health Data Pipeline (CSA Architecture)...")

normalize_name <- function(x) {
  x %>%
    replace_na("") %>%
    str_replace_all("&", " and ") %>%
    str_replace_all("[^[:alnum:]]+", " ") %>%
    str_squish() %>%
    str_to_lower()
}

empty_311_tbl <- tibble(
  SRType = character(),
  SRStatus = character(),
  CreatedDate = numeric(),
  Longitude = numeric(),
  Latitude = numeric()
)

coerce_311_schema <- function(df) {
  if (is.null(df) || nrow(df) == 0) {
    return(empty_311_tbl)
  }

  df <- as_tibble(df)

  if (!"SRType" %in% names(df)) df$SRType <- NA_character_
  if (!"SRStatus" %in% names(df)) df$SRStatus <- NA_character_
  if (!"CreatedDate" %in% names(df)) df$CreatedDate <- NA_real_
  if (!"Longitude" %in% names(df)) df$Longitude <- NA_real_
  if (!"Latitude" %in% names(df)) df$Latitude <- NA_real_

  df %>%
    transmute(
      SRType = as.character(SRType),
      SRStatus = as.character(SRStatus),
      CreatedDate = as.numeric(CreatedDate),
      Longitude = as.numeric(Longitude),
      Latitude = as.numeric(Latitude)
    )
}

parse_arcgis_batch <- function(txt) {
  parsed_fast <- tryCatch(fromJSON(txt), error = function(e) NULL)

  if (
    !is.null(parsed_fast) &&
    !is.null(parsed_fast$features) &&
    is.data.frame(parsed_fast$features) &&
    "attributes" %in% names(parsed_fast$features)
  ) {
    attrs <- parsed_fast$features$attributes
    geoms <- parsed_fast$features$geometry
    
    # Extract the GPS coordinates from the spatial JSON
    if (!is.null(geoms) && "x" %in% names(geoms) && "y" %in% names(geoms)) {
      attrs$Longitude <- geoms$x
      attrs$Latitude <- geoms$y
    }
    return(coerce_311_schema(attrs))
  }

  parsed_safe <- tryCatch(fromJSON(txt, simplifyVector = FALSE), error = function(e) NULL)

  if (!is.null(parsed_safe) && !is.null(parsed_safe$features) && length(parsed_safe$features) > 0) {
    rows <- purrr::map_dfr(parsed_safe$features, function(feature) {
      attr <- feature$attributes %||% list()
      geom <- feature$geometry %||% list()

      tibble(
        SRType = if (is.null(attr$SRType)) NA_character_ else as.character(attr$SRType),
        SRStatus = if (is.null(attr$SRStatus)) NA_character_ else as.character(attr$SRStatus),
        CreatedDate = if (is.null(attr$CreatedDate)) NA_real_ else as.numeric(attr$CreatedDate),
        Longitude = if (is.null(geom$x)) NA_real_ else as.numeric(geom$x),
        Latitude = if (is.null(geom$y)) NA_real_ else as.numeric(geom$y)
      )
    })
    return(coerce_311_schema(rows))
  }

  empty_311_tbl
}

# 1. SETUP: Authenticate Census API
census_key <- trimws(Sys.getenv("CENSUS_API_KEY"))
if (identical(census_key, "")) stop("CENSUS_API_KEY is not set.")
Sys.setenv(CENSUS_API_KEY = census_key)

# 2. EXTRACT: Fetch Spatial Boundaries (CSAs)
print("Loading Local CSA Boundaries...")
# Accept either the canonical name or the original upload name
geojson_path <- if (file.exists("csa_boundaries.geojson")) {
  "csa_boundaries.geojson"
} else if (file.exists("csa_boundaries_geojson__1_.geojson")) {
  "csa_boundaries_geojson__1_.geojson"
} else {
  stop("Missing CSA boundaries GeoJSON. Expected csa_boundaries.geojson in repo root.")
}

csa_boundaries <- st_read(geojson_path, quiet = TRUE) %>%
  st_transform(crs = 4326)

# Auto-detect BNIA's CSA name column (Community, CSA2010, CSA2020, or Name)
csa_col <- names(csa_boundaries)[str_detect(str_to_lower(names(csa_boundaries)), "^community$|csa2010|csa2020|csa_name|^name$")][1]
if(is.na(csa_col)) stop("Could not detect the CSA name column in the geojson.")

csa_boundaries <- csa_boundaries %>%
  rename(CSA = !!sym(csa_col)) %>%
  select(CSA, geometry) %>%
  mutate(
    CSA = trimws(CSA),
    CSA_key = normalize_name(CSA)
  )

# 3. EXTRACT & TRANSFORM: Historical 311 Environmental Hazards
print("Fetching historical 311 data with GPS coordinates...")

fetch_arcgis_layer <- function(service_url, out_fields) {
  meta_response <- GET(service_url, query = list(f = "json"), timeout(60))
  stop_for_status(meta_response)

  meta_text <- content(meta_response, "text", encoding = "UTF-8")
  meta <- tryCatch(fromJSON(meta_text), error = function(e) NULL)

  page_size <- if (!is.null(meta$maxRecordCount)) meta$maxRecordCount else 2000L
  page_size <- min(as.integer(page_size), 2000L)
  offset <- 0L
  batches <- list()

  repeat {
    response <- GET(
      service_url,
      query = list(
        where = "1=1",
        outFields = paste(out_fields, collapse = ","),
        returnGeometry = "true", # FORCE SERVER TO SEND GPS POINTS
        outSR = "4326",
        orderByFields = "RowID ASC",
        resultOffset = offset,
        resultRecordCount = page_size,
        f = "json"
      ),
      timeout(120)
    )
    stop_for_status(response)
    txt <- content(response, "text", encoding = "UTF-8")
    batch <- parse_arcgis_batch(txt)

    if (nrow(batch) == 0) break
    batches[[length(batches) + 1L]] <- batch
    if (!str_detect(txt, '"exceededTransferLimit"\\s*:\\s*true')) break
    offset <- offset + nrow(batch)
  }

  if (length(batches) == 0) return(empty_311_tbl)
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

three11_batches <- vector("list", length(three11_sources))
names(three11_batches) <- names(three11_sources)

for (year_label in names(three11_sources)) {
  print(paste("  Downloading 311 records for", year_label, "..."))
  three11_batches[[year_label]] <- fetch_arcgis_layer(
    three11_sources[[year_label]],
    out_fields = c("SRType", "SRStatus", "CreatedDate") # Removed Neighborhood string
  )
}

raw_311 <- dplyr::bind_rows(three11_batches) %>% coerce_311_schema()

# The Spatial Intersection: Map exact GPS points to BNIA CSA Polygons
csa_311_clean <- raw_311 %>%
  filter(!is.na(CreatedDate), !is.na(Longitude), !is.na(Latitude)) %>%
  mutate(Year = lubridate::year(lubridate::as_datetime(CreatedDate / 1000, tz = "UTC"))) %>%
  st_as_sf(coords = c("Longitude", "Latitude"), crs = 4326) %>%
  st_join(csa_boundaries, join = st_intersects) %>%
  st_drop_geometry() %>%
  filter(!is.na(CSA), Year >= 2016, Year <= 2023, SRStatus != "Closed (Duplicate)") %>%
  transmute(
    CSA = CSA,
    CSA_key = CSA_key,
    Category = case_when(
      str_detect(str_to_lower(SRType), "rat|rodent") ~ "rt",
      str_detect(str_to_lower(SRType), "trash|dumping") ~ "dp",
      str_detect(str_to_lower(SRType), "water|sewer") ~ "ws",
      TRUE ~ "other"
    ),
    Year = Year
  ) %>%
  filter(Category != "other")

csa_311_yearly <- csa_311_clean %>%
  group_by(CSA, CSA_key, Category, Year) %>%
  summarize(Total = n(), .groups = "drop")

total_hazards <- csa_311_yearly %>%
  group_by(CSA, CSA_key, Year) %>%
  summarize(Total = sum(Total), .groups = "drop") %>%
  mutate(Category = "hz")

all_311_yearly <- bind_rows(csa_311_yearly, total_hazards)

complete_grid <- tidyr::crossing(
  csa_boundaries %>% st_drop_geometry() %>% select(CSA, CSA_key),
  Category = c("rt", "dp", "ws", "hz"),
  Year = 2016:2023
)

csa_311_arrays <- complete_grid %>%
  left_join(all_311_yearly, by = c("CSA", "CSA_key", "Category", "Year")) %>%
  mutate(Total = replace_na(Total, 0)) %>%
  arrange(CSA, Category, Year) %>%
  group_by(CSA, CSA_key, Category) %>%
  summarize(yearly_array = list(as.numeric(Total)), .groups = "drop") %>%
  pivot_wider(names_from = Category, values_from = yearly_array)

# 4. EXTRACT & TRANSFORM: ACS Demographic Baselines
print("Fetching and processing Census data...")
acs_vars <- c(Poverty = "B17001_002", Total_Pop = "B17001_001")

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

csa_acs_summary <- st_intersection(
  csa_boundaries %>% select(CSA, CSA_key),
  baltimore_acs
) %>%
  mutate(intersect_area = st_area(geometry)) %>%
  st_drop_geometry() %>%
  group_by(CSA, CSA_key) %>%
  summarize(
    pv = round(weighted.mean(pv, as.numeric(intersect_area), na.rm = TRUE), 1),
    .groups = "drop"
  )

# 5. MERGE: Combine Data
base_data_path <- "data.json"

if (file.exists(base_data_path)) {
  current_data_raw <- read_json(base_data_path, simplifyVector = FALSE)

  # ── GUARD: data.json must be a named object, not a plain array ──────────────
  # If the file is a plain array (legacy format), abort with a clear message.
  # The correct format is: {"Neighborhood Name": {"hi": 84, ...}, ...}
  if (is.null(names(current_data_raw))) {
    stop(paste0(
      "data.json must be a named object keyed by Neighborhood. ",
      "Example: {\"Canton\": {\"hi\": 84, \"le\": 80, ...}}"
    ))
  }

  # Drop any NULL-named entries (e.g. jail placeholder)
  current_data_raw <- current_data_raw[nchar(names(current_data_raw)) > 0]

  current_data <- tibble(
    CSA = names(current_data_raw),
    data = unname(current_data_raw)
  ) %>%
    unnest_wider(data) %>%
    mutate(CSA_key = normalize_name(CSA))
} else {
  stop("Missing initial data.json to use as a base.")
}

final_dashboard_data <- current_data %>%
  select(-any_of(c("pv", "rt", "dp", "ws", "hz"))) %>%
  left_join(csa_acs_summary %>% select(CSA_key, pv), by = "CSA_key") %>%
  left_join(csa_311_arrays %>% select(CSA_key, rt, dp, ws, hz), by = "CSA_key") %>%
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
json_source <- final_dashboard_data %>% select(-CSA_key)

json_ready_data <- split(json_source, json_source$CSA) %>%
  map(~ .x %>% select(-CSA) %>% as.list() %>% map(~ .x[[1]]))

write_json(json_ready_data, "data.json", auto_unbox = TRUE, pretty = TRUE)
print("Pipeline Complete! Longitudinal CSA data.json updated.")
