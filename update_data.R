library(tidyverse)
library(sf)
library(jsonlite)
library(tidycensus)
library(httr)

print("Starting Baltimore Health Data Pipeline...")

# 1. SETUP: Authenticate Census API
census_api_key(Sys.getenv("CENSUS_API_KEY"))

# 2. EXTRACT: Fetch Spatial Boundaries (NSAs)
print("Fetching NSA Boundaries...")
nsa_url <- "https://opendata.baltimorecity.gov/egis/rest/services/Hosted/Neighborhood_Statistical_Areas/FeatureServer/0/query?outFields=*&where=1%3D1&f=geojson"

# The Bulletproof Download: Mimic a browser and fetch the raw data securely
response <- GET(nsa_url, add_headers(`User-Agent` = "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"))

# Stop the script with a clear message if the server blocks us (so we don't get corrupt files)
stop_for_status(response) 

# Save the pure, raw JSON content to the server's hard drive
writeBin(content(response, "raw"), "nsa_boundaries.geojson")

# Read the newly saved local file
nsa_boundaries <- st_read("nsa_boundaries.geojson", quiet = TRUE) %>%
  st_transform(crs = 4326) %>%
  select(Neighborhood = Name, geometry) %>%
  mutate(Neighborhood = trimws(Neighborhood))

# 3. EXTRACT & TRANSFORM: Historical 311 Environmental Hazards
print("Fetching historical 311 data...")
three11_url <- "https://data.baltimorecity.gov/resource/ni4d-8w7k.csv?$where=createddate>='2016-01-01T00:00:00'%20AND%20createddate<='2023-12-31T23:59:59'&$limit=1000000"
raw_311 <- read_csv(three11_url)

nsa_311_clean <- raw_311 %>%
  filter(srstatus != "Closed (Duplicate)", !is.na(latitude), !is.na(longitude)) %>%
  mutate(
    latitude = as.numeric(latitude),
    longitude = as.numeric(longitude),
    Year = as.numeric(substr(createddate, 1, 4))
  ) %>%
  st_as_sf(coords = c("longitude", "latitude"), crs = 4326) %>%
  st_join(nsa_boundaries, join = st_intersects) %>%
  st_drop_geometry() %>%
  filter(!is.na(Neighborhood)) %>%
  mutate(
    Category = case_when(
      str_detect(str_to_lower(srtype), "rat|rodent") ~ "rt",
      str_detect(str_to_lower(srtype), "trash|dumping") ~ "dp",
      str_detect(str_to_lower(srtype), "water|sewer") ~ "ws",
      TRUE ~ "other"
    )
  ) %>%
  filter(Category != "other")

nsa_311_yearly <- nsa_311_clean %>%
  group_by(Neighborhood, Category, Year) %>%
  summarize(Total = n(), .groups = 'drop')

total_hazards <- nsa_311_yearly %>%
  group_by(Neighborhood, Year) %>%
  summarize(Total = sum(Total), .groups = 'drop') %>%
  mutate(Category = "hz")

all_311_yearly <- bind_rows(nsa_311_yearly, total_hazards)

complete_grid <- expand_grid(
  Neighborhood = unique(nsa_boundaries$Neighborhood),
  Category = c("rt", "dp", "ws", "hz"),
  Year = 2016:2023
)

nsa_311_arrays <- complete_grid %>%
  left_join(all_311_yearly, by = c("Neighborhood", "Category", "Year")) %>%
  mutate(Total = replace_na(Total, 0)) %>%
  arrange(Neighborhood, Category, Year) %>%
  group_by(Neighborhood, Category) %>%
  summarize(yearly_array = list(Total), .groups = 'drop') %>%
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
  mutate(pv = round((PovertyE / Total_PopE) * 100, 1)) %>%
  select(GEOID, pv, geometry) %>%
  mutate(pv = replace_na(pv, 0))

nsa_acs_summary <- st_intersection(nsa_boundaries, baltimore_acs) %>%
  mutate(intersect_area = st_area(geometry)) %>%
  st_drop_geometry() %>%
  group_by(Neighborhood) %>%
  summarize(pv = round(weighted.mean(pv, as.numeric(intersect_area), na.rm = TRUE), 1))

# 5. MERGE: Combine Data
base_data_path <- "data.json"
if(file.exists(base_data_path)) {
  current_data <- read_json(base_data_path, simplifyVector = TRUE) %>%
    as_tibble(rownames = "Neighborhood")
} else {
  stop("Missing initial data.json to use as a base.")
}

final_dashboard_data <- current_data %>%
  select(-any_of(c("pv", "rt", "dp", "ws", "hz"))) %>%
  left_join(nsa_acs_summary, by = "Neighborhood") %>%
  left_join(nsa_311_arrays, by = "Neighborhood") %>%
  mutate(pv = replace_na(pv, 0)) %>%
  mutate(across(c(rt, dp, ws, hz), ~ lapply(., function(x) {
    if(is.null(x) || all(is.na(x))) rep(0, 8) else x
  })))

# 6. LOAD: Write to JSON
print("Formatting and writing output...")
json_ready_data <- final_dashboard_data %>%
  column_to_rownames(var = "Neighborhood") %>%
  as.list() %>%
  purrr::transpose()

write_json(json_ready_data, "data.json", auto_unbox = TRUE, pretty = TRUE)
print("Pipeline Complete! Longitudinal data.json updated.")
