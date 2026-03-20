library(tidyverse)
library(sf)
library(jsonlite)
library(tidycensus)

print("Starting Baltimore Health Data Pipeline...")

# 1. SETUP: Authenticate Census API
# GitHub Actions will securely pass this key from your repository secrets
census_api_key(Sys.getenv("CENSUS_API_KEY"))

# 2. EXTRACT: Fetch Spatial Boundaries (NSAs)
print("Fetching NSA Boundaries...")
nsa_url <- "https://opendata.baltimorecity.gov/egis/rest/services/Hosted/Neighborhood_Statistical_Areas/FeatureServer/0/query?outFields=*&where=1%3D1&f=geojson"
nsa_boundaries <- st_read(nsa_url, quiet = TRUE) %>%
  st_transform(crs = 4326) %>%
  select(Neighborhood = Name, geometry) %>%
  mutate(Neighborhood = trimws(Neighborhood))

# 3. EXTRACT & TRANSFORM: Historical 311 Environmental Hazards
print("Fetching historical 311 data (this may take a few minutes)...")

# Pull exactly 2016 through 2023 to match your JavaScript slider years
# Switch the API endpoint from .json to .csv, and remove all spaces so the URL doesn't break
three11_url <- "https://data.baltimorecity.gov/resource/ni4d-8w7k.csv?$where=createddate>='2016-01-01T00:00:00'%20AND%20createddate<='2023-12-31T23:59:59'"

# Use the built-in tidyverse CSV reader instead of RSocrata
raw_311 <- read_csv(three11_url)

nsa_311_clean <- raw_311 %>%
  filter(srstatus != "Closed (Duplicate)", !is.na(latitude), !is.na(longitude)) %>%
  mutate(
    latitude = as.numeric(latitude),
    longitude = as.numeric(longitude),
    Year = as.numeric(substr(createddate, 1, 4)) # Extract the exact year
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

# Aggregate by Neighborhood, Category, and Year
nsa_311_yearly <- nsa_311_clean %>%
  group_by(Neighborhood, Category, Year) %>%
  summarize(Total = n(), .groups = 'drop')

# Calculate the Composite Total Hazards (hz) per year
total_hazards <- nsa_311_yearly %>%
  group_by(Neighborhood, Year) %>%
  summarize(Total = sum(Total), .groups = 'drop') %>%
  mutate(Category = "hz")

# Combine specific categories with the composite totals
all_311_yearly <- bind_rows(nsa_311_yearly, total_hazards)

# ALIGNMENT MATRIX: We must ensure every neighborhood has a value for EVERY year (even if 0)
# This prevents arrays from being too short and breaking the JavaScript slider
complete_grid <- expand_grid(
  Neighborhood = unique(nsa_boundaries$Neighborhood),
  Category = c("rt", "dp", "ws", "hz"),
  Year = 2016:2023
)

nsa_311_arrays <- complete_grid %>%
  left_join(all_311_yearly, by = c("Neighborhood", "Category", "Year")) %>%
  mutate(Total = replace_na(Total, 0)) %>%
  arrange(Neighborhood, Category, Year) %>%
  
  # NEST THE DATA: Collapse the years into an array (list) for each category
  group_by(Neighborhood, Category) %>%
  summarize(yearly_array = list(Total), .groups = 'drop') %>%
  
  # PIVOT: Turn categories into columns
  pivot_wider(names_from = Category, values_from = yearly_array)

# 4. EXTRACT & TRANSFORM: ACS Demographic Baselines
print("Fetching and processing Census data...")
# Variables: B17001_002 (Poverty), B23025_005 (Unemployed), B15003_017+ (HS Grad proxy)
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

# Spatial interpolation: Map Census Tracts to NSAs using geometric intersection
nsa_acs_summary <- st_intersection(nsa_boundaries, baltimore_acs) %>%
  mutate(intersect_area = st_area(geometry)) %>%
  st_drop_geometry() %>%
  group_by(Neighborhood) %>%
  # Calculate area-weighted poverty average for the neighborhood
  summarize(pv = round(weighted.mean(pv, as.numeric(intersect_area), na.rm = TRUE), 1))

# 5. MERGE: Combine Data and Preserve Static Baselines
base_data_path <- "data.json"
if(file.exists(base_data_path)) {
  current_data <- read_json(base_data_path, simplifyVector = TRUE) %>%
    as_tibble(rownames = "Neighborhood")
} else {
  stop("Missing initial data.json to use as a base.")
}

final_dashboard_data <- current_data %>%
  # Remove the old columns so we can replace them with live data
  select(-any_of(c("pv", "rt", "dp", "ws", "hz"))) %>%
  # Join the static Census data
  left_join(nsa_acs_summary, by = "Neighborhood") %>%
  # Join the longitudinal 311 array data
  left_join(nsa_311_arrays, by = "Neighborhood") %>%
  
  # Handle missing data cleanly
  # 1. Standard replacement for the single-value Census poverty variable
  mutate(pv = replace_na(pv, 0)) %>%
  # 2. Array replacement for the 311 longitudinal variables
  # If a neighborhood is completely missing from the 311 data, replace it with an array of 8 zeros
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
