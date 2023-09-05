library(tidyverse)
library(sf)
library(jsonlite)

json_to_tibble <- function(x) {
    if(is.na(x))  return(x)
    parse_json(x) %>%
    enframe() %>%
    unnest(value)
}

bracket_to_tibble <- function(x){
    value <- str_replace_all(x, "\\[|\\]", "") %>%
        str_split(",", simplify = TRUE) %>%
        as.numeric()

    name <- seq_len(length(value))

    tibble::tibble(name = name, value = value)
}


dat <- read_csv("SafeGraph - Patterns and Core Data - Chipotle - July 2021/Core Places and Patterns Data/chipotle_core_poi_and_patterns.csv")

dat %>%
    slice(5:10) %>%
    pull(popularity_by_day)

dat_nest <- dat %>%
    slice(1:50) %>% # for the example in class.
    mutate(
        open_hours = map(open_hours, ~json_to_tibble(.x)),
        visits_by_day = map(visits_by_day, ~bracket_to_tibble(.x)),
        visitor_country_of_origin = map(
            visitor_country_of_origin, ~json_to_tibble(.x)),
        bucketed_dwell_times = map(
            bucketed_dwell_times, ~json_to_tibble(.x)),
        related_same_day_brand = map(
            related_same_day_brand, ~json_to_tibble(.x)),
        related_same_month_brand = map(
            related_same_month_brand, ~json_to_tibble(.x)),
        popularity_by_hour = map(
            popularity_by_hour, ~json_to_tibble(.x)),
        popularity_by_day = map(
            popularity_by_day, ~json_to_tibble(.x)),
        device_type = map(
            device_type, ~json_to_tibble(.x)),
        visitor_home_cbgs = map(
            visitor_home_cbgs, ~json_to_tibble(.x)),
        visitor_home_aggregation = map(
            visitor_home_aggregation, ~json_to_tibble(.x)),
        visitor_daytime_cbgs = map(
            visitor_daytime_cbgs, ~json_to_tibble(.x))
        )

dat_nest <- dat_nest %>%
    select(placekey, location_name, latitude, longitude, street_address,
        city, region, postal_code,
        raw_visit_counts:visitor_daytime_cbgs,
        parent_placekey, open_hours)

dat_nest %>%
    slice(1:5) %>%
    select(placekey, location_name, latitude,
        longitude, city, region, device_type)

dat %>%
    slice(1:5) %>%
    select(placekey, location_name, latitude,
        longitude, city, region, device_type) 

dat_nest %>% 
    slice(1:5) %>%
    filter(!is.na(device_type)) %>%
    select(placekey, location_name, latitude, longitude,
        city, region, device_type) %>%
    unnest(device_type) %>%
    filter(!is.na(name)) %>%
    pivot_wider(names_from = name, values_from = value)

dat_nest %>% 
    slice(1:5) %>%
    filter(!is.na(device_type)) %>%
    select(placekey, location_name, latitude, longitude,
        city, region, popularity_by_day) %>%
    unnest(popularity_by_day) %>%
    filter(!is.na(name)) %>%
    pivot_wider(names_from = name, values_from = value)

# This is a problem
dat_nest %>% 
    slice(1:5) %>%
    filter(!is.na(device_type)) %>%
    select(placekey, location_name, latitude, longitude, 
        city, region, device_type, popularity_by_day) %>%
    unnest(popularity_by_day) %>%
    filter(!is.na(name)) %>%
    pivot_wider(names_from = name, values_from = value) %>%
    unnest(device_type)
# notice the duplicate rows.

dat_all <- dat %>%
    mutate(
        open_hours = map(open_hours, ~json_to_tibble(.x)),
        visits_by_day = map(visits_by_day, ~bracket_to_tibble(.x)),
        visitor_country_of_origin = map(
            visitor_country_of_origin, ~json_to_tibble(.x)),
        bucketed_dwell_times = map(
            bucketed_dwell_times, ~json_to_tibble(.x)),
        related_same_day_brand = map(
            related_same_day_brand, ~json_to_tibble(.x)),
        related_same_month_brand = map(
            related_same_month_brand, ~json_to_tibble(.x)),
        popularity_by_hour = map(
            popularity_by_hour, ~json_to_tibble(.x)),
        popularity_by_day = map(
            popularity_by_day, ~json_to_tibble(.x)),
        device_type = map(
            device_type, ~json_to_tibble(.x)),
        visitor_home_cbgs = map(
            visitor_home_cbgs, ~json_to_tibble(.x)),
        visitor_home_aggregation = map(
            visitor_home_aggregation, ~json_to_tibble(.x)),
        visitor_daytime_cbgs = map(
            visitor_daytime_cbgs, ~json_to_tibble(.x))) %>%
    select(placekey, latitude, longitude, street_address,
        city, region, postal_code,
        raw_visit_counts:visitor_daytime_cbgs,
        parent_placekey, open_hours)

write_rds(dat_all, "chipotle_nested.rds")
