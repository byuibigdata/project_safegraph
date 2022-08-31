```python
#  weekly_patterns (start_date: "2021-07-12" end_date: "2021-07-18")
# "naics_code: 813110,"
# weekly_patterns function
query_sg_week = """query {
  search(
      filter: {
        address: { 
            city: "San Francisco", 
            region: "CA" 
        }
    }
    ){
    places {
      results(first: 500 after: "") {
        pageInfo { hasNextPage, endCursor}
        edges {
          node {
            weekly_patterns (start_date: "2019-01-07" end_date: "2019-01-13") {
              placekey
              parent_placekey
              location_name
              street_address
              city
              region
              postal_code
              iso_country_code
              date_range_start
              date_range_end
              raw_visit_counts
              raw_visitor_counts
              visits_by_each_hour {
                visits
              	}
              visits_by_day {
                visits
              }
              device_type
              poi_cbg
              visitor_home_cbgs
              visitor_home_aggregation
              visitor_daytime_cbgs
              visitor_country_of_origin
              distance_from_home
              median_dwell
              bucketed_dwell_times
              related_same_day_brand
              related_same_week_brand
            }
          }
        }
      }
    }
  }
}
"""


# Core API call

# %%
query_core = """
query {
  search(
      filter: {
        address: { 
            city: "San Francisco", 
            region: "CA" 
        }
    }
    ) {
    places {
      results(first: 500 after: "") {
      pageInfo {hasNextPage, endCursor}
      edges {
        node {
          safegraph_core {
          	placekey
            brands {
              brand_id
            }
            latitude
            longitude
            street_address
            city
            region
            postal_code
            parent_placekey
            location_name
            naics_code
            opened_on
            closed_on
            }
        	}
      	}
    	}
  	}
	}
}
"""
```