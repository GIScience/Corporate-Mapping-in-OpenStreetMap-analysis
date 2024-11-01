install.packages(c("sf"))
install.packages(c("spdep"))
install.packages(c("geojsonsf"))

library(spdep)
library(sf)
library(geojsonsf)
library(glue)

# List of country ISO A3 codes
country_iso_a3_list <- c(
    # 'BRA', 'USA', 'IND', 'IDN',
    'AGO', 'GRC', 'KHM', 'UKR',
    'PHL', 'URY', 'ALB', 'CHL',
    'HTI', 'KWT', 'QAT', 'MAR',
    'ROU', 'VEN', 'COL', 'IRQ',
    'MEX', 'SRB', 'VNM', 'MMR',
    'TCD', 'ZAF', 'DOM', 'PER',
    'MYS', 'THA', 'ARE', 'ECU',
    'IRN', 'PAK', 'TZA', 'EGY'
)  # Add more country codes as needed


# Initialize an empty data frame to store results
results_df <- data.frame(
    name = character(),
    leesL = numeric(),
    leesL_expectation = numeric(),
    leesL_pvalue = numeric(),
    alternative = character(),
    stringsAsFactors = FALSE
)

alternative = "two.sided"

# Loop through each country code
for (country_iso_a3 in country_iso_a3_list) {
    print(country_iso_a3)

    input_path =  glue("./{country_iso_a3}_newData_prepped.geojson")

    # Read the data
    data <- geojson_sf(input_path)
    data <- st_transform(data, 3857)

    # Define Neighbors
    country_nbQueen <- poly2nb(data, queen = TRUE)
    neighbor_counts <- card(country_nbQueen)

    # Filter out polygons without neighbors
    data <- data[neighbor_counts > 0, ]

    # Re-define Neighbors after filtering
    country_nbQueen <- poly2nb(data, queen = TRUE)
    country_lwQueen <- nb2listw(country_nbQueen, style = "W", zero.policy = TRUE)

    # Conduct Lee test
    lee_result <- lee.test(
        x = data$normalized_difference_avg_monthly_edits_corporate,
        y = data$normalized_difference_avg_monthly_edits_non_corporate,
        listw = country_lwQueen,
        zero.policy = TRUE,
        alternative = alternative,
        na.action = na.fail
    )

    # Print the result for the current country
    print(lee_result)

    # Append the result to the results data frame
    results_df <- rbind(results_df, data.frame(
        name = country_iso_a3,
        leesL = lee_result$estimate[1],
        leesL_expectation = lee_result$estimate[2],
        leesL_pvalue = lee_result$p.value,
        alternative = alternative,
        stringsAsFactors = FALSE
    ))
}

# Write the combined results to a CSV file
write.csv(results_df, "./combined_lee_stats.csv", row.names = FALSE)
