install.packages(c("sf"))
install.packages(c("spdep"))
install.packages(c("geojsonsf"))

library(spdep)
library(sf)
library(geojsonsf)
library(glue)


###################################
# calculate Lee's L for ARE
###################################

country_iso_a3 = 'ARE'
input_path = glue("./{country_iso_a3}_newData_prepped.geojson")
output_path = glue("./{country_iso_a3}_lee_stats.csv")

data <- geojson_sf(input_path)
data <- st_transform(data, 3857)

# Define Neighbors
country_nbQueen <- poly2nb(data, queen= TRUE)
country_lwQueen <- nb2listw(country_nbQueen, style= "W", zero.policy = TRUE)

alternative="two.sided"

lee_result <- lee.test(
  x = data$normalized_difference_avg_monthly_edits_corporate,
  y = data$normalized_difference_avg_monthly_edits_non_corporate,
  listw = country_lwQueen,
  zero.policy = TRUE,
  alternative = alternative,
  na.action = na.fail
)

print(lee_result)

result_df <- data.frame(
    name = country_iso_a3,
    leesL = lee_result$estimate[1],
    leesL_expectation = lee_result$estimate[2],
    leesL_pvalue = lee_result$p.value,
    alternative = alternative
  )

write.csv(result_df, output_path)


###################################
# calculate Lee's L for COL
###################################

country_iso_a3 = 'COL'
input_path = glue("./{country_iso_a3}_newData_prepped.geojson")
output_path = glue("./{country_iso_a3}_lee_stats.csv")

data <- geojson_sf(input_path)
data <- st_transform(data, 3857)

# Define Neighbors
country_nbQueen <- poly2nb(data, queen= TRUE)
country_lwQueen <- nb2listw(country_nbQueen, style= "W", zero.policy = TRUE)

alternative="two.sided"

lee_result <- lee.test(
  x = data$normalized_difference_avg_monthly_edits_corporate,
  y = data$normalized_difference_avg_monthly_edits_non_corporate,
  listw = country_lwQueen,
  zero.policy = TRUE,
  alternative = alternative,
  na.action = na.fail
)

print(lee_result)

result_df <- data.frame(
    name = country_iso_a3,
    leesL = lee_result$estimate[1],
    leesL_expectation = lee_result$estimate[2],
    leesL_pvalue = lee_result$p.value,
    alternative = alternative
  )

write.csv(result_df, output_path)


###################################
# calculate Lee's L for VNM
###################################

country_iso_a3 = 'VNM'
input_path = glue("./{country_iso_a3}_newData_prepped.geojson")
output_path = glue("./{country_iso_a3}_lee_stats.csv")

data <- geojson_sf(input_path)
data <- st_transform(data, 3857)

# Define Neighbors
country_nbQueen <- poly2nb(data, queen= TRUE)
country_lwQueen <- nb2listw(country_nbQueen, style= "W", zero.policy = TRUE)

alternative="two.sided"

lee_result <- lee.test(
  x = data$normalized_difference_avg_monthly_edits_corporate,
  y = data$normalized_difference_avg_monthly_edits_non_corporate,
  listw = country_lwQueen,
  zero.policy = TRUE,
  alternative = alternative,
  na.action = na.fail
)

print(lee_result)

result_df <- data.frame(
    name = country_iso_a3,
    leesL = lee_result$estimate[1],
    leesL_expectation = lee_result$estimate[2],
    leesL_pvalue = lee_result$p.value,
    alternative = alternative
  )

write.csv(result_df, output_path)


###################################
# calculate Lee's L for MEX
###################################

country_iso_a3 = 'MEX'
input_path = glue("./{country_iso_a3}_newData_prepped.geojson")
output_path = glue("./{country_iso_a3}_lee_stats.csv")

data <- geojson_sf(input_path)
data <- st_transform(data, 3857)

# Define Neighbors
country_nbQueen <- poly2nb(data, queen= TRUE)
country_lwQueen <- nb2listw(country_nbQueen, style= "W", zero.policy = TRUE)

alternative="two.sided"

lee_result <- lee.test(
  x = data$normalized_difference_avg_monthly_edits_corporate,
  y = data$normalized_difference_avg_monthly_edits_non_corporate,
  listw = country_lwQueen,
  zero.policy = TRUE,
  alternative = alternative,
  na.action = na.fail
)

print(lee_result)

result_df <- data.frame(
    name = country_iso_a3,
    leesL = lee_result$estimate[1],
    leesL_expectation = lee_result$estimate[2],
    leesL_pvalue = lee_result$p.value,
    alternative = alternative
  )

write.csv(result_df, output_path)


###################################
# calculate Lee's L for IND
# This didn't work!!!
# needs more than 250GB RAM.
###################################

country_iso_a3 = 'IND'
input_path = glue("./{country_iso_a3}_newData_prepped.geojson")
output_path = glue("./{country_iso_a3}_lee_stats.csv")

data <- geojson_sf(input_path)
data <- st_transform(data, 3857)

# Define Neighbors
country_nbQueen <- poly2nb(data, queen= TRUE)
country_lwQueen <- nb2listw(country_nbQueen, style= "W", zero.policy = TRUE)

alternative="two.sided"

lee_result <- lee.test(
  x = data$normalized_difference_avg_monthly_edits_corporate,
  y = data$normalized_difference_avg_monthly_edits_non_corporate,
  listw = country_lwQueen,
  zero.policy = TRUE,
  alternative = alternative,
  na.action = na.fail
)

print(lee_result)

result_df <- data.frame(
    name = country_iso_a3,
    leesL = lee_result$estimate[1],
    leesL_expectation = lee_result$estimate[2],
    leesL_pvalue = lee_result$p.value,
    alternative = alternative
  )

write.csv(result_df, output_path)


###################################
# other countries
###################################


