install.packages(c("sf"))
install.packages(c("spdep"))
install.packages(c("geojsonsf"))

library(spdep)
library(sf)
library(geojsonsf)
library(glue)


###################################################
# Bivariate Local Moran's I Function
###################################################
getLocalMoranFactor <- function(x, y, listw, pval, quadr = "mean", p.adjust.method ="holm", method="normal", nb)
{
    # adjust for multiple testing
    lMc <- spdep::localmoran_bv(
        x,
        y,
        listw = country_lwQueen,
        nsim = 499
    )
    lMc[,5] <-  p.adjustSP(lMc[,5], method = p.adjust.method, nb = nb)
    lMcQuadr <- attr(lMc, "quadr")

    lMcFac <- as.character(lMcQuadr[, quadr])
    # which values are significant
    idx <- which(lMc[,5]> pval)
    lMcFac[idx] <- "Not sign."
    lMcFac <- factor(lMcFac, levels = c("Not sign.", "Low-Low", "Low-High", "High-Low",  "High-High"))

    # set those with empty neighbors to NA
    idx <- which(card(nb)==0)
    lMcFac[idx] <- "Not sign."
    return(list(lMcFac, lMc[,5]))
}

####################################################
# calculate Bivariate Local Moran's I for ARE
####################################################
country_iso_a3 = 'ARE'
input_path = glue("./{country_iso_a3}_newData_prepped.geojson")
output_path = glue("./{country_iso_a3}_LISA_bv.gpkg")

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

# Calculate bivariate local Moran's I
data_lisa_bv <- getLocalMoranFactor(
    x= data$normalized_difference_avg_monthly_edits_corporate,
    y= data$normalized_difference_avg_monthly_edits_non_corporate,
    listw= country_lwQueen,
    nb= country_nbQueen,
    pval=.05
)

# local Moran's I quadrants
data$localM_bv <- data_lisa_bv[[1]]
xtabs(~localM_bv, data=data)
# adjusted p-values
data$localM_adjPval_bv <- data_lisa_bv[[2]]


# save output
st_write(dplyr::select(data, !fid),
    dsn = output_path,
    layer = country_iso_a3,
    append = TRUE,
    quiet = TRUE
)


####################################################
# calculate Bivariate Local Moran's I for COL
####################################################
country_iso_a3 = 'COL'
input_path = glue("./{country_iso_a3}_newData_prepped.geojson")
output_path = glue("./{country_iso_a3}_LISA_bv.gpkg")

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

# Calculate bivariate local Moran's I
data_lisa_bv <- getLocalMoranFactor(
    x= data$normalized_difference_avg_monthly_edits_corporate,
    y= data$normalized_difference_avg_monthly_edits_non_corporate,
    listw= country_lwQueen,
    nb= country_nbQueen,
    pval=.05
)

# local Moran's I quadrants
data$localM_bv <- data_lisa_bv[[1]]
xtabs(~localM_bv, data=data)
# adjusted p-values
data$localM_adjPval_bv <- data_lisa_bv[[2]]


# save output
st_write(dplyr::select(data, !fid),
    dsn = output_path,
    layer = country_iso_a3,
    append = TRUE,
    quiet = TRUE
)

####################################################
# calculate Bivariate Local Moran's I for VNM
####################################################
country_iso_a3 = 'VNM'
input_path = glue("./{country_iso_a3}_newData_prepped.geojson")
output_path = glue("./{country_iso_a3}_LISA_bv.gpkg")

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

# Calculate bivariate local Moran's I
data_lisa_bv <- getLocalMoranFactor(
    x= data$normalized_difference_avg_monthly_edits_corporate,
    y= data$normalized_difference_avg_monthly_edits_non_corporate,
    listw= country_lwQueen,
    nb= country_nbQueen,
    pval=.05
)

# local Moran's I quadrants
data$localM_bv <- data_lisa_bv[[1]]
xtabs(~localM_bv, data=data)
# adjusted p-values
data$localM_adjPval_bv <- data_lisa_bv[[2]]


# save output
st_write(dplyr::select(data, !fid),
    dsn = output_path,
    layer = country_iso_a3,
    append = TRUE,
    quiet = TRUE
)


####################################################
# calculate Bivariate Local Moran's I for MEX
####################################################
country_iso_a3 = 'MEX'
input_path = glue("./{country_iso_a3}_newData_prepped.geojson")
output_path = glue("./{country_iso_a3}_LISA_bv.gpkg")

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

# Calculate bivariate local Moran's I
data_lisa_bv <- getLocalMoranFactor(
    x= data$normalized_difference_avg_monthly_edits_corporate,
    y= data$normalized_difference_avg_monthly_edits_non_corporate,
    listw= country_lwQueen,
    nb= country_nbQueen,
    pval=.05
)

# local Moran's I quadrants
data$localM_bv <- data_lisa_bv[[1]]
xtabs(~localM_bv, data=data)
# adjusted p-values
data$localM_adjPval_bv <- data_lisa_bv[[2]]


# save output
st_write(dplyr::select(data, !fid),
    dsn = output_path,
    layer = country_iso_a3,
    append = TRUE,
    quiet = TRUE
)