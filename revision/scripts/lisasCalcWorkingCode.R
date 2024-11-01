# Required Libraries
library(spdep)
library(sf)
library(geojsonsf)
library(tmap)
library(tidyverse)
library(ggplot2)
library(ggpubr)
library(lwgeom)
library(rgeoboundaries)

countries <- gb_adm0() # download all countries
countries <- st_transform(countries, crs= st_crs(are))

# Function to run Local Moran's I and write results to a geopackage
runLocalMoransI <- function(geojson_path, country_name) {

  # Load Data
  are <- geojson_sf(geojson_path)
  are <- st_transform(are, 3857)
  #are <- are |> filter(!is.na(fid))

  tm_shape(are) + tm_polygons(col="normalized_difference_avg_monthly_edits_non_corporate")


  # Load and transform countries for background map

  #countries <- gb_adm0() # download all countries
  #countries <- st_transform(countries, crs= st_crs(are))

  #countries$shapeName
  are_poly <- countries |> filter(shapeName==country_name)


  # Define Neighbors
  are_nbQueen <- poly2nb(are, queen= TRUE)
  are_lwQueen <- nb2listw(are_nbQueen, style= "W", zero.policy = TRUE)

  # Function to calculate Local Moran's I
  getLocalMoranFactor <- function(x, listw, pval, quadr = "mean", p.adjust.method ="holm", method="normal", nb)
  {
    x <- na.omit(x)
    if(! (quadr %in% c("mean", "median", "pysal")))
      stop("getLocalMoranFactor: quadr needs to be one of the following values: 'mean', 'median', 'pysal'")
    if(! (method %in% c("normal", "perm")))
      stop("getLocalMoranFactor: method needs to be one of the following values: 'normal', 'perm'")
    if(! (p.adjust.method %in% p.adjust.methods))
      stop(paste("getLocalMoranFactor: p.adjust.method needs to be one of the following values:", p.adjust.methods))

    # adjust for multiple testing
    if(method == "normal")
      lMc <- spdep::localmoran(x, listw= listw)
    else
      lMc <- spdep::localmoran_perm(x, listw= listw)
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

  are_lisa <- getLocalMoranFactor(x= are$normalized_difference_avg_monthly_edits_non_corporate,
                                  listw= are_lwQueen,
                                  nb= are_nbQueen,
                                  pval=.05)

  # local Moran's I quadrants
  are$localM_non_corpo <- are_lisa[[1]]
  xtabs(~localM_non_corpo, data=are)
  # adjusted p-values
  are$localM_adjPval_non_corpo <- are_lisa[[2]]


  ggplot(are |> filter(localM_adjPval_non_corpo < 0.1), aes(x=localM_adjPval_non_corpo)) +
    geom_histogram(binwidth=0.005) + xlab("Adjusted p-values") +
    labs(title="Histogram for UAE, diff non corpo", subtitle="Only adjusted p-values < 0.1")

  are_lisa_corpo <- getLocalMoranFactor(x= are$normalized_difference_avg_monthly_edits_corporate,
                                        listw= are_lwQueen,
                                        nb= are_nbQueen,
                                        pval=.05)
  # local Moran's I quadrants
  are$localM_corpo <- are_lisa_corpo[[1]]
  xtabs(~localM_corpo, data=are)
  # adjusted p-values
  are$localM_adjPval_corpo <- are_lisa_corpo[[2]]


  # Confusion Matrix
  xtabs(~ localM_non_corpo + localM_corpo,
        data=are)

  xtabs(~ localM_non_corpo + localM_corpo,
        data=are) |>
    prop.table() |>
    round(4) * 100


  st_write(dplyr::select(are, !fid),
           dsn = "lisa_diff_non_corpo_corpo_NewData_missing countries.gpkg",
           layer = country_name,
           append = TRUE,
           quiet = TRUE)

}

setwd("C:/Users/Lilly/Downloads")

rextra1 = runLocalMoransI("../small_scale_newData/prepped_geopackages/TZA_newData_prepped.geojson", "TZA")
rextra2 = runLocalMoransI("../small_scale_newData/prepped_geopackages/ARE_newData_prepped.geojson", "ARE")


r2 = runLocalMoransI("../small_scale_newData/prepped_geopackages/AGO_newData_prepped.geojson", "AGO")
r3 = runLocalMoransI("../small_scale_newData/prepped_geopackages/ALB_newData_prepped.geojson", "ALB")
r4 = runLocalMoransI("../small_scale_newData/prepped_geopackages/BRA_newData_prepped.geojson", "BRA")
r5 = runLocalMoransI("../small_scale_newData/prepped_geopackages/col_newData_prepped.geojson", "COL")
r6 = runLocalMoransI("../small_scale_newData/prepped_geopackages/CHL_newData_prepped.geojson", "CHL")
r7 = runLocalMoransI("../small_scale_newData/prepped_geopackages/DOM_newData_prepped.geojson", "DOM")
r8 = runLocalMoransI("../small_scale_newData/prepped_geopackages/ECU_newData_prepped.geojson", "ECU")
r9 = runLocalMoransI("../small_scale_newData/prepped_geopackages/EGY_newData_prepped.geojson", "EGY")
r10 = runLocalMoransI("../small_scale_newData/prepped_geopackages/GRC_newData_prepped.geojson", "GRC")
r11 = runLocalMoransI("../small_scale_newData/prepped_geopackages/HTI_newData_prepped.geojson", "HTI")
r12 = runLocalMoransI("../small_scale_newData/prepped_geopackages/IND_newData_prepped.geojson", "IND")
r13 = runLocalMoransI("../small_scale_newData/prepped_geopackages/IRN_newData_prepped.geojson", "IRN")
r14 = runLocalMoransI("../small_scale_newData/prepped_geopackages/IRQ_newData_prepped.geojson", "IRQ")
r15 = runLocalMoransI("../small_scale_newData/prepped_geopackages/KHM_newData_prepped.geojson", "KHM")
r16 = runLocalMoransI("../small_scale_newData/prepped_geopackages/KWT_newData_prepped.geojson", "KWT")
r17 = runLocalMoransI("../small_scale_newData/prepped_geopackages/MAR_newData_prepped.geojson", "MAR")
r18 = runLocalMoransI("../small_scale_newData/prepped_geopackages/MEX_newData_prepped.geojson", "MEX")
r19 = runLocalMoransI("../small_scale_newData/prepped_geopackages/MMR_newData_prepped.geojson", "MMR")
r20 = runLocalMoransI("../small_scale_newData/prepped_geopackages/MYS_newData_prepped.geojson", "MYS")
r21 = runLocalMoransI("../small_scale_newData/prepped_geopackages/PAK_newData_prepped.geojson", "PAK")
r22 = runLocalMoransI("../small_scale_newData/prepped_geopackages/PER_newData_prepped.geojson", "PER")
r23= runLocalMoransI("../small_scale_newData/prepped_geopackages/PHL_newData_prepped.geojson", "PHL")
r24 = runLocalMoransI("../small_scale_newData/prepped_geopackages/QAT_newData_prepped.geojson", "QAT")
r25 = runLocalMoransI("../small_scale_newData/prepped_geopackages/ROU_newData_prepped.geojson", "ROU")
r26 = runLocalMoransI("../small_scale_newData/prepped_geopackages/SRB_newData_prepped.geojson", "SRB")
r27 = runLocalMoransI("../small_scale_newData/prepped_geopackages/TCD_newData_prepped.geojson", "TCD")
r28 = runLocalMoransI("../small_scale_newData/prepped_geopackages/THA_newData_prepped.geojson", "THA")
r29 = runLocalMoransI("../small_scale_newData/prepped_geopackages/UKR_newData_prepped.geojson", "UKR")
r30 = runLocalMoransI("../small_scale_newData/prepped_geopackages/URY_newData_prepped.geojson", "URY")
r31 = runLocalMoransI("../small_scale_newData/prepped_geopackages/USA_newData_prepped.geojson", "USA")
r32 = runLocalMoransI("../small_scale_newData/prepped_geopackages/VEN_newData_prepped.geojson", "VEN")
r33 = runLocalMoransI("../small_scale_newData/prepped_geopackages/VNM_newData_prepped.geojson", "VNM")
r34 = runLocalMoransI("../small_scale_newData/prepped_geopackages/ZAF_newData_prepped.geojson", "ZAF")


