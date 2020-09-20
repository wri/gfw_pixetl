from gfw_tile_prep.stages import rasterize, translate, upload_file, delete_file, info
from gfw_tile_prep.utils import str2bool
from parallelpipe import Stage
import csv
import logging
import argparse
import os

# parallel workers
WORKERS = 35

# tile specifications
TILE_SIZE = 400
PIXEL_SIZE = 0.00025

# local PG settings
HOST = "localhost"
PORT = 5432
DBNAME = "gadm"
DBUSER = "postgres"
PASSWORD = "postgres"
PG_CONN = "PG:dbname={} port={} host={} user={} password={}".format(
    DBNAME, PORT, HOST, DBUSER, PASSWORD
)

# data sources
SRC = {
    "GrossAnnualAbovegroundRemovalsCarbon": {
        "type": "raster",
        "src": "{protocol}/gfw2-data/climate/carbon_model/annual_removal_factor_AGC_all_forest_types/standard/20200824/{tile_id}_annual_removal_factor_AGC_Mg_ha_all_forest_types.tif",
        "s3_target": "{protocol}/gfw-files/flux_2_1_0/annual_removal_factor_AGC_all_forest_types/standard/{tile_id}.tif",
        "data_type": "Float32",
        "nodata": 0,
    },
    "GrossAnnualBelowgroundRemovalsCarbon": {
        "type": "raster",
        "src": "{protocol}/gfw2-data/climate/carbon_model/annual_removal_factor_BGC_all_forest_types/standard/20200824/{tile_id}_annual_removal_factor_BGC_Mg_ha_all_forest_types.tif",
        "s3_target": "{protocol}/gfw-files/flux_2_1_0/annual_removal_factor_BGC_all_forest_types/standard/{tile_id}.tif",
        "data_type": "Float32",
        "nodata": 0,
    },
    "GrossCumulAbovegroundRemovalsCo2": {
        "type": "raster",
        # "src": "{protocol}/gfw2-data/climate/carbon_model/gross_removals_AGCO2_all_forest_types/standard/per_hectare/20200824/{tile_id}_gross_removals_AGCO2_Mg_ha_all_forest_types_2001_19.tif",
        # "s3_target": "{protocol}/gfw-files/flux_2_1_0/gross_removals_AGCO2_all_forest_types/standard/{tile_id}.tif",
        # "src": "{protocol}/gfw2-data/climate/carbon_model/gross_removals_AGCO2_all_forest_types/maxgain/per_hectare/20200915/{tile_id}_gross_removals_AGCO2_Mg_ha_all_forest_types_2001_19_maxgain.tif",
        # "s3_target": "{protocol}/gfw-files/flux_2_1_0/gross_removals_AGCO2_all_forest_types/maxgain/{tile_id}.tif",
        # "src": "{protocol}/gfw2-data/climate/carbon_model/gross_removals_AGCO2_all_forest_types/biomass_swap/per_hectare/20200919/{tile_id}_gross_removals_AGCO2_Mg_ha_all_forest_types_2001_19_biomass_swap.tif",
        # "s3_target": "{protocol}/gfw-files/flux_2_1_0/gross_removals_AGCO2_all_forest_types/biomass_swap/{tile_id}.tif",
        # "src": "{protocol}/gfw2-data/climate/carbon_model/gross_removals_AGCO2_all_forest_types/US_removals/per_hectare/20200919/{tile_id}_gross_removals_AGCO2_Mg_ha_all_forest_types_2001_19_US_removals.tif",
        # "s3_target": "{protocol}/gfw-files/flux_2_1_0/gross_removals_AGCO2_all_forest_types/US_removals/{tile_id}.tif",
        # "src": "{protocol}/gfw2-data/climate/carbon_model/gross_removals_AGCO2_all_forest_types/no_primary_gain/per_hectare/20200918/{tile_id}_gross_removals_AGCO2_Mg_ha_all_forest_types_2001_19_no_primary_gain.tif",
        # "s3_target": "{protocol}/gfw-files/flux_2_1_0/gross_removals_AGCO2_all_forest_types/no_primary_gain/{tile_id}.tif",
        # "src": "{protocol}/gfw2-data/climate/carbon_model/gross_removals_AGCO2_all_forest_types/legal_Amazon_loss/per_hectare/20200920/{tile_id}_gross_removals_AGCO2_Mg_ha_all_forest_types_2001_19_Amazon_loss.tif",
        # "s3_target": "{protocol}/gfw-files/flux_2_1_0/gross_removals_AGCO2_all_forest_types/legal_Amazon_loss/{tile_id}.tif",
        "data_type": "Float32",
        "nodata": 0,
    },
    "GrossCumulBelowgroundRemovalsCo2": {
        "type": "raster",
        # "src": "{protocol}/gfw2-data/climate/carbon_model/gross_removals_BGCO2_all_forest_types/standard/per_hectare/20200824/{tile_id}_gross_removals_BGCO2_Mg_ha_all_forest_types_2001_19.tif",
        # "s3_target": "{protocol}/gfw-files/flux_2_1_0/gross_removals_BGCO2_all_forest_types/standard/{tile_id}.tif",
        "src": "{protocol}/gfw2-data/climate/carbon_model/gross_removals_BGCO2_all_forest_types/maxgain/per_hectare/20200915/{tile_id}_gross_removals_BGCO2_Mg_ha_all_forest_types_2001_19_maxgain.tif",
        "s3_target": "{protocol}/gfw-files/flux_2_1_0/gross_removals_BGCO2_all_forest_types/maxgain/{tile_id}.tif",
        # "src": "{protocol}/gfw2-data/climate/carbon_model/gross_removals_BGCO2_all_forest_types/biomass_swap/per_hectare/20200919/{tile_id}_gross_removals_BGCO2_Mg_ha_all_forest_types_2001_19_biomass_swap.tif",
        # "s3_target": "{protocol}/gfw-files/flux_2_1_0/gross_removals_BGCO2_all_forest_types/biomass_swap/{tile_id}.tif",
        # "src": "{protocol}/gfw2-data/climate/carbon_model/gross_removals_BGCO2_all_forest_types/US_removals/per_hectare/20200919/{tile_id}_gross_removals_BGCO2_Mg_ha_all_forest_types_2001_19_US_removals.tif",
        # "s3_target": "{protocol}/gfw-files/flux_2_1_0/gross_removals_BGCO2_all_forest_types/US_removals/{tile_id}.tif",
        # "src": "{protocol}/gfw2-data/climate/carbon_model/gross_removals_BGCO2_all_forest_types/no_primary_gain/per_hectare/20200918/{tile_id}_gross_removals_BGCO2_Mg_ha_all_forest_types_2001_19_no_primary_gain.tif",
        # "s3_target": "{protocol}/gfw-files/flux_2_1_0/gross_removals_BGCO2_all_forest_types/no_primary_gain/{tile_id}.tif",
        # "src": "{protocol}/gfw2-data/climate/carbon_model/gross_removals_BGCO2_all_forest_types/legal_Amazon_loss/per_hectare/20200920/{tile_id}_gross_removals_BGCO2_Mg_ha_all_forest_types_2001_19_Amazon_loss.tif",
        # "s3_target": "{protocol}/gfw-files/flux_2_1_0/gross_removals_BGCO2_all_forest_types/legal_Amazon_loss/{tile_id}.tif",
        "data_type": "Float32",
        "nodata": 0,
    },
    "NetFluxCo2e": {
        "type": "raster",
        # "src": "{protocol}/gfw2-data/climate/carbon_model/net_flux_all_forest_types_all_drivers/biomass_soil/standard/per_hectare/20200824/{tile_id}_net_flux_Mg_CO2e_ha_biomass_soil_2001_19.tif",
        # "s3_target": "{protocol}/gfw-files/flux_2_1_0/net_flux_all_forest_types_all_drivers/standard/{tile_id}.tif",
        # "src": "{protocol}/gfw2-data/climate/carbon_model/net_flux_all_forest_types_all_drivers/biomass_soil/maxgain/per_hectare/20200915/{tile_id}_net_flux_Mg_CO2e_ha_biomass_soil_2001_19_maxgain.tif",
        # "s3_target": "{protocol}/gfw-files/flux_2_1_0/net_flux_all_forest_types_all_drivers/maxgain/{tile_id}.tif",
        "src": "{protocol}/gfw2-data/climate/carbon_model/net_flux_all_forest_types_all_drivers/biomass_soil/no_shifting_ag/per_hectare/20200914/{tile_id}_net_flux_Mg_CO2e_ha_biomass_soil_2001_19_no_shifting_ag.tif",
        "s3_target": "{protocol}/gfw-files/flux_2_1_0/net_flux_all_forest_types_all_drivers/no_shifting_ag/{tile_id}.tif",
        # "src": "{protocol}/gfw2-data/climate/carbon_model/net_flux_all_forest_types_all_drivers/biomass_soil/convert_to_grassland/per_hectare/20200914/{tile_id}_net_flux_Mg_CO2e_ha_biomass_soil_2001_19_convert_to_grassland.tif",
        # "s3_target": "{protocol}/gfw-files/flux_2_1_0/net_flux_all_forest_types_all_drivers/convert_to_grassland/{tile_id}.tif",
        # "src": "{protocol}/gfw2-data/climate/carbon_model/net_flux_all_forest_types_all_drivers/biomass_soil/biomass_swap/per_hectare/20200919/{tile_id}_net_flux_Mg_CO2e_ha_biomass_soil_2001_19_biomass_swap.tif",
        # "s3_target": "{protocol}/gfw-files/flux_2_1_0/net_flux_all_forest_types_all_drivers/biomass_swap/{tile_id}.tif",
        # "src": "{protocol}/gfw2-data/climate/carbon_model/net_flux_all_forest_types_all_drivers/biomass_soil/US_removals/per_hectare/20200919/{tile_id}_net_flux_Mg_CO2e_ha_biomass_soil_2001_19_US_removals.tif",
        # "s3_target": "{protocol}/gfw-files/flux_2_1_0/net_flux_all_forest_types_all_drivers/US_removals/{tile_id}.tif",
        # "src": "{protocol}/gfw2-data/climate/carbon_model/net_flux_all_forest_types_all_drivers/biomass_soil/no_primary_gain/per_hectare/20200918/{tile_id}_net_flux_Mg_CO2e_ha_biomass_soil_2001_19_no_primary_gain.tif",
        # "s3_target": "{protocol}/gfw-files/flux_2_1_0/net_flux_all_forest_types_all_drivers/no_primary_gain/{tile_id}.tif",
        # "src": "{protocol}/gfw2-data/climate/carbon_model/net_flux_all_forest_types_all_drivers/biomass_soil/legal_Amazon_loss/per_hectare/20200920/{tile_id}_net_flux_Mg_CO2e_ha_biomass_soil_2001_19_legal_Amazon_loss.tif",
        # "s3_target": "{protocol}/gfw-files/flux_2_1_0/net_flux_all_forest_types_all_drivers/legal_Amazon_loss/{tile_id}.tif",
        "data_type": "Float32",
        "nodata": 0,
    },
    "AgcEmisYear": {
        "type": "raster",
        # "src": "{protocol}/gfw2-data/climate/carbon_model/carbon_pools/aboveground_carbon/loss_pixels/standard/20200824/{tile_id}_Mg_AGC_ha_emis_year.tif",
        # "s3_target": "{protocol}/gfw-files/flux_2_1_0/agc_emis_year/standard/{tile_id}.tif",
        # "src": "{protocol}/gfw2-data/climate/carbon_model/carbon_pools/aboveground_carbon/loss_pixels/maxgain/20200915/{tile_id}_Mg_AGC_ha_emis_year_maxgain.tif",
        # "s3_target": "{protocol}/gfw-files/flux_2_1_0/agc_emis_year/maxgain/{tile_id}.tif",
        # "src": "{protocol}/gfw2-data/climate/carbon_model/carbon_pools/aboveground_carbon/loss_pixels/biomass_swap/20200919/{tile_id}_Mg_AGC_ha_emis_year_biomass_swap.tif",
        # "s3_target": "{protocol}/gfw-files/flux_2_1_0/agc_emis_year/biomass_swap/{tile_id}.tif",
        # "src": "{protocol}/gfw2-data/climate/carbon_model/carbon_pools/aboveground_carbon/loss_pixels/US_removals/20200919/{tile_id}_Mg_AGC_ha_emis_year_US_removals.tif",
        # "s3_target": "{protocol}/gfw-files/flux_2_1_0/agc_emis_year/US_removals/{tile_id}.tif",
        # "src": "{protocol}/gfw2-data/climate/carbon_model/carbon_pools/aboveground_carbon/loss_pixels/no_primary_gain/20200918/{tile_id}_Mg_AGC_ha_emis_year_no_primary_gain.tif",
        # "s3_target": "{protocol}/gfw-files/flux_2_1_0/agc_emis_year/no_primary_gain/{tile_id}.tif",
        # "src": "{protocol}/gfw2-data/climate/carbon_model/carbon_pools/aboveground_carbon/loss_pixels/legal_Amazon_loss/20200920/{tile_id}_Mg_AGC_ha_emis_year_legal_Amazon_loss.tif",
        # "s3_target": "{protocol}/gfw-files/flux_2_1_0/agc_emis_year/legal_Amazon_loss/{tile_id}.tif",
        "data_type": "Float32",
        "nodata": 0,
    },
    "BgcEmisYear": {
        "type": "raster",
        "src": "{protocol}/gfw2-data/climate/carbon_model/carbon_pools/belowground_carbon/loss_pixels/standard/20200824/{tile_id}_Mg_BGC_ha_emis_year.tif",
        "s3_target": "{protocol}/gfw-files/flux_2_1_0/bgc_emis_year/standard/{tile_id}.tif",
        # "src": "{protocol}/gfw2-data/climate/carbon_model/carbon_pools/aboveground_carbon/loss_pixels/biomass_swap/20200107/{tile_id}_t_AGC_ha_emis_year_biomass_swap.tif",
        # "s3_target": "{protocol}/gfw-files/2018_update/agc_emis_year/biomass_swap/{tile_id}.tif",
        # "src": "{protocol}/gfw2-data/climate/carbon_model/carbon_pools/aboveground_carbon/loss_pixels/US_removals/20200107/{tile_id}_t_AGC_ha_emis_year_US_removals.tif",
        # "s3_target": "{protocol}/gfw-files/2018_update/agc_emis_year/US_removals/{tile_id}.tif",
        # "src": "{protocol}/gfw2-data/climate/carbon_model/carbon_pools/aboveground_carbon/loss_pixels/no_primary_gain/20200107/{tile_id}_t_AGC_ha_emis_year_no_primary_gain.tif",
        # "s3_target": "{protocol}/gfw-files/2018_update/agc_emis_year/no_primary_gain/{tile_id}.tif",
        # "src": "{protocol}/gfw2-data/climate/carbon_model/carbon_pools/aboveground_carbon/loss_pixels/legal_Amazon_loss/20200117/{tile_id}_t_AGC_ha_emis_year_legal_Amazon_loss.tif",
        # "s3_target": "{protocol}/gfw-files/2018_update/agc_emis_year/legal_Amazon_loss/{tile_id}.tif",
        # "src": "{protocol}/gfw2-data/climate/carbon_model/carbon_pools/aboveground_carbon/loss_pixels/Mekong_loss/20200210/{tile_id}_t_AGC_ha_emis_year_Mekong_loss.tif",
        # "s3_target": "{protocol}/gfw-files/2018_update/agc_emis_year/Mekong_loss/{tile_id}.tif",
        "data_type": "Float32",
        "nodata": 0,
    },
    "DeadwoodCarbonEmisYear": {
        "type": "raster",
        "src": "{protocol}/gfw2-data/climate/carbon_model/carbon_pools/deadwood_carbon/loss_pixels/standard/20200824/{tile_id}_Mg_deadwood_C_ha_emis_year_2000.tif",
        "s3_target": "{protocol}/gfw-files/flux_2_1_0/deadwood_emis_year/standard/{tile_id}.tif",
        # "src": "{protocol}/gfw2-data/climate/carbon_model/carbon_pools/aboveground_carbon/loss_pixels/biomass_swap/20200107/{tile_id}_t_AGC_ha_emis_year_biomass_swap.tif",
        # "s3_target": "{protocol}/gfw-files/2018_update/agc_emis_year/biomass_swap/{tile_id}.tif",
        # "src": "{protocol}/gfw2-data/climate/carbon_model/carbon_pools/aboveground_carbon/loss_pixels/US_removals/20200107/{tile_id}_t_AGC_ha_emis_year_US_removals.tif",
        # "s3_target": "{protocol}/gfw-files/2018_update/agc_emis_year/US_removals/{tile_id}.tif",
        # "src": "{protocol}/gfw2-data/climate/carbon_model/carbon_pools/aboveground_carbon/loss_pixels/no_primary_gain/20200107/{tile_id}_t_AGC_ha_emis_year_no_primary_gain.tif",
        # "s3_target": "{protocol}/gfw-files/2018_update/agc_emis_year/no_primary_gain/{tile_id}.tif",
        # "src": "{protocol}/gfw2-data/climate/carbon_model/carbon_pools/aboveground_carbon/loss_pixels/legal_Amazon_loss/20200117/{tile_id}_t_AGC_ha_emis_year_legal_Amazon_loss.tif",
        # "s3_target": "{protocol}/gfw-files/2018_update/agc_emis_year/legal_Amazon_loss/{tile_id}.tif",
        # "src": "{protocol}/gfw2-data/climate/carbon_model/carbon_pools/aboveground_carbon/loss_pixels/Mekong_loss/20200210/{tile_id}_t_AGC_ha_emis_year_Mekong_loss.tif",
        # "s3_target": "{protocol}/gfw-files/2018_update/agc_emis_year/Mekong_loss/{tile_id}.tif",
        "data_type": "Float32",
        "nodata": 0,
    },
    "LitterCarbonEmisYear": {
        "type": "raster",
        "src": "{protocol}/gfw2-data/climate/carbon_model/carbon_pools/litter_carbon/loss_pixels/standard/20200824/{tile_id}_Mg_litter_C_ha_emis_year_2000.tif",
        "s3_target": "{protocol}/gfw-files/flux_2_1_0/litter_emis_year/standard/{tile_id}.tif",
        # "src": "{protocol}/gfw2-data/climate/carbon_model/carbon_pools/aboveground_carbon/loss_pixels/biomass_swap/20200107/{tile_id}_t_AGC_ha_emis_year_biomass_swap.tif",
        # "s3_target": "{protocol}/gfw-files/2018_update/agc_emis_year/biomass_swap/{tile_id}.tif",
        # "src": "{protocol}/gfw2-data/climate/carbon_model/carbon_pools/aboveground_carbon/loss_pixels/US_removals/20200107/{tile_id}_t_AGC_ha_emis_year_US_removals.tif",
        # "s3_target": "{protocol}/gfw-files/2018_update/agc_emis_year/US_removals/{tile_id}.tif",
        # "src": "{protocol}/gfw2-data/climate/carbon_model/carbon_pools/aboveground_carbon/loss_pixels/no_primary_gain/20200107/{tile_id}_t_AGC_ha_emis_year_no_primary_gain.tif",
        # "s3_target": "{protocol}/gfw-files/2018_update/agc_emis_year/no_primary_gain/{tile_id}.tif",
        # "src": "{protocol}/gfw2-data/climate/carbon_model/carbon_pools/aboveground_carbon/loss_pixels/legal_Amazon_loss/20200117/{tile_id}_t_AGC_ha_emis_year_legal_Amazon_loss.tif",
        # "s3_target": "{protocol}/gfw-files/2018_update/agc_emis_year/legal_Amazon_loss/{tile_id}.tif",
        # "src": "{protocol}/gfw2-data/climate/carbon_model/carbon_pools/aboveground_carbon/loss_pixels/Mekong_loss/20200210/{tile_id}_t_AGC_ha_emis_year_Mekong_loss.tif",
        # "s3_target": "{protocol}/gfw-files/2018_update/agc_emis_year/Mekong_loss/{tile_id}.tif",
        "data_type": "Float32",
        "nodata": 0,
    },
    "SoilCarbonEmisYear": {
        "type": "raster",
        # "src": "{protocol}/gfw2-data/climate/carbon_model/carbon_pools/soil_carbon/loss_pixels/standard/20200824/{tile_id}_Mg_soil_C_ha_emis_year_2000.tif",
        # "s3_target": "{protocol}/gfw-files/flux_2_1_0/soil_emis_year/standard/{tile_id}.tif",
        # "src": "{protocol}/gfw2-data/climate/carbon_model/carbon_pools/soil_carbon/loss_pixels/maxgain/20200915/{tile_id}_Mg_soil_C_ha_emis_year_2000_maxgain.tif",
        # "s3_target": "{protocol}/gfw-files/flux_2_1_0/soil_emis_year/maxgain/{tile_id}.tif",
        # "src": "{protocol}/gfw2-data/climate/carbon_model/carbon_pools/soil_carbon/loss_pixels/biomass_swap/20200919/{tile_id}_Mg_soil_C_ha_emis_year_2000_biomass_swap.tif",
        # "s3_target": "{protocol}/gfw-files/flux_2_1_0/soil_emis_year/biomass_swap/{tile_id}.tif",
        # "src": "{protocol}/gfw2-data/climate/carbon_model/carbon_pools/soil_carbon/loss_pixels/US_removals/20200919/{tile_id}_Mg_soil_C_ha_emis_year_2000_US_removals.tif",
        # "s3_target": "{protocol}/gfw-files/flux_2_1_0/soil_emis_year/US_removals/{tile_id}.tif",
        # "src": "{protocol}/gfw2-data/climate/carbon_model/carbon_pools/soil_carbon/loss_pixels/no_primary_gain/20200918/{tile_id}_Mg_soil_C_ha_emis_year_2000_no_primary_gain.tif",
        # "s3_target": "{protocol}/gfw-files/flux_2_1_0/soil_emis_year/no_primary_gain/{tile_id}.tif",
        # "src": "{protocol}/gfw2-data/climate/carbon_model/carbon_pools/soil_carbon/loss_pixels/legal_Amazon_loss/20200920/{tile_id}_Mg_soil_C_ha_emis_year_2000_legal_Amazon_loss.tif",
        # "s3_target": "{protocol}/gfw-files/flux_2_1_0/soil_emis_year/legal_Amazon_loss/{tile_id}.tif",
        "data_type": "Float32",
        "nodata": 0,
    },
    "Agc2000": {
        "type": "raster",
        "src": "{protocol}/gfw2-data/climate/carbon_model/carbon_pools/aboveground_carbon/extent_2000/standard/20200824/{tile_id}_Mg_AGC_ha_2000.tif",
        "s3_target": "{protocol}/gfw-files/flux_2_1_0/agc_2000/standard/{tile_id}.tif",
        "data_type": "Float32",
        "nodata": 0,
    },
    "Bgc2000": {
        "type": "raster",
        "src": "{protocol}/gfw2-data/climate/carbon_model/carbon_pools/belowground_carbon/extent_2000/standard/20200824/{tile_id}_Mg_BGC_ha_2000.tif",
        "s3_target": "{protocol}/gfw-files/flux_2_1_0/bgc_2000/standard/{tile_id}.tif",
        "data_type": "Float32",
        "nodata": 0,
    },
    "DeadwoodCarbon2000": {
        "type": "raster",
        "src": "{protocol}/gfw2-data/climate/carbon_model/carbon_pools/deadwood_carbon/extent_2000/standard/20200824/{tile_id}_Mg_deadwood_C_ha_2000.tif",
        "s3_target": "{protocol}/gfw-files/flux_2_1_0/deadwood_carbon_2000/standard/{tile_id}.tif",
        "data_type": "Float32",
        "nodata": 0,
    },
    "LitterCarbon2000": {
        "type": "raster",
        "src": "{protocol}/gfw2-data/climate/carbon_model/carbon_pools/litter_carbon/extent_2000/standard/20200824/{tile_id}_Mg_litter_C_ha_2000.tif",
        "s3_target": "{protocol}/gfw-files/flux_2_1_0/litter_carbon_2000/standard/{tile_id}.tif",
        "data_type": "Float32",
        "nodata": 0,
    },
    "SoilCarbon2000": {
        "type": "raster",
        "src": "{protocol}/gfw2-data/climate/carbon_model/carbon_pools/soil_carbon/intermediate_full_extent/standard/20200724/{tile_id}_t_soil_C_ha_full_extent_2000.tif",
        "s3_target": "{protocol}/gfw-files/flux_2_1_0/soil_carbon_2000/standard/{tile_id}.tif",
        "data_type": "Float32",
        "nodata": 0,
    },
    "GrossEmissionsCo2OnlyCo2e": {
        "type": "raster",
        # "src": "{protocol}/gfw2-data/climate/carbon_model/gross_emissions/all_drivers/CO2_only/biomass_soil/standard/20200824/{tile_id}_gross_emis_CO2_only_all_drivers_Mg_CO2e_ha_biomass_soil_2001_19.tif",
        # "s3_target": "{protocol}/gfw-files/flux_2_1_0/gross_emissions_co2_only_co2e/standard/{tile_id}.tif",
        # "src": "{protocol}/gfw2-data/climate/carbon_model/gross_emissions/all_drivers/CO2_only/soil_only/standard/20200828/{tile_id}_gross_emis_CO2_only_all_drivers_Mg_CO2e_ha_soil_only_2001_19.tif",
        # "s3_target": "{protocol}/gfw-files/flux_2_1_0/gross_emissions_co2_only_co2e/soil_only/{tile_id}.tif",
        # "src": "{protocol}/gfw2-data/climate/carbon_model/gross_emissions/all_drivers/CO2_only/biomass_soil/maxgain/20200915/{tile_id}_gross_emis_CO2_only_all_drivers_Mg_CO2e_ha_biomass_soil_2001_19_maxgain.tif",
        # "s3_target": "{protocol}/gfw-files/flux_2_1_0/gross_emissions_co2_only_co2e/maxgain/{tile_id}.tif",
        "src": "{protocol}/gfw2-data/climate/carbon_model/gross_emissions/all_drivers/CO2_only/biomass_soil/no_shifting_ag/20200914/{tile_id}_gross_emis_CO2_only_all_drivers_Mg_CO2e_ha_biomass_soil_2001_19_no_shifting_ag.tif",
        "s3_target": "{protocol}/gfw-files/flux_2_1_0/gross_emissions_co2_only_co2e/no_shifting_ag/{tile_id}.tif",
        # "src": "{protocol}/gfw2-data/climate/carbon_model/gross_emissions/all_drivers/CO2_only/biomass_soil/convert_to_grassland/20200914/{tile_id}_gross_emis_CO2_only_all_drivers_Mg_CO2e_ha_biomass_soil_2001_19_convert_to_grassland.tif",
        # "s3_target": "{protocol}/gfw-files/flux_2_1_0/gross_emissions_co2_only_co2e/convert_to_grassland/{tile_id}.tif",
        # "src": "{protocol}/gfw2-data/climate/carbon_model/gross_emissions/all_drivers/CO2_only/biomass_soil/biomass_swap/20200919/{tile_id}_gross_emis_CO2_only_all_drivers_Mg_CO2e_ha_biomass_soil_2001_19_biomass_swap.tif",
        # "s3_target": "{protocol}/gfw-files/flux_2_1_0/gross_emissions_co2_only_co2e/biomass_swap/{tile_id}.tif",
        # "src": "{protocol}/gfw2-data/climate/carbon_model/gross_emissions/all_drivers/CO2_only/biomass_soil/US_removals/20200919/{tile_id}_gross_emis_CO2_only_all_drivers_Mg_CO2e_ha_biomass_soil_2001_19_US_removals.tif",
        # "s3_target": "{protocol}/gfw-files/flux_2_1_0/gross_emissions_co2_only_co2e/US_removals/{tile_id}.tif",
        # "src": "{protocol}/gfw2-data/climate/carbon_model/gross_emissions/all_drivers/CO2_only/biomass_soil/no_primary_gain/20200918/{tile_id}_gross_emis_CO2_only_all_drivers_Mg_CO2e_ha_biomass_soil_2001_19_no_primary_gain.tif",
        # "s3_target": "{protocol}/gfw-files/flux_2_1_0/gross_emissions_co2_only_co2e/no_primary_gain/{tile_id}.tif",
        # "src": "{protocol}/gfw2-data/climate/carbon_model/gross_emissions/all_drivers/CO2_only/biomass_soil/legal_Amazon_loss/20200920/{tile_id}_gross_emis_CO2_only_all_drivers_Mg_CO2e_ha_biomass_soil_2001_19_legal_Amazon_loss.tif",
        # "s3_target": "{protocol}/gfw-files/flux_2_1_0/gross_emissions_co2_only_co2e/legal_Amazon_loss/{tile_id}.tif",
        "data_type": "Float32",
        "nodata": 0,
    },
    "GrossEmissionsCo2eNonCo2": {
        "type": "raster",
        # "src": "{protocol}/gfw2-data/climate/carbon_model/gross_emissions/all_drivers/non_CO2/biomass_soil/standard/20200824/{tile_id}_gross_emis_non_CO2_all_drivers_Mg_CO2e_ha_biomass_soil_2001_19.tif",
        # "s3_target": "{protocol}/gfw-files/flux_2_1_0/gross_emissions_non_co2_co2e/standard/{tile_id}.tif",
        # "src": "{protocol}/gfw2-data/climate/carbon_model/gross_emissions/all_drivers/non_CO2/soil_only/standard/20200828/{tile_id}_gross_emis_non_CO2_all_drivers_Mg_CO2e_ha_soil_only_2001_19.tif",
        # "s3_target": "{protocol}/gfw-files/flux_2_1_0/gross_emissions_non_co2_co2e/soil_only/{tile_id}.tif",
        # "src": "{protocol}/gfw2-data/climate/carbon_model/gross_emissions/all_drivers/non_CO2/biomass_soil/maxgain/20200915/{tile_id}_gross_emis_non_CO2_all_drivers_Mg_CO2e_ha_biomass_soil_2001_19_maxgain.tif",
        # "s3_target": "{protocol}/gfw-files/flux_2_1_0/gross_emissions_non_co2_co2e/maxgain/{tile_id}.tif",
        "src": "{protocol}/gfw2-data/climate/carbon_model/gross_emissions/all_drivers/non_CO2/biomass_soil/no_shifting_ag/20200914/{tile_id}_gross_emis_non_CO2_all_drivers_Mg_CO2e_ha_biomass_soil_2001_19_no_shifting_ag.tif",
        "s3_target": "{protocol}/gfw-files/flux_2_1_0/gross_emissions_non_co2_co2e/no_shifting_ag/{tile_id}.tif",
        # "src": "{protocol}/gfw2-data/climate/carbon_model/gross_emissions/all_drivers/non_CO2/biomass_soil/convert_to_grassland/20200914/{tile_id}_gross_emis_non_CO2_all_drivers_Mg_CO2e_ha_biomass_soil_2001_19_convert_to_grassland.tif",
        # "s3_target": "{protocol}/gfw-files/flux_2_1_0/gross_emissions_non_co2_co2e/convert_to_grassland/{tile_id}.tif",
        # "src": "{protocol}/gfw2-data/climate/carbon_model/gross_emissions/all_drivers/non_CO2/biomass_soil/biomass_swap/20200919/{tile_id}_gross_emis_non_CO2_all_drivers_Mg_CO2e_ha_biomass_soil_2001_19_biomass_swap.tif",
        # "s3_target": "{protocol}/gfw-files/flux_2_1_0/gross_emissions_non_co2_co2e/biomass_swap/{tile_id}.tif",
        # "src": "{protocol}/gfw2-data/climate/carbon_model/gross_emissions/all_drivers/non_CO2/biomass_soil/US_removals/20200919/{tile_id}_gross_emis_non_CO2_all_drivers_Mg_CO2e_ha_biomass_soil_2001_19_US_removals.tif",
        # "s3_target": "{protocol}/gfw-files/flux_2_1_0/gross_emissions_non_co2_co2e/US_removals/{tile_id}.tif",
        # "src": "{protocol}/gfw2-data/climate/carbon_model/gross_emissions/all_drivers/non_CO2/biomass_soil/no_primary_gain/20200918/{tile_id}_gross_emis_non_CO2_all_drivers_Mg_CO2e_ha_biomass_soil_2001_19_no_primary_gain.tif",
        # "s3_target": "{protocol}/gfw-files/flux_2_1_0/gross_emissions_non_co2_co2e/no_primary_gain/{tile_id}.tif",
        # "src": "{protocol}/gfw2-data/climate/carbon_model/gross_emissions/all_drivers/non_CO2/biomass_soil/legal_Amazon_loss/20200920/{tile_id}_gross_emis_non_CO2_all_drivers_Mg_CO2e_ha_biomass_soil_2001_19_legal_Amazon_loss.tif",
        # "s3_target": "{protocol}/gfw-files/flux_2_1_0/gross_emissions_non_co2_co2e/legal_Amazon_loss/{tile_id}.tif",
        "data_type": "Float32",
        "nodata": 0,
    },
    "JplTropicsAbovegroundBiomassDensity2000": {
        "type": "raster",
        "src": "{protocol}/gfw2-data/climate/Saatchi_JPL_biomass/1km_2000/processed/20200107/{tile_id}_Mg_aboveground_biomass_ha_2000_JPL.tif",
        "s3_target": "{protocol}/gfw-files/2018_update/jpl_tropics_abovegroundbiomass_density_2000/Mg_ha-1/{tile_id}.tif",
        "data_type": "Float32",
        "nodata": 0
    },
    "StdevAnnualAbovegroundRemovalsCarbon": {
        "type": "raster",
        "src": "{protocol}/gfw2-data/climate/carbon_model/stdev_annual_removal_factor_AGC_all_forest_types/standard/20200831/{tile_id}_annual_removal_factor_stdev_AGC_Mg_ha_all_forest_types.tif",
        "s3_target": "{protocol}/gfw-files/flux_2_1_0/stdev_annual_removal_factor_AGC_all_forest_types/standard/{tile_id}.tif",
        "data_type": "Float32",
        "nodata": 0,
    },
    "StdevSoilCarbonEmisYear": {
        "type": "raster",
        "src": "{protocol}/gfw2-data/climate/carbon_model/stdev_soil_carbon_full_extent/standard/20200828/{tile_id}_Mg_soil_C_ha_stdev_full_extent_2000.tif",
        "s3_target": "{protocol}/gfw-files/flux_2_1_0/stdev_soil_carbon_full_extent/standard/{tile_id}.tif",
        "data_type": "Float32",
        "nodata": 0,
    },



    "FluxModelExtent": {
        "type": "raster",
        # "src": "{protocol}/gfw2-data/climate/carbon_model/model_extent/standard/20200824/{tile_id}_model_extent.tif",
        # "s3_target": "{protocol}/gfw-files/flux_2_1_0/model_extent/standard/{tile_id}.tif",
        "src": "{protocol}/gfw2-data/climate/carbon_model/model_extent/biomass_swap/20200919/{tile_id}_model_extent_biomass_swap.tif",
        "s3_target": "{protocol}/gfw-files/flux_2_1_0/model_extent/biomass_swap/{tile_id}.tif",
        "src": "{protocol}/gfw2-data/climate/carbon_model/model_extent/legal_Amazon_loss/20200920/{tile_id}_model_extent_legal_Amazon_loss.tif",
        "s3_target": "{protocol}/gfw-files/flux_2_1_0/model_extent/legal_Amazon_loss/{tile_id}.tif",
        "data_type": "Byte",
        "nodata": 0
    },
    "RemovalForestType": {
        "type": "raster",
        # "src": "{protocol}/gfw2-data/climate/carbon_model/removal_forest_type/standard/20200824/{tile_id}_removal_forest_type.tif",
        # "s3_target": "{protocol}/gfw-files/flux_2_1_0/removal_forest_type/standard/{tile_id}.tif",
        "src": "{protocol}/gfw2-data/climate/carbon_model/removal_forest_type/biomass_swap/20200919/{tile_id}_removal_forest_type_biomass_swap.tif",
        "s3_target": "{protocol}/gfw-files/flux_2_1_0/removal_forest_type/biomass_swap/{tile_id}.tif",
        "src": "{protocol}/gfw2-data/climate/carbon_model/removal_forest_type/US_removals/20200919/{tile_id}_removal_forest_type_US_removals.tif",
        "s3_target": "{protocol}/gfw-files/flux_2_1_0/removal_forest_type/US_removals/{tile_id}.tif",
        "src": "{protocol}/gfw2-data/climate/carbon_model/removal_forest_type/no_primary_gain/20200918/{tile_id}_removal_forest_type_no_primary_gain.tif",
        "s3_target": "{protocol}/gfw-files/flux_2_1_0/removal_forest_type/no_primary_gain/{tile_id}.tif",
        "src": "{protocol}/gfw2-data/climate/carbon_model/removal_forest_type/legal_Amazon_loss/20200920/{tile_id}_removal_forest_type_legal_Amazon_loss.tif",
        "s3_target": "{protocol}/gfw-files/flux_2_1_0/removal_forest_type/legal_Amazon_loss/{tile_id}.tif",
        "data_type": "Byte",
        "nodata": 0
    },
    "ForestAgeCategory": {
        "type": "raster",
        # "src": "{protocol}/gfw2-data/climate/carbon_model/forest_age_category_IPCC/standard/20200824/{tile_id}_forest_age_category_IPCC__1_young_2_mid_3_old.tif",
        # "s3_target": "{protocol}/gfw-files/flux_2_1_0/forest_age_category/standard/{tile_id}.tif",
        "src": "{protocol}/gfw2-data/climate/carbon_model/forest_age_category_IPCC/biomass_swap/20200919/{tile_id}_forest_age_category_IPCC__1_young_2_mid_3_old_biomass_swap.tif",
        "s3_target": "{protocol}/gfw-files/flux_2_1_0/forest_age_category/biomass_swap/{tile_id}.tif",
        "src": "{protocol}/gfw2-data/climate/carbon_model/forest_age_category_IPCC/no_primary_gain/20200918/{tile_id}_forest_age_category_IPCC__1_young_2_mid_3_old_no_primary_gain.tif",
        "s3_target": "{protocol}/gfw-files/flux_2_1_0/forest_age_category/no_primary_gain/{tile_id}.tif",
        "src": "{protocol}/gfw2-data/climate/carbon_model/forest_age_category_IPCC/legal_Amazon_loss/20200920/{tile_id}_forest_age_category_IPCC__1_young_2_mid_3_old_legal_Amazon_loss.tif",
        "s3_target": "{protocol}/gfw-files/flux_2_1_0/forest_age_category/legal_Amazon_loss/{tile_id}.tif",
        "data_type": "Byte",
        "nodata": 0
    },
    "PlantationsTypeFluxModel": {
        "type": "raster",
        "src": "{protocol}/gfw2-data/climate/carbon_model/other_emissions_inputs/plantation_type/standard/20200730/{tile_id}_plantation_type_oilpalm_woodfiber_other_unmasked.tif",
        "s3_target": "{protocol}/gfw-files/flux_2_1_0/plantation_type/standard/{tile_id}.tif",
        "data_type": "Byte",
        "nodata": 0
    },
    "PeatlandsExtentFluxModel": {
        "type": "raster",
        "src": "{protocol}/gfw2-data/climate/carbon_model/other_emissions_inputs/peatlands/processed/20200807/{tile_id}_peat_mask_processed.tif",
        "s3_target": "{protocol}/gfw-files/flux_2_1_0/peatlands_flux_extent/standard/{tile_id}.tif",
        "data_type": "Byte",
        "nodata": 0
    },
    # "ifl_primary": {
    #     "type": "raster",
    #     "src": "{protocol}/gfw2-data/climate/carbon_model/ifl_primary_merged/processed/20190905/{tile_id}_ifl_2000_primary_2001_merged.tif",
    #     "s3_target": "{protocol}/gfw-files/2018_update/ifl_primary/standard/{tile_id}.tif",
    #     "data_type": "Byte",
    #     "nodata": 0
    # },
    # "jpl_tropics_abovegroundbiomass_extent_2000": {
    #     "type": "raster",
    #     "src": "{protocol}/gfw2-data/climate/Saatchi_JPL_biomass/1km_2000/extent_binary/{tile_id}_Saatchi_JPL_AGB_1km_2000_extent_binary.tif",
    #     "s3_target": "{protocol}/gfw-files/2018_update/jpl_tropics_abovegroundbiomass_extent_2000/{tile_id}.tif",
    #     "data_type": "Byte",
    #     "nodata": 0
    # },
    "FiaRegionsUsExtent": {
        "type": "raster",
        "src": "{protocol}/gfw2-data/climate/carbon_model/US_FIA_region/processed/20200724/{tile_id}_FIA_regions_processed.tif",
        "s3_target": "{protocol}/gfw-files/flux_2_1_0/FIA_regions/{tile_id}.tif",
        "data_type": "Byte",
        "nodata": 0
    },
    # "legal_Amazon_2000": {
    #     "type": "raster",
    #     "src": "{protocol}/gfw2-data/climate/carbon_model/sensit_analysis_legal_Amazon_loss/forest_extent_2000/processed/tiles/20200116/{tile_id}_legal_Amazon_forest_extent_2000.tif",
    #     "s3_target": "{protocol}/gfw-files/2018_update/legal_Amazon_2000/{tile_id}.tif",
    #     "data_type": "Byte",
    #     "nodata": 0
    # },
    # "legal_Amazon_annual_loss": {
    #     "type": "raster",
    #     "src": "{protocol}/gfw2-data/climate/carbon_model/sensit_analysis_legal_Amazon_loss/annual_loss/processed/tiles/20200117/{tile_id}_legal_Amazon_annual_loss_2001_2015.tif",
    #     "s3_target": "{protocol}/gfw-files/2018_update/legal_Amazon_annual_loss/{tile_id}.tif",
    #     "data_type": "Byte",
    #     "nodata": 0
    # },
    # "Mekong_first_year_annual_loss_2001_2015": {
    #     "type": "raster",
    #     "src": "{protocol}/gfw2-data/climate/carbon_model/sensit_analysis_Mekong_loss/processed/20200210/{tile_id}_Mekong_loss_2001_15.tif",
    #     "s3_target": "{protocol}/gfw-files/2018_update/Mekong_first_year_annual_loss_2001_2015/{tile_id}.tif",
    #     "data_type": "Byte",
    #     "nodata": 0
    # },
    # "Mekong_loss_extent": {
    #     "type": "raster",
    #     "src": "{protocol}/gfw2-data/climate/carbon_model/sensit_analysis_Mekong_loss/extent/20200211/Mekong_loss_extent.tif",
    #     "s3_target": "{protocol}/gfw-files/2018_update/Mekong_loss_extent/{tile_id}.tif",
    #     "data_type": "Byte",
    #     "nodata": 0
    # },
    # "tropic_latitude_extent": {
    #     "type": "raster",
    #     "src": "{protocol}/gfw2-data/climate/carbon_model/other_emissions_inputs/tropical_latitude_extent/tropical_latitude_extent_no_Australia.tif",
    #     "s3_target": "{protocol}/gfw-files/2018_update/tropic_latitude_extent/tropical_latitude_extent_no_Australia.tif.tif",
    #     "data_type": "Byte",
    #     "nodata": 0
    # },
    "BurnYearHansenLoss": {
        "type": "raster",
        "src": "{protocol}/gfw2-data/climate/carbon_model/other_emissions_inputs/burn_year/20200807/burn_year_with_Hansen_loss/{tile_id}_burnyear.tif",
        "s3_target": "{protocol}/gfw-files/flux_2_1_0/burn_year_with_Hansen_loss/{tile_id}.tif",
        "data_type": "Byte",
        "nodata": 0
    },
    "GrossEmissionsNodeCodes": {
        "type": "raster",
        # "src": "{protocol}/gfw2-data/climate/carbon_model/gross_emissions/decision_tree_nodes/biomass_soil/standard/20200824/{tile_id}_gross_emis_decision_tree_nodes_biomass_soil_2001_19.tif",
        # "s3_target": "{protocol}/gfw-files/flux_2_1_0/gross_emissions_node_codes/standard/{tile_id}.tif",
        # "src": "{protocol}/gfw2-data/climate/carbon_model/gross_emissions/decision_tree_nodes/soil_only/standard/20200828/{tile_id}_gross_emis_decision_tree_nodes_soil_only_2001_19.tif",
        # "s3_target": "{protocol}/gfw-files/flux_2_1_0/gross_emissions_node_codes/soil_only/{tile_id}.tif",
        "src": "{protocol}/gfw2-data/climate/carbon_model/gross_emissions/decision_tree_nodes/biomass_soil/no_shifting_ag/20200914/{tile_id}_gross_emis_decision_tree_nodes_biomass_soil_2001_19_no_shifting_ag.tif",
        "s3_target": "{protocol}/gfw-files/flux_2_1_0/gross_emissions_node_codes/no_shifting_ag/{tile_id}.tif",
        "data_type": "Int16",
        "nodata": 0
    },



    "carbon_flux_custom_area_1": {
        "type": "raster",
        "src": "{protocol}/gfw-files/dgibbs/Mars_GHG_accounting/IDN_spam2010V1r1_global_grid_land_2010_2005CropAreas_rasterized.tif",
        "s3_target": "{protocol}/gfw-files/2018_update/carbon_flux_custom_area_1/{tile_id}.tif",
        "data_type": "Int16",
        "nodata": 0
    },



    "area": {
        "type": "raster",
        "src": "{protocol}/gfw2-data/analyses/area_28m/hanson_2013_area_{tile_id}.tif",
        "s3_target": "{protocol}/gfw-files/2018_update/area/{tile_id}.tif",
        "data_type": "Float32",
        "nodata": 0,
    },
    "loss": {
        "type": "raster",
        "src": "{protocol}/gfw2-data/forest_change/hansen_2018/{tile_id}.tif",
        "s3_target": "{protocol}/gfw-files/2018_update/loss/{tile_id}.tif",
        "data_type": "Byte",
        "nodata": 0,
    },
    "gain": {
        "type": "raster",
        "src": "{protocol}/gfw2-data/forest_change/tree_cover_gain/gaindata_2012/Hansen_GFC2015_gain_{tile_id}.tif",
        "s3_target": "{protocol}/gfw-files/2018_update/gain/{tile_id}.tif",
        "data_type": "Byte",
        "nodata": 0,
    },
    "tcd_2000": {
        "type": "raster",
        "src": "{protocol}/gfw2-data/forest_cover/2000_treecover/Hansen_GFC2014_treecover2000_{tile_id}.tif",
        "s3_target": "{protocol}/gfw-files/2018_update/tcd_2000/{tile_id}.tif",
        "data_type": "Byte",
        "nodata": 0,
    },
    "tcd_2010": {
        "type": "raster",
        "src": "{protocol}/gfw2-data/forest_cover/2010_treecover_27m/treecover2010_{tile_id}.tif",
        "s3_target": "{protocol}/gfw-files/2018_update/tcd_2010/{tile_id}.tif",
        "data_type": "Byte",
        "nodata": 0,
    },
    "co2_pixel": {
        "type": "raster",
        "src": "{protocol}/gfw2-data/climate/WHRC_biomass/WHRC_V4/t_co2_pixel/{tile_id}_t_co2_pixel_2000.tif",
        "s3_target": "{protocol}/gfw-files/2018_update/co2_pixel/{tile_id}.tif",
        "data_type": "Float32",
        "nodata": 0,
    },
    "biomass": {
        "type": "raster",
        "src": "{protocol}/gfw2-data/climate/WHRC_biomass/WHRC_V4/Processed/{tile_id}_t_aboveground_biomass_ha_2000.tif",
        "s3_target": "{protocol}/gfw-files/2018_update/biomass/{tile_id}.tif",
        "data_type": "Int16",
        "nodata": 0,
    },

    "drivers": {
        "type": "raster",
        "src": "{protocol}/gfw-files/2018_update/raw/drivers/drivers.vrt",
        "s3_target": "{protocol}/gfw-files/2018_update/drivers/{tile_id}.tif",
        "data_type": "Byte",
        "nodata": 16,
        "single_tile": True,
    },
    "global_landcover": {
        "type": "raster",
        "src": "{protocol}/gfw-files/2018_update/raw/global_landcover/global_landcover.vrt",
        "s3_target": "{protocol}/gfw-files/2018_update/global_landcover/{tile_id}.tif",
        "data_type": "Byte",
        "nodata": 0,
        "single_tile": True,
    },
    "primary_forest": {
        "type": "raster",
        "src": "{protocol}/gfw-files/2018_update/raw/primary_forest/primary_forest.vrt",
        "s3_target": "{protocol}/gfw-files/2018_update/primary_forest/{tile_id}.tif",
        "data_type": "Byte",
        "nodata": 0,
        "single_tile": True,
    },
    "idn_primary_forest": {
        "type": "raster",
        "src": "{protocol}/gfw-files/2018_update/raw/idn_primary_forest/idn_primary_forest.vrt",
        "s3_target": "{protocol}/gfw-files/2018_update/idn_primary_forest/{tile_id}.tif",
        "data_type": "Byte",
        "nodata": 0,
        "single_tile": True,
    },
    "erosion": {
        "type": "raster",
        "src": "{protocol}/gfw-files/2018_update/raw/erosion/erosion.vrt",
        "s3_target": "{protocol}/gfw-files/2018_update/erosion/{tile_id}.tif",
        "data_type": "Byte",
        "nodata": 0,
        "single_tile": True,
    },
    "gadm36": {
        "type": "raster",
        "src": "{protocol}/gfw2-data/analyses/gadm/tiles/adm2/gadm_adm2_{tile_id}.tif",
        "s3_target": "{protocol}/gfw-files/2018_update/gadm36/{tile_id}.tif",
        "data_type": "UInt16",
        "nodata": 0,
    },
    "biodiversity_significance": {
        "type": "raster",
        "src": "{protocol}/gfw-files/2018_update/raw/biodiversity/bio_s.vrt",
        "s3_target": "{protocol}/gfw-files/2018_update/biodiversity_significance/{tile_id}.tif",
        "data_type": "Float64",
        "nodata": 0,
    },
    "biodiversity_intactness": {
        "type": "raster",
        "src": "{protocol}/gfw-files/2018_update/raw/biodiversity/intactness.vrt",
        "s3_target": "{protocol}/gfw-files/2018_update/biodiversity_intactness/{tile_id}.tif",
        "data_type": "Float64",
        "nodata": 0,
    },
    "wdpa": {
        "type": "vector",
        "src": "protected_areas_10_10",
        "s3_target": "{protocol}/gfw-files/2018_update/wdpa/{tile_id}.tif",
        "oid": "val",
        "order": "desc",
        "data_type": "Byte",
        "nodata": 0,
    },
    "aze": {
        "type": "vector",
        "src": "aze_10_10",
        "s3_target": "{protocol}/gfw-files/2018_update/aze/{tile_id}.tif",
        "oid": "val",
        "order": "desc",
        "data_type": "Byte",
        "nodata": 0,
    },
    "plantations": {
        "type": "vector",
        "src": "plantations_10_10",
        "s3_target": "{protocol}/gfw-files/2018_update/plantations/{tile_id}.tif",
        "oid": "val",
        "order": "asc",
        "data_type": "Byte",
        "nodata": 0,
    },
    "river_basins": {
        "type": "vector",
        "src": "river_basins_10_10",
        "s3_target": "{protocol}/gfw-files/2018_update/river_basins/{tile_id}.tif",
        "oid": "val",
        "order": "asc",
        "data_type": "UInt16",
        "nodata": 0,
    },
    "ecozones": {
        "type": "vector",
        "src": "fao_ecozones_10_10",
        "s3_target": "{protocol}/gfw-files/2018_update/ecozones/{tile_id}.tif",
        "oid": "val",
        "order": "asc",
        "data_type": "Byte",
        "nodata": 0,
    },
    "urb_watersheds": {
        "type": "vector",
        "src": "urb_watersheds_10_10",
        "s3_target": "{protocol}/gfw-files/2018_update/urb_watersheds/{tile_id}.tif",
        "oid": "val",
        "order": "asc",
        "data_type": "Byte",
        "nodata": 0,
    },
    "mangroves_1996": {
        "type": "vector",
        "src": "mangroves_1996_10_10",
        "s3_target": "{protocol}/gfw-files/2018_update/mangroves_1996/{tile_id}.tif",
        "oid": "val",
        "order": "asc",
        "data_type": "Byte",
        "nodata": 0,
    },
    "mangroves_2016": {
        "type": "vector",
        "src": "mangroves_2016_10_10",
        "s3_target": "{protocol}/gfw-files/2018_update/mangroves_2016/{tile_id}.tif",
        "oid": "val",
        "order": "asc",
        "data_type": "Byte",
        "nodata": 0,
    },
    "water_stress": {
        "type": "vector",
        "src": "water_stress_10_10",
        "s3_target": "{protocol}/gfw-files/2018_update/water_stress/{tile_id}.tif",
        "oid": "val",
        "order": "asc",
        "data_type": "Byte",
        "nodata": 0,
    },
    "ifl": {
        "type": "vector",
        "src": "intact_forest_10_10",
        "s3_target": "{protocol}/gfw-files/2018_update/ifl/{tile_id}.tif",
        "oid": "val",
        "order": "asc",
        "data_type": "UInt16",
        "nodata": 0,
    },
    "endemic_bird_areas": {
        "type": "vector",
        "src": "endemic_bird_areas_10_10",
        "s3_target": "{protocol}/gfw-files/2018_update/endemic_bird_areas/{tile_id}.tif",
        "oid": "val",
        "order": "asc",
        "data_type": "Byte",
        "nodata": 0,
    },
    "tiger_landscapes": {
        "type": "vector",
        "src": "tiger_10_10",
        "s3_target": "{protocol}/gfw-files/2018_update/tiger_landscapes/{tile_id}.tif",
        "oid": "val",
        "order": "asc",
        "data_type": "Byte",
        "nodata": 0,
    },
    "landmark": {
        "type": "vector",
        "src": "landmark_10_10",
        "s3_target": "{protocol}/gfw-files/2018_update/landmark/{tile_id}.tif",
        "oid": "val",
        "order": "asc",
        "data_type": "Byte",
        "nodata": 0,
    },
    "land_rights": {
        "type": "vector",
        "src": "land_rights_10_10",
        "s3_target": "{protocol}/gfw-files/2018_update/land_rights/{tile_id}.tif",
        "oid": "val",
        "order": "asc",
        "data_type": "Byte",
        "nodata": 0,
    },
    "kba": {
        "type": "vector",
        "src": "kba_10_10",
        "s3_target": "{protocol}/gfw-files/2018_update/kba/{tile_id}.tif",
        "oid": "val",
        "order": "asc",
        "data_type": "Byte",
        "nodata": 0,
    },
    "mining": {
        "type": "vector",
        "src": "mining_10_10",
        "s3_target": "{protocol}/gfw-files/2018_update/mining/{tile_id}.tif",
        "oid": "val",
        "order": "asc",
        "data_type": "Byte",
        "nodata": 0,
    },
    "rspo": {
        "type": "vector",
        "src": "rspo_10_10",
        "s3_target": "{protocol}/gfw-files/2018_update/rspo/{tile_id}.tif",
        "oid": "val",
        "order": "asc",
        "data_type": "Byte",
        "nodata": 0,
    },
    "peatlands": {
        "type": "vector",
        "src": "peatlands_10_10",
        "s3_target": "{protocol}/gfw-files/2018_update/peatlands/{tile_id}.tif",
        "oid": "val",
        "order": "asc",
        "data_type": "Byte",
        "nodata": 0,
    },
    "oil_palm": {
        "type": "vector",
        "src": "oilpalm_10_10",
        "s3_target": "{protocol}/gfw-files/2018_update/oil_palm/{tile_id}.tif",
        "oid": "val",
        "order": "asc",
        "data_type": "Byte",
        "nodata": 0,
    },
    "idn_forest_moratorium": {
        "type": "vector",
        "src": "forest_moratorium_10_10",
        "s3_target": "{protocol}/gfw-files/2018_update/idn_forest_moratorium/{tile_id}.tif",
        "oid": "val",
        "order": "asc",
        "data_type": "Byte",
        "nodata": 0,
    },
    "idn_forest_area": {
        "type": "vector",
        "src": "idn_forest_area_10_10",
        "s3_target": "{protocol}/gfw-files/2018_update/idn_forest_area/{tile_id}.tif",
        "oid": "val",
        "order": "asc",
        "data_type": "UInt16",
        "nodata": 0,
    },
    "idn_land_cover": {
        "type": "vector",
        "src": "idn_land_cover_10_10",
        "s3_target": "{protocol}/gfw-files/2018_update/idn_land_cover/{tile_id}.tif",
        "oid": "val",
        "order": "asc",
        "data_type": "UInt16",
        "nodata": 0,
    },
    "mex_protected_areas": {
        "type": "vector",
        "src": "mex_protected_areas_10_10",
        "s3_target": "{protocol}/gfw-files/2018_update/mex_protected_areas/{tile_id}.tif",
        "oid": "val",
        "order": "asc",
        "data_type": "Byte",
        "nodata": 0,
    },
    "mex_psa": {
        "type": "vector",
        "src": "mex_psa_10_10",
        "s3_target": "{protocol}/gfw-files/2018_update/mex_psa/{tile_id}.tif",
        "oid": "val",
        "order": "asc",
        "data_type": "Byte",
        "nodata": 0,
    },
    "mex_forest_zoning": {
        "type": "vector",
        "src": "mex_forest_zoning_10_10",
        "s3_target": "{protocol}/gfw-files/2018_update/mex_forest_zoning/{tile_id}.tif",
        "oid": "val",
        "order": "asc",
        "data_type": "Byte",
        "nodata": 0,
    },
    "per_permanent_production_forests": {
        "type": "vector",
        "src": "per_permanent_production_forests_10_10",
        "s3_target": "{protocol}/gfw-files/2018_update/per_permanent_production_forests/{tile_id}.tif",
        "oid": "val",
        "order": "asc",
        "data_type": "Byte",
        "nodata": 0,
    },
    "per_protected_areas": {
        "type": "vector",
        "src": "per_protected_areas_10_10",
        "s3_target": "{protocol}/gfw-files/2018_update/per_protected_areas/{tile_id}.tif",
        "oid": "val",
        "order": "asc",
        "data_type": "Byte",
        "nodata": 0,
    },
    "per_forest_concessions": {
        "type": "vector",
        "src": "per_forest_concessions_10_10",
        "s3_target": "{protocol}/gfw-files/2018_update/per_forest_concessions/{tile_id}.tif",
        "oid": "val",
        "order": "asc",
        "data_type": "Byte",
        "nodata": 0,
    },
    "bra_biomes": {
        "type": "vector",
        "src": "bra_biomes_10_10",
        "s3_target": "{protocol}/gfw-files/2018_update/bra_biomes/{tile_id}.tif",
        "oid": "val",
        "order": "asc",
        "data_type": "Byte",
        "nodata": 0,
    },
    "wood_fiber": {
        "type": "vector",
        "src": "wood_fiber_10_10",
        "s3_target": "{protocol}/gfw-files/2018_update/wood_fiber/{tile_id}.tif",
        "oid": "val",
        "order": "asc",
        "data_type": "Byte",
        "nodata": 0,
    },
    "resource_rights": {
        "type": "vector",
        "src": "resource_rights_10_10",
        "s3_target": "{protocol}/gfw-files/2018_update/resource_rights/{tile_id}.tif",
        "oid": "val",
        "order": "asc",
        "data_type": "Byte",
        "nodata": 0,
    },
    "logging": {
        "type": "vector",
        "src": "logging_10_10",
        "s3_target": "{protocol}/gfw-files/2018_update/logging/{tile_id}.tif",
        "oid": "val",
        "order": "asc",
        "data_type": "Byte",
        "nodata": 0,
    },
    "oil_gas": {
        "type": "vector",
        "src": "logging_10_10",
        "s3_target": "{protocol}/gfw-files/2018_update/oil_gas/{tile_id}.tif",
        "oid": "val",
        "order": "asc",
        "data_type": "Byte",
        "nodata": 0,
    },
}


def get_tiles(overwrite=False, **kwargs):
    tiles = list()

    if "single_tile" not in kwargs.keys():
        kwargs["single_tile"] = False

    dir = os.path.dirname(__file__)
    with open(os.path.join(dir, "csv/tiles.csv")) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=",")
        for row in csv_reader:
            tiles.append(row)

    pipe = (
        tiles
        | Stage(info, kwargs["src"], **kwargs).setup(workers=WORKERS)
        | Stage(
            info,
            kwargs["s3_target"],
            include_existing=overwrite,
            exclude_missing=False,
            **{k: v for k, v in kwargs.items() if k != "single_tile"}
        ).setup(workers=WORKERS)
    )

    tiles_to_process = list()
    for output in pipe.results():
        tiles_to_process.append(output)

    return tiles_to_process


def raster_pipe(layer, overwrite):

    kwargs = SRC[layer]
    kwargs["tile_size"] = TILE_SIZE
    kwargs["pg_conn"] = PG_CONN
    kwargs["pixel_size"] = PIXEL_SIZE
    kwargs["is_vector"] = False

    tiles = get_tiles(overwrite, **kwargs)
    pipe = (
        tiles
        | Stage(translate, name=layer, **kwargs).setup(workers=WORKERS, qsize=WORKERS)
        | Stage(upload_file, **kwargs).setup(workers=WORKERS)
        | Stage(delete_file, **kwargs).setup(workers=WORKERS)
    )

    for output in pipe.results():
        logging.info(output)


def vector_pipe(layer, overwrite):

    kwargs = SRC[layer]
    kwargs["tile_size"] = TILE_SIZE
    kwargs["pixel_size"] = PIXEL_SIZE
    kwargs["pg_conn"] = PG_CONN
    kwargs["host"] = HOST
    kwargs["port"] = PORT
    kwargs["dbname"] = DBNAME
    kwargs["dbuser"] = DBUSER
    kwargs["password"] = PASSWORD
    kwargs["is_vector"] = True

    # import_vector(layer, **kwargs)
    # prep_layers(layer, **kwargs)

    tiles = get_tiles(overwrite, **kwargs)

    pipe = (
        tiles
        | Stage(rasterize, layer=layer, **kwargs).setup(workers=WORKERS, qsize=WORKERS)
        | Stage(upload_file, **kwargs).setup(workers=WORKERS)
        | Stage(delete_file, **kwargs).setup(workers=WORKERS)
    )

    for output in pipe.results():
        logging.info(output)


def delete_empty_tiles():
    pass


if __name__ == "__main__":

    layers = list(SRC.keys())

    parser = argparse.ArgumentParser(description="Prepare GFW tiles for SPARK pipeline")

    parser.add_argument("--layer", "-l", type=str, choices=layers)

    parser.add_argument(
        "--overwrite",
        type=str2bool,
        nargs="?",
        default=False,
        const=True,
        help="Overwrite existing output files",
    )

    args = parser.parse_args()

    if SRC[args.layer]["type"] == "raster":  # type: ignore
        raster_pipe(args.layer, args.overwrite)
    else:
        vector_pipe(args.layer, args.overwrite)