from gfw_tile_prep.stages import rasterize, translate, upload_file, delete_file, info
from gfw_tile_prep.utils import str2bool
from parallelpipe import Stage
import csv
import logging
import argparse
import os

# parallel workers
WORKERS = 22

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
    "gross_annual_removals_biomass": {
        "type": "raster",
        "src": "{protocol}/gfw2-data/climate/carbon_model/annual_gain_rate_all_forest_types/standard/20191016/{tile_id}_annual_gain_rate_AGB_BGB_t_ha_all_forest_types.tif",
        "s3_target": "{protocol}/gfw-files/2018_update/gross_annual_removals_biomass/standard/{tile_id}.tif",
        "data_type": "Float32",
        "nodata": 0,
    },
    "gross_cumul_removals_co2": {
        "type": "raster",
        # "src": "{protocol}/gfw2-data/climate/carbon_model/cumulative_gain_AGCO2_BGCO2_all_forest_types/standard/20191016/{tile_id}_cumul_gain_AGCO2_BGCO2_t_ha_all_forest_types_2001_15.tif",
        # "s3_target": "{protocol}/gfw-files/2018_update/gross_cumul_removals_co2/standard/{tile_id}.tif",
        # "src": "{protocol}/gfw2-data/climate/carbon_model/cumulative_gain_AGCO2_BGCO2_all_forest_types/maxgain/20191104/{tile_id}_cumul_gain_AGCO2_BGCO2_t_ha_all_forest_types_2001_15_maxgain.tif",
        # "s3_target": "{protocol}/gfw-files/2018_update/gross_cumul_removals_co2/maxgain/{tile_id}.tif",
        # "src": "{protocol}/gfw2-data/climate/carbon_model/cumulative_gain_AGCO2_BGCO2_all_forest_types/biomass_swap/20200107/{tile_id}_cumul_gain_AGCO2_BGCO2_t_ha_all_forest_types_2001_15_biomass_swap.tif",
        # "s3_target": "{protocol}/gfw-files/2018_update/gross_cumul_removals_co2/biomass_swap/{tile_id}.tif",
        # "src": "{protocol}/gfw2-data/climate/carbon_model/cumulative_gain_AGCO2_BGCO2_all_forest_types/US_removals/20200107/{tile_id}_cumul_gain_AGCO2_BGCO2_t_ha_all_forest_types_2001_15_US_removals.tif",
        # "s3_target": "{protocol}/gfw-files/2018_update/gross_cumul_removals_co2/US_removals/{tile_id}.tif",
        # "src": "{protocol}/gfw2-data/climate/carbon_model/cumulative_gain_AGCO2_BGCO2_all_forest_types/no_primary_gain/20200106/{tile_id}_cumul_gain_AGCO2_BGCO2_t_ha_all_forest_types_2001_15_no_primary_gain.tif",
        # "s3_target": "{protocol}/gfw-files/2018_update/gross_cumul_removals_co2/no_primary_gain/{tile_id}.tif",
        # "src": "{protocol}/gfw2-data/climate/carbon_model/cumulative_gain_AGCO2_BGCO2_all_forest_types/legal_Amazon_loss/20200117/{tile_id}_cumul_gain_AGCO2_BGCO2_t_ha_all_forest_types_2001_15_legal_Amazon_loss.tif",
        # "s3_target": "{protocol}/gfw-files/2018_update/gross_cumul_removals_co2/legal_Amazon_loss/{tile_id}.tif",
        "src": "{protocol}/gfw2-data/climate/carbon_model/cumulative_gain_AGCO2_BGCO2_all_forest_types/Mekong_loss/20200210/{tile_id}_cumul_gain_AGCO2_BGCO2_t_ha_all_forest_types_2001_15_Mekong_loss.tif",
        "s3_target": "{protocol}/gfw-files/2018_update/gross_cumul_removals_co2/Mekong_loss/{tile_id}.tif",
        "data_type": "Float32",
        "nodata": 0,
    },
    "agc_emis_year": {
        "type": "raster",
        # "src": "{protocol}/gfw2-data/climate/carbon_model/carbon_pools/aboveground_carbon/loss_pixels/standard/20191105/{tile_id}_t_AGC_ha_emis_year.tif",
        # "s3_target": "{protocol}/gfw-files/2018_update/agc_emis_year/standard/{tile_id}.tif",
        # "src": "{protocol}/gfw2-data/climate/carbon_model/carbon_pools/aboveground_carbon/loss_pixels/biomass_swap/20200107/{tile_id}_t_AGC_ha_emis_year_biomass_swap.tif",
        # "s3_target": "{protocol}/gfw-files/2018_update/agc_emis_year/biomass_swap/{tile_id}.tif",
        # "src": "{protocol}/gfw2-data/climate/carbon_model/carbon_pools/aboveground_carbon/loss_pixels/US_removals/20200107/{tile_id}_t_AGC_ha_emis_year_US_removals.tif",
        # "s3_target": "{protocol}/gfw-files/2018_update/agc_emis_year/US_removals/{tile_id}.tif",
        # "src": "{protocol}/gfw2-data/climate/carbon_model/carbon_pools/aboveground_carbon/loss_pixels/no_primary_gain/20200107/{tile_id}_t_AGC_ha_emis_year_no_primary_gain.tif",
        # "s3_target": "{protocol}/gfw-files/2018_update/agc_emis_year/no_primary_gain/{tile_id}.tif",
        # "src": "{protocol}/gfw2-data/climate/carbon_model/carbon_pools/aboveground_carbon/loss_pixels/legal_Amazon_loss/20200117/{tile_id}_t_AGC_ha_emis_year_legal_Amazon_loss.tif",
        # "s3_target": "{protocol}/gfw-files/2018_update/agc_emis_year/legal_Amazon_loss/{tile_id}.tif",
        "src": "{protocol}/gfw2-data/climate/carbon_model/carbon_pools/aboveground_carbon/loss_pixels/Mekong_loss/20200210/{tile_id}_t_AGC_ha_emis_year_Mekong_loss.tif",
        "s3_target": "{protocol}/gfw-files/2018_update/agc_emis_year/Mekong_loss/{tile_id}.tif",
        "data_type": "Float32",
        "nodata": 0,
    },
    "bgc_emis_year": {
        "type": "raster",
        "src": "{protocol}/gfw2-data/climate/carbon_model/carbon_pools/belowground_carbon/loss_pixels/standard/20191105/{tile_id}_t_BGC_ha_emis_year.tif",
        "s3_target": "{protocol}/gfw-files/2018_update/bgc_emis_year/standard/{tile_id}.tif",
        "data_type": "Float32",
        "nodata": 0,
    },
    "deadwood_carbon_emis_year": {
        "type": "raster",
        "src": "{protocol}/gfw2-data/climate/carbon_model/carbon_pools/deadwood_carbon/loss_pixels/standard/20191105/{tile_id}_t_deadwood_C_ha_emis_year_2000.tif",
        "s3_target": "{protocol}/gfw-files/2018_update/deadwood_carbon_emis_year/standard/{tile_id}.tif",
        "data_type": "Float32",
        "nodata": 0,
    },
    "litter_carbon_emis_year": {
        "type": "raster",
        "src": "{protocol}/gfw2-data/climate/carbon_model/carbon_pools/litter_carbon/loss_pixels/standard/20191105/{tile_id}_t_litter_C_ha_emis_year_2000.tif",
        "s3_target": "{protocol}/gfw-files/2018_update/litter_carbon_emis_year/standard/{tile_id}.tif",
        "data_type": "Float32",
        "nodata": 0,
    },
    "soil_carbon_emis_year": {
        "type": "raster",
        # "src": "{protocol}/gfw2-data/climate/carbon_model/carbon_pools/soil_carbon/loss_pixels/standard/20191105/{tile_id}_t_soil_C_ha_emis_year_2000.tif",
        # "s3_target": "{protocol}/gfw-files/2018_update/soil_carbon_emis_year/standard/{tile_id}.tif",
        # "src": "{protocol}/gfw2-data/climate/carbon_model/carbon_pools/soil_carbon/loss_pixels/biomass_swap/20200107/{tile_id}_t_soil_C_ha_emis_year_2000_biomass_swap.tif",
        # "s3_target": "{protocol}/gfw-files/2018_update/soil_carbon_emis_year/biomass_swap/{tile_id}.tif",
        # "src": "{protocol}/gfw2-data/climate/carbon_model/carbon_pools/soil_carbon/loss_pixels/US_removals/20200107/{tile_id}_t_soil_C_ha_emis_year_2000_US_removals.tif",
        # "s3_target": "{protocol}/gfw-files/2018_update/soil_carbon_emis_year/US_removals/{tile_id}.tif",
        # "src": "{protocol}/gfw2-data/climate/carbon_model/carbon_pools/soil_carbon/loss_pixels/no_primary_gain/20200107/{tile_id}_t_soil_C_ha_emis_year_2000_no_primary_gain.tif",
        # "s3_target": "{protocol}/gfw-files/2018_update/soil_carbon_emis_year/no_primary_gain/{tile_id}.tif",
        # "src": "{protocol}/gfw2-data/climate/carbon_model/carbon_pools/soil_carbon/loss_pixels/legal_Amazon_loss/20200117/{tile_id}_t_soil_C_ha_emis_year_2000_legal_Amazon_loss.tif",
        # "s3_target": "{protocol}/gfw-files/2018_update/soil_carbon_emis_year/legal_Amazon_loss/{tile_id}.tif",
        "src": "{protocol}/gfw2-data/climate/carbon_model/carbon_pools/soil_carbon/loss_pixels/Mekong_loss/20200210/{tile_id}_t_soil_C_ha_emis_year_2000_Mekong_loss.tif",
        "s3_target": "{protocol}/gfw-files/2018_update/soil_carbon_emis_year/Mekong_loss/{tile_id}.tif",
        "data_type": "Float32",
        "nodata": 0,
    },
    "total_carbon_emis_year": {
        "type": "raster",
        "src": "{protocol}/gfw2-data/climate/carbon_model/carbon_pools/total_carbon/loss_pixels/standard/20191105/{tile_id}_t_total_C_ha_emis_year.tif",
        "s3_target": "{protocol}/gfw-files/2018_update/total_carbon_emis_year/standard/{tile_id}.tif",
        "data_type": "Float32",
        "nodata": 0,
    },
    "agc_2000": {
        "type": "raster",
        "src": "{protocol}/gfw2-data/climate/carbon_model/carbon_pools/aboveground_carbon/extent_2000/20190531/{tile_id}_t_AGC_ha_2000.tif",
        "s3_target": "{protocol}/gfw-files/2018_update/agc_2000/standard/{tile_id}.tif",
        "data_type": "Float32",
        "nodata": 0,
    },
    "bgc_2000": {
        "type": "raster",
        "src": "{protocol}/gfw2-data/climate/carbon_model/carbon_pools/belowground_carbon/extent_2000/20190531/{tile_id}_t_BGC_ha_2000.tif",
        "s3_target": "{protocol}/gfw-files/2018_update/bgc_2000/standard/{tile_id}.tif",
        "data_type": "Float32",
        "nodata": 0,
    },
    "deadwood_carbon_2000": {
        "type": "raster",
        "src": "{protocol}/gfw2-data/climate/carbon_model/carbon_pools/deadwood_carbon/extent_2000/20190531/{tile_id}_t_deadwood_C_ha_2000.tif",
        "s3_target": "{protocol}/gfw-files/2018_update/deadwood_carbon_2000/standard/{tile_id}.tif",
        "data_type": "Float32",
        "nodata": 0,
    },
    "litter_carbon_2000": {
        "type": "raster",
        "src": "{protocol}/gfw2-data/climate/carbon_model/carbon_pools/litter_carbon/extent_2000/20190531/{tile_id}_t_litter_C_ha_2000.tif",
        "s3_target": "{protocol}/gfw-files/2018_update/litter_carbon_2000/standard/{tile_id}.tif",
        "data_type": "Float32",
        "nodata": 0,
    },
    "soil_carbon_2000": {
        "type": "raster",
        "src": "{protocol}/gfw2-data/climate/carbon_model/carbon_pools/soil_carbon/intermediate_full_extent/20190419/{tile_id}_t_soil_C_ha_full_extent_2000.tif",
        "s3_target": "{protocol}/gfw-files/2018_update/soil_carbon_2000/standard/{tile_id}.tif",
        "data_type": "Float32",
        "nodata": 0,
    },
    "total_carbon_2000": {
        "type": "raster",
        "src": "{protocol}/gfw2-data/climate/carbon_model/carbon_pools/total_carbon/extent_2000/20190602/{tile_id}_t_total_C_ha_2000.tif",
        "s3_target": "{protocol}/gfw-files/2018_update/total_carbon_2000/standard/{tile_id}.tif",
        "data_type": "Float32",
        "nodata": 0,
    },
    "gross_emissions_co2_only_co2e": {
        "type": "raster",
        # "src": "{protocol}/gfw2-data/climate/carbon_model/gross_emissions/all_drivers/CO2_only/biomass_soil/standard/20191106/{tile_id}_gross_emis_CO2_only_all_drivers_t_CO2e_ha_biomass_soil.tif",
        # "s3_target": "{protocol}/gfw-files/2018_update/gross_emissions_co2_only_co2e/standard/{tile_id}.tif",
        # "src": "{protocol}/gfw2-data/climate/carbon_model/gross_emissions/all_drivers/CO2_only/soil_only/standard/20191106/{tile_id}_gross_emis_CO2_only_all_drivers_t_CO2e_ha_soil_only.tif",
        # "s3_target": "{protocol}/gfw-files/2018_update/gross_emissions_co2_only_co2e/soil_only/{tile_id}.tif",
        # "src": "{protocol}/gfw2-data/climate/carbon_model/gross_emissions/all_drivers/CO2_only/biomass_soil/no_shifting_ag/20191120/{tile_id}_gross_emis_CO2_only_all_drivers_t_CO2e_ha_biomass_soil_no_shifting_ag.tif",
        # "s3_target": "{protocol}/gfw-files/2018_update/gross_emissions_co2_only_co2e/no_shifting_ag/{tile_id}.tif",
        # "src": "{protocol}/gfw2-data/climate/carbon_model/gross_emissions/all_drivers/CO2_only/biomass_soil/convert_to_grassland/20191206/{tile_id}_gross_emis_CO2_only_all_drivers_t_CO2e_ha_biomass_soil_convert_to_grassland.tif",
        # "s3_target": "{protocol}/gfw-files/2018_update/gross_emissions_co2_only_co2e/convert_to_grassland/{tile_id}.tif",
        # "src": "{protocol}/gfw2-data/climate/carbon_model/gross_emissions/all_drivers/CO2_only/biomass_soil/biomass_swap/20200107/{tile_id}_gross_emis_CO2_only_all_drivers_t_CO2e_ha_biomass_soil_biomass_swap.tif",
        # "s3_target": "{protocol}/gfw-files/2018_update/gross_emissions_co2_only_co2e/biomass_swap/{tile_id}.tif",
        # "src": "{protocol}/gfw2-data/climate/carbon_model/gross_emissions/all_drivers/CO2_only/biomass_soil/US_removals/20200107/{tile_id}_gross_emis_CO2_only_all_drivers_t_CO2e_ha_biomass_soil_US_removals.tif",
        # "s3_target": "{protocol}/gfw-files/2018_update/gross_emissions_co2_only_co2e/US_removals/{tile_id}.tif",
        # "src": "{protocol}/gfw2-data/climate/carbon_model/gross_emissions/all_drivers/CO2_only/biomass_soil/no_primary_gain/20200107/{tile_id}_gross_emis_CO2_only_all_drivers_t_CO2e_ha_biomass_soil_no_primary_gain.tif",
        # "s3_target": "{protocol}/gfw-files/2018_update/gross_emissions_co2_only_co2e/no_primary_gain/{tile_id}.tif",
        # "src": "{protocol}/gfw2-data/climate/carbon_model/gross_emissions/all_drivers/CO2_only/biomass_soil/legal_Amazon_loss/20200117/{tile_id}_gross_emis_CO2_only_all_drivers_t_CO2e_ha_biomass_soil_legal_Amazon_loss.tif",
        # "s3_target": "{protocol}/gfw-files/2018_update/gross_emissions_co2_only_co2e/legal_Amazon_loss/{tile_id}.tif",
        "src": "{protocol}/gfw2-data/climate/carbon_model/gross_emissions/all_drivers/CO2_only/biomass_soil/Mekong_loss/20200210/{tile_id}_gross_emis_CO2_only_all_drivers_t_CO2e_ha_biomass_soil_Mekong_loss.tif",
        "s3_target": "{protocol}/gfw-files/2018_update/gross_emissions_co2_only_co2e/Mekong_loss/{tile_id}.tif",
        "data_type": "Float32",
        "nodata": 0,
    },
    "gross_emissions_non_co2_co2e": {
        "type": "raster",
        # "src": "{protocol}/gfw2-data/climate/carbon_model/gross_emissions/all_drivers/non_CO2/biomass_soil/standard/20191106/{tile_id}_gross_emis_non_CO2_all_drivers_t_CO2e_ha_biomass_soil.tif",
        # "s3_target": "{protocol}/gfw-files/2018_update/gross_emissions_non_co2_co2e/standard/{tile_id}.tif",
        # "src": "{protocol}/gfw2-data/climate/carbon_model/gross_emissions/all_drivers/non_CO2/soil_only/standard/20191106/{tile_id}_gross_emis_non_CO2_all_drivers_t_CO2e_ha_soil_only.tif",
        # "s3_target": "{protocol}/gfw-files/2018_update/gross_emissions_non_co2_co2e/soil_only/{tile_id}.tif",
        # "src": "{protocol}/gfw2-data/climate/carbon_model/gross_emissions/all_drivers/non_CO2/biomass_soil/convert_to_grassland/20191206/{tile_id}_gross_emis_non_CO2_all_drivers_t_CO2e_ha_biomass_soil_convert_to_grassland.tif",
        # "s3_target": "{protocol}/gfw-files/2018_update/gross_emissions_non_co2_co2e/convert_to_grassland/{tile_id}.tif",
        # "src": "{protocol}/gfw2-data/climate/carbon_model/gross_emissions/all_drivers/non_CO2/biomass_soil/biomass_swap/20200107/{tile_id}_gross_emis_non_CO2_all_drivers_t_CO2e_ha_biomass_soil_biomass_swap.tif",
        # "s3_target": "{protocol}/gfw-files/2018_update/gross_emissions_non_co2_co2e/biomass_swap/{tile_id}.tif",
        # "src": "{protocol}/gfw2-data/climate/carbon_model/gross_emissions/all_drivers/non_CO2/biomass_soil/US_removals/20200107/{tile_id}_gross_emis_non_CO2_all_drivers_t_CO2e_ha_biomass_soil_US_removals.tif",
        # "s3_target": "{protocol}/gfw-files/2018_update/gross_emissions_non_co2_co2e/US_removals/{tile_id}.tif",
        # "src": "{protocol}/gfw2-data/climate/carbon_model/gross_emissions/all_drivers/non_CO2/biomass_soil/no_primary_gain/20200107/{tile_id}_gross_emis_non_CO2_all_drivers_t_CO2e_ha_biomass_soil_no_primary_gain.tif",
        # "s3_target": "{protocol}/gfw-files/2018_update/gross_emissions_non_co2_co2e/no_primary_gain/{tile_id}.tif",
        "src": "{protocol}/gfw2-data/climate/carbon_model/gross_emissions/all_drivers/non_CO2/biomass_soil/Mekong_loss/20200210/{tile_id}_gross_emis_non_CO2_all_drivers_t_CO2e_ha_biomass_soil_Mekong_loss.tif",
        "s3_target": "{protocol}/gfw-files/2018_update/gross_emissions_non_co2_co2e/Mekong_loss/{tile_id}.tif",
        "data_type": "Float32",
        "nodata": 0,
    },
    "net_flux_co2e": {
        "type": "raster",
        # "src": "{protocol}/gfw2-data/climate/carbon_model/net_flux_all_forest_types_all_drivers/biomass_soil/standard/20191106/{tile_id}_net_flux_t_CO2e_ha_2001_15_biomass_soil.tif",
        # "s3_target": "{protocol}/gfw-files/2018_update/net_flux_co2e/standard/{tile_id}.tif",
        # "src": "{protocol}/gfw2-data/climate/carbon_model/net_flux_all_forest_types_all_drivers/biomass_soil/maxgain/20191106/{tile_id}_net_flux_t_CO2e_ha_2001_15_biomass_soil_maxgain.tif",
        # "s3_target": "{protocol}/gfw-files/2018_update/net_flux_co2e/maxgain/{tile_id}.tif",
        # "src": "{protocol}/gfw2-data/climate/carbon_model/net_flux_all_forest_types_all_drivers/biomass_soil/convert_to_grassland/20191206/{tile_id}_net_flux_t_CO2e_ha_2001_15_biomass_soil_convert_to_grassland.tif",
        # "s3_target": "{protocol}/gfw-files/2018_update/net_flux_co2e/convert_to_grassland/{tile_id}.tif",
        # "src": "{protocol}/gfw2-data/climate/carbon_model/net_flux_all_forest_types_all_drivers/biomass_soil/biomass_swap/20200107/{tile_id}_net_flux_t_CO2e_ha_2001_15_biomass_soil_biomass_swap.tif",
        # "s3_target": "{protocol}/gfw-files/2018_update/net_flux_co2e/biomass_swap/{tile_id}.tif",
        # "src": "{protocol}/gfw2-data/climate/carbon_model/net_flux_all_forest_types_all_drivers/biomass_soil/US_removals/20200107/{tile_id}_net_flux_t_CO2e_ha_2001_15_biomass_soil_US_removals.tif",
        # "s3_target": "{protocol}/gfw-files/2018_update/net_flux_co2e/US_removals/{tile_id}.tif",
        # "src": "{protocol}/gfw2-data/climate/carbon_model/net_flux_all_forest_types_all_drivers/biomass_soil/no_primary_gain/20200107/{tile_id}_net_flux_t_CO2e_ha_2001_15_biomass_soil_no_primary_gain.tif",
        # "s3_target": "{protocol}/gfw-files/2018_update/net_flux_co2e/no_primary_gain/{tile_id}.tif",
        # "src": "{protocol}/gfw2-data/climate/carbon_model/net_flux_all_forest_types_all_drivers/biomass_soil/legal_Amazon_loss/20200117/{tile_id}_net_flux_t_CO2e_ha_2001_15_biomass_soil_legal_Amazon_loss.tif",
        # "s3_target": "{protocol}/gfw-files/2018_update/net_flux_co2e/legal_Amazon_loss/{tile_id}.tif",
        "src": "{protocol}/gfw2-data/climate/carbon_model/net_flux_all_forest_types_all_drivers/biomass_soil/Mekong_loss/20200210/{tile_id}_net_flux_t_CO2e_ha_2001_15_biomass_soil_Mekong_loss.tif",
        "s3_target": "{protocol}/gfw-files/2018_update/net_flux_co2e/Mekong_loss/{tile_id}.tif",
        "data_type": "Float32",
        "nodata": 0,
    },
    "peatlands_flux": {
        "type": "raster",
        "src": "{protocol}/gfw2-data/climate/carbon_model/other_emissions_inputs/peatlands/processed/20190429/{tile_id}_peat_mask_processed.tif",
        "s3_target": "{protocol}/gfw-files/2018_update/peatlands_flux/standard/{tile_id}.tif",
        "data_type": "Byte",
        "nodata": 0
    },
    "forest_age_category": {
        "type": "raster",
        "src": "{protocol}/gfw2-data/climate/carbon_model/forest_age_category_natural_forest/standard/20191016/{tile_id}_forest_age_category_natural_forest.tif",
        "s3_target": "{protocol}/gfw-files/2018_update/forest_age_category/standard/{tile_id}.tif",
        "data_type": "Byte",
        "nodata": 0
    },
    "ifl_primary": {
        "type": "raster",
        "src": "{protocol}/gfw2-data/climate/carbon_model/ifl_primary_merged/processed/20190905/{tile_id}_ifl_2000_primary_2001_merged.tif",
        "s3_target": "{protocol}/gfw-files/2018_update/ifl_primary/standard/{tile_id}.tif",
        "data_type": "Byte",
        "nodata": 0
    },
    "jpl_tropics_abovegroundbiomass_extent_2000": {
        "type": "raster",
        "src": "{protocol}/gfw2-data/climate/Saatchi_JPL_biomass/1km_2000/extent_binary/{tile_id}_Saatchi_JPL_AGB_1km_2000_extent_binary.tif",
        "s3_target": "{protocol}/gfw-files/2018_update/jpl_tropics_abovegroundbiomass_extent_2000/{tile_id}.tif",
        "data_type": "Byte",
        "nodata": 0
    },
    "jpl_tropics_abovegroundbiomass_density_2000": {
        "type": "raster",
        "src": "{protocol}/gfw2-data/climate/Saatchi_JPL_biomass/1km_2000/processed/20200107/{tile_id}_Mg_aboveground_biomass_ha_2000_JPL.tif",
        "s3_target": "{protocol}/gfw-files/2018_update/jpl_tropics_abovegroundbiomass_density_2000/Mg_ha-1/{tile_id}.tif",
        "data_type": "Float32",
        "nodata": 0
    },
    "FIA_regions_US_extent": {
        "type": "raster",
        "src": "{protocol}/gfw2-data/climate/carbon_model/sensit_analysis_US_removals/FIA_region/processed/20191216/{tile_id}_FIA_regions_processed.tif",
        "s3_target": "{protocol}/gfw-files/2018_update/FIA_regions_US_extent/{tile_id}.tif",
        "data_type": "Byte",
        "nodata": 0
    },
    "legal_Amazon_2000": {
        "type": "raster",
        "src": "{protocol}/gfw2-data/climate/carbon_model/sensit_analysis_legal_Amazon_loss/forest_extent_2000/processed/tiles/20200116/{tile_id}_legal_Amazon_forest_extent_2000.tif",
        "s3_target": "{protocol}/gfw-files/2018_update/legal_Amazon_2000/{tile_id}.tif",
        "data_type": "Byte",
        "nodata": 0
    },
    "legal_Amazon_annual_loss": {
        "type": "raster",
        "src": "{protocol}/gfw2-data/climate/carbon_model/sensit_analysis_legal_Amazon_loss/annual_loss/processed/tiles/20200117/{tile_id}_legal_Amazon_annual_loss_2001_2015.tif",
        "s3_target": "{protocol}/gfw-files/2018_update/legal_Amazon_annual_loss/{tile_id}.tif",
        "data_type": "Byte",
        "nodata": 0
    },
    "Mekong_first_year_annual_loss_2001_2015": {
        "type": "raster",
        "src": "{protocol}/gfw2-data/climate/carbon_model/sensit_analysis_Mekong_loss/processed/20200210/{tile_id}_Mekong_loss_2001_15.tif",
        "s3_target": "{protocol}/gfw-files/2018_update/Mekong_first_year_annual_loss_2001_2015/{tile_id}.tif",
        "data_type": "Byte",
        "nodata": 0
    },
    "Mekong_loss_extent": {
        "type": "raster",
        "src": "{protocol}/gfw2-data/climate/carbon_model/sensit_analysis_Mekong_loss/extent/20200211/Mekong_loss_extent.tif",
        "s3_target": "{protocol}/gfw-files/2018_update/Mekong_loss_extent/{tile_id}.tif",
        "data_type": "Byte",
        "nodata": 0
    },
    "tropic_latitude_extent": {
        "type": "raster",
        "src": "{protocol}/gfw2-data/climate/carbon_model/other_emissions_inputs/tropical_latitude_extent/tropical_latitude_extent_no_Australia.tif",
        "s3_target": "{protocol}/gfw-files/2018_update/tropic_latitude_extent/{tile_id}.tif",
        "data_type": "Byte",
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
    "mangrove_biomass": {
        "type": "raster",
        "src": "{protocol}/gfw2-data/climate/carbon_model/mangrove_biomass/processed/20190220/{tile_id}_mangrove_agb_t_ha_2000.tif",
        "s3_target": "{protocol}/gfw-files/2018_update/mangrove_biomass/{tile_id}.tif",
        "data_type": "Float32",
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