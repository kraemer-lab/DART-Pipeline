# Configuration file to be passed to scripts

ISO3=VNM
ADMIN=2  # can be 1, 2, or 3

# Scale at which core parameters (t2m, r, mx2t24 ...) are processed
# SPI and SPEI are always calculated weekly
# If processing at daily resolution, files are not concatenated into one file
# as the core metrics are not combined with SPI and SPEI at weekly resolution
TEMPORAL_RESOLUTION=weekly

# Note that data will be downloaded one year before and after the study
# period to align with ISO weeks
START_YEAR=2001
END_YEAR=2019

# Change this if you want SPI and SPEI gamma parameters to be calculated
# for a different range. Usually a range of 15 years is enough for estimation
INDEX_START_YEAR="$START_YEAR"
INDEX_END_YEAR="$END_YEAR"

# Set BC_ENABLE=0 (anything other than 1) to turn off bias correction
BC_ENABLE=1

# Bias correction parameters (BC_*)

# Precipitation reference, used for correcting total precipitation
BC_PRECIP_REF=vngp_regrid_era_full.nc

# Historical forecast data downloaded from ECMWF MARS service
BC_HISTORICAL_FORECAST=eefh_testv2_test_githubv1_3.nc

# Percentile at which reference data is clipped for precipitation bias
# correction. This is done to reduce the effect of outliers in bias correction
# using quantile mapping.
# BC_CLIP_PRECIP_PERCENTILE=0.99

# Historical observations, containing the following variables
# - t2m: 2m air temperature
# -   r: Relative humidity
# -  tp: Total precipitation
BC_HISTORICAL_OBS=T2m_r_tp_Vietnam_ERA5.nc

# Selecting one year before and after the range for ISO weeks
# Do not change -- used by other scripts
_fetch_start_year=$((START_YEAR - 1))
_fetch_end_year=$((END_YEAR + 1))
