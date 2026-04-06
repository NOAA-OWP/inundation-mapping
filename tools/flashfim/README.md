## About FLASH FIM
FLASH FIM is a product that uses flow values from the MRMS Flooded Locations And Simulated Hydrographs (FLASH) streamflow products as the input flow forcing to generate HAND Inundation. While a rapidly updating configuration of the National Water Model (NWM) is currently under development, it cannot yet be implemented, so the use of FLASH flow forecasts to generate FIM can provide two main benefits over the current NWM short range forecast:
- **More frequent update times:** FLASH updates every 10 minutes compared to the current NWM short range forecast's 1 hour update frequency. 
- **Reduced forecast latency:** Shorter delay from model initialization to product availability. From a little under 2 hours for the current NWM to 15 minutes for FLASH FIM.

## About FLASH Services
FLASH forecasts are derived using the current radar-only MRMS precipitation values to force three hydrologic models:

- Sacramento-Soil Moisture Accounting Model (SAC-SMA): Spatially lumped hydrologic model for simulating runoff within a basin.
- Coupled Routing and Excess Storage (CREST): Similar to SAC-SMA with an additional percent impervious parameter that enables better prediction in urban areas.
- Hydrophobic Model: "Worst case scenario" model that assumes no infiltration of precipitation.

The resulting products are 1km x 1km resolution gridded forecasts of the maximum 6 hr flow value assuming precipitation stops at the time of model initialization.


## How to use the tool
The tool found within this folder `/tools/flashfim/conflate_flash_flows.py` conflates the gridded flow values from the three FLASH forecasts to NWM feature_ids and outputs a flow file in a compatible format to input into `/tools/inundate_mosaic_wrapper.py` or other similar inundation scripts in the repository. To use this tool you need:
- The reference hydrofabric flowpaths version 3.0 or newer.
    - Can be accessed here: https://www.lynker-spatial.com/
- A geopackage of HUC8s to use to subset the flowpaths.
    - Download the Watershed Boundary Dataset (WBD) from here: https://www.usgs.gov/national-hydrography/access-national-hydrography-products
- A list of HUC8s to conflate flows for.
- The timestep you want to conflate the flows for. 
    - If the timestep parameter is left blank it defaults to pulling the latest or most current FLASH flow predictions. To select a historical timestep use the format YYYYMMDD-HHMMSS to the nearest 10 minutes Ex. 20250704-083000. 


