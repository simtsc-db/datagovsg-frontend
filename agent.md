# Singapore Palantir clone

## High-level specification
A webapp that shows a 3D map of Singapore and integrates with realtime APIs from data.gov.sg. 3D objects on the map are clickable and show additional information when clicked. Views can be set to account for different focus.

## Tech stack and plugins
- databricks-ai-dev-kit
- apx 
- CesiumJS (for 3D map)

## Features
- 3D map of Singapore
- Realtime data from data.gov.sg
- Clickable 3D objects
- Filterable objects

## Environment Configuration
I want to login to the workspace and make sure you can create all required assets.
Relevant API keys and environment configurations are documented in config.md

## Datasource Integration
- Read data from data.gov.sg APIs. The appropriate APIs will be given in the respective view specifications. 

## Views to include
- hdb_housing.md for specifications on showing HDB related information such as carparks.
- lta_traffic.md contains specifications on showing LTA related information such as traffic cams.
- nea_environmental.md contains specifications on showing NEA related information such as air quality and lightning observations.