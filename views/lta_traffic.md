# Use Case
The user can explore Singapore and see the current taxi situation in an instant and where traffic cameras are located.


## View
Use 3D objects to represent objects on the map.
For cameras: mark them in red and make them clickable to show the latest traffic image for the respective camera.
For taxis: mark them blue, and update their current position. Add a label on top with relevant metadata.


# APIs
- https://data.gov.sg/datasets/d_6cdb6b405b25aaaacbaf7689bcc6fae0/view. This API contains the realtime camera images.
- https://data.gov.sg/datasets/d_147f4906651f5b32925dfe6560296161/view. This API contains metadata with additional information of the cameras location and can be joined to the realtime data.
- https://data.gov.sg/datasets/d_e25662f1a062dd046453926aa284ba64/view. This API contains the realtime location of taxis.