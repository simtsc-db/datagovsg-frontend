# Use Case
The user can explore Singapore and see in an instant which carparks are filled and which ones are mostly free.


## View
Use 3D objects to represent objects at the relevant address based on the geo-location of the carpark. Color the object gradually from transparent green (almost empty) to red (carpark full). Provide additional carpark information when clicking the object.


# APIs
- https://data.gov.sg/datasets/d_ca933a644e55d34fe21f28b8052fac63/view. This API contains the realtime update. 
- Metadata for the carparks can be obtained from this API https://data.gov.sg/datasets/d_23f946fa557947f93a8043bbef41dd09/view, and needs to be joined with the realtime data.