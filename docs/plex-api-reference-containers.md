## API calls for containers

Get Container

v1
Method
Get
Description
Gets an active container by serialNo.
Resource URL
https://connect.plex.com/inventory/v1/inventory-tracking/containers/{serialNo}
URL Parameters
Name
Type
Format
Description
Default Value
Required
serialNo
*
string
Serial Number to search for.

List Containers

v1
Method
Get
Description
Gets active containers.
Resource URL
https://connect.plex.com/inventory/v1/inventory-tracking/containers
Query Parameters
Name
Type
Format
Description
Default Value
Required
partId
string
uuid	
The ID of the part.

false
locationId
string
uuid	
The ID associated with the inventory location.

false
masterUnitId
string
uuid	
The ID of the master unit associated with the container.

false
lotId
string
uuid	
The lot ID associated with the container.

false
eun
string
The EUN associated with the container.

false
inventoryType
array (string)
The list of inventory types.

false

List Container Location Moves

v1
Method
Get
Description
List of container location moves entries.
Resource URL
https://connect.plex.com/inventory/v1/inventory-history/container-location-moves
Query Parameters
Name
Type
Format
Description
Default Value
Required
serialNo
string
25 characters max. The serial number of the container that was changed.

false
beginDate
*
string
date-time	
The date and time from when container change transactions are to be retrieved.

true
endDate
*
string
date-time	
The date and time until when container change transactions are to be retrieved.

true
partId
string
uuid	
The container's Part ID.

false
partOperationId
string
uuid	
The container's Part Operation ID.

false
partType
string
50 characters max. Setup table "Part Type" (part.dbo.Part_Type). Subclassification for the part. Examples include flange, fastener, gears, crown wheels, clutch hubs, body panels, gear shafts.

false
partGroup
string
50 characters max. Setup table "Part Group" (part.dbo.Part_Group). Subclassification for the part.

false
partSource
string
50 characters max. Setup table "Part Source" (part.dbo.Part_Source). Categorize where the parts are from, purchased or made in house. The field is not tied to purchasing for lookups or searches.

false
locationId
string
uuid	
The location ID of the container that was changed.

false
movedFromLocationId
string
uuid	
The prior location ID of the container before it was changed.

false

List Container Location Moves

v1
Method
Get
Description
List of container location moves entries.
Resource URL
https://connect.plex.com/inventory/v1/inventory-history/container-location-moves
Query Parameters
Name
Type
Format
Description
Default Value
Required
serialNo
string
25 characters max. The serial number of the container that was changed.

false
beginDate
*
string
date-time	
The date and time from when container change transactions are to be retrieved.

true
endDate
*
string
date-time	
The date and time until when container change transactions are to be retrieved.

true
partId
string
uuid	
The container's Part ID.

false
partOperationId
string
uuid	
The container's Part Operation ID.

false
partType
string
50 characters max. Setup table "Part Type" (part.dbo.Part_Type). Subclassification for the part. Examples include flange, fastener, gears, crown wheels, clutch hubs, body panels, gear shafts.

false
partGroup
string
50 characters max. Setup table "Part Group" (part.dbo.Part_Group). Subclassification for the part.

false
partSource
string
50 characters max. Setup table "Part Source" (part.dbo.Part_Source). Categorize where the parts are from, purchased or made in house. The field is not tied to purchasing for lookups or searches.

false
locationId
string
uuid	
The location ID of the container that was changed.

false
movedFromLocationId
string
uuid	
The prior location ID of the container before it was changed.

false

Get Container Shipping Details
Beta

v1-beta1
Disclaimer: This API is part of a limited beta release and may be used solely for testing and evaluation purposes. Do not use beta APIs for any production, commercial, or professional use, or any other purpose from which you or others derive material economic benefit, including competitive analysis. Please note that beta APIs may not be fully tested and may never be put into production.

We're very interested in your feedback that you can provide here: Plex-API-Beta@Plex.com
Method
Get
Description
Retrieve shipping details for a container.
Resource URL
https://connect.plex.com/shipping/v1-beta1/inventory/containers/{serialNo}/shipping-details
URL Parameters
Name
Type
Format
Description
Default Value
Required
serialNo
*
string
The serial number of the container.

List Shippable Containers
Beta

v1-beta1
Disclaimer: This API is part of a limited beta release and may be used solely for testing and evaluation purposes. Do not use beta APIs for any production, commercial, or professional use, or any other purpose from which you or others derive material economic benefit, including competitive analysis. Please note that beta APIs may not be fully tested and may never be put into production.

We're very interested in your feedback that you can provide here: Plex-API-Beta@Plex.com
Method
Get
Description
List shippable containers by query parameters.
Resource URL
https://connect.plex.com/shipping/v1-beta1/inventory/customer-shippers/{shipperId}/shippable-containers
URL Parameters
Name
Type
Format
Description
Default Value
Required
shipperId
*
string
uuid	
The shipper ID associated with the container.

true
Query Parameters
Name
Type
Format
Description
Default Value
Required
buildingId
string
uuid	
The ID of the building associated with the shippable container.

false
location
string
The location associated with the shippable container.

false
locationGroup
string
The location group associated with the shippable container.

false
partId
string
uuid	
The ID of the part associated with the shippable container.

false
Move Tank Containers

v1
Method
Post
Description
Move tank containers.
Resource URL
https://connect.plex.com/production/v1/tank-management/containers/move-containers
Request Body
Schema

{
fromLocationCode:"string"
toLocationCode:"string"
partId:"00000000-0000-0000-0000-000000000000"
partOperationId:"00000000-0000-0000-0000-000000000000"
moveQuantity:0.1
}

List Container Adjustments

v1
Method
Get
Description
List manual container inventory adjustment entries. This API returns results similar to the Inventory Adjustments application. See Knowledge Center MES API documentation for more details.
Resource URL
https://connect.plex.com/inventory/v1/inventory-history/container-adjustments
Query Parameters
Name
Type
Format
Description
Default Value
Required
serialNo
string
25 characters max. The serial number of the container that was adjusted.

false
beginDate
*
string
date-time	
The date and time from when container adjustment transactions are to be retrieved.

true
endDate
*
string
date-time	
The date and time until when container adjustment transactions are to be retrieved.

true
partId
string
uuid	
The container's Part ID.

false
partOperationId
string
uuid	
The container's Part Operation ID.

false
adjustmentCode
string
50 characters max. Setup table "Adjustment Reason" (part.dbo.adjustment_reason). The Adjustment Reason setup table lists the acceptable reasons for inventory changes.

false
locationId
string
uuid	
The location ID of the container that was adjusted.

false