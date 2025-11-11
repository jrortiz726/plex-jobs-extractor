## List of workcenter-related calls

Create Approved Workcenter

v1
Method
Post
Description
Create a new approved workcenter. Does not follow Rate Display Setting. The standardProductionRate is overwritten to Operation.Standard_Rate / Part_Operation.Repetitions when Standard Rate Form Display is on and the Standard Rate is > 0.
Resource URL
https://connect.plex.com/production/v1/production-definitions/approved-workcenters
Request Body
Schema
{
partId:"00000000-0000-0000-0000-000000000000"
partOperationId:"00000000-0000-0000-0000-000000000000"
workcenterId:"00000000-0000-0000-0000-000000000000"
crewSize:0.1
standardProductionRate:0.1
note:"string"
setupTime:0.1
idealRate:0.1
targetRate:0.1
setupCrewSize:0.1
}
Response Structure
Status code	Description
+
201
Created
+
400
Bad Request

Get Workcenter

v1
Method
Get
Description
Retrieve information of a specific workcenter by its UUID.
Resource URL
https://connect.plex.com/production/v1/production-definitions/workcenters/{id}
URL Parameters
Name
Type
Format
Description
Default Value
Required
id
*
string
uuid	
The UUID assigned to the workcenter.

true

Get Workcenter Event

v1
Method
Get
Description
Retrieves a workcenter event by ID.
Resource URL
https://connect.plex.com/production/v1/production-definitions/workcenter-events/{workcenterEventId}
URL Parameters
Name
Type
Format
Description
Default Value
Required
workcenterEventId
*
string
uuid	
A unique identifier of a workcenter event

true

Get Workcenter Setup

v1
Method
Get
Description
Retrieves a workcenters current status, setup, and operators by ID.
Resource URL
https://connect.plex.com/production/v1/control/workcenters/{workcenterId}
URL Parameters
Name
Type
Format
Description
Default Value
Required
workcenterId
*
string
uuid	
A unique identifier of the workcenter.

true

Get Workcenter Status

v1
Method
Get
Description
Retrieves a workcenter status by ID.
Resource URL
https://connect.plex.com/production/v1/production-definitions/workcenter-statuses/{workcenterStatusId}
URL Parameters
Name
Type
Format
Description
Default Value
Required
workcenterStatusId
*
string
uuid	
A unique identifier of a workcenter status.

true

List Approved Workcenters

v1
Method
Get
Description
Gets all the approved workcenters based on the query parameters.
Resource URL
https://connect.plex.com/production/v1/production-definitions/approved-workcenters
Query Parameters
Name
Type
Format
Description
Default Value
Required
workcenterId
string
uuid	
The ID of the workcenter where part is produced.

false
partId
string
uuid	
The ID of the part that is produced.

false
partOperationId
string
uuid	
The ID of the part operation.

false

List Production Line Workcenters

v1
Method
Get
Description
Retrieves a list of all production line workcenters by the ID.
Resource URL
https://connect.plex.com/production/v1/production-definitions/production-lines/{id}/workcenters
URL Parameters
Name
Type
Format
Description
Default Value
Required
id
*
string
uuid	
The production line ID.

true

List Workcenter Events

v1
Method
Get
Description
Retrieves a list of workcenter events.
Resource URL
https://connect.plex.com/production/v1/production-definitions/workcenter-events
Query Parameters
Name
Type
Format
Description
Default Value
Required
active
boolean
Filters results by if workcenter event is active or not.

false
description
string
The description of a workcenter event.

false

List Workcenter Setup Entries

v1
Method
Get
Description
Gets the work center setup entries.
Resource URL
https://connect.plex.com/production/v1/production-history/workcenter-setup-entries
Query Parameters
Name
Type
Format
Description
Default Value
Required
beginSetupDate
*
string
date-time	
Gets or sets the Start of the DateTime (in UTC) Range for the requested setup entries

true
endSetupDate
string
date-time	
Gets or sets the End of the DateTime (in UTC) Range for the requested setup entries

false
workcenterId
string
uuid	
Gets or sets the WorkCenterId (UUID)

false
partId
string
uuid	
Gets or sets the PartId (UUID)

false

List Workcenter Status

v1
Method
Get
Description
Retrieves a list of all workcenter statuses on the query parameters.
Resource URL
https://connect.plex.com/production/v1/production-definitions/workcenter-statuses
Query Parameters
Name
Type
Format
Description
Default Value
Required
description
string
The description of a workcenter status.

false
active
boolean
Filters results by if status is active or not.

false
offStatus
boolean
Filters results if status is an off status or not.

false

List Workcenter Status Entries

v1
Method
Get
Description
Retrieve list of historical workcenter status entries. The API returns results that are a cross of Workcenter Status Summary and Workcenter Log applications. See Knowledge Center MES API documentation for more details.
Resource URL
https://connect.plex.com/production/v1/production-history/workcenter-status-entries
Query Parameters
Name
Type
Format
Description
Default Value
Required
workcenterId
string
uuid	
UUID V4 format XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX. UUID of the workcenter where the status transaction was recorded.

false
jobId
string
uuid	
UUID V4 format XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX. UUID of the production job that was loaded to the workcenter when the workcenter status entry was recorded. Note that a production job is not necessary to set or change the workcenter's status.

false
partId
string
uuid	
UUID V4 format XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX. UUID of the part that was being produced in the workcenter when the workcenter status entry was recorded.

false
status
string
50 characters max. The status of a workcenter.

false
beginDate
*
string
date-time	
Date/Time Format YYYY-MM-DDThh:mm:ss.fffffffZ See Knowledge center topic "Dates and Times" for more information (KC path Platform > Plex APIs > HTTP Data Source API > Dates and Times). The date and time from when recorded workcenter status transaction are to be retrieved.

true
endDate
*
string
date-time	
Date/Time Format YYYY-MM-DDThh:mm:ss.fffffffZ See Knowledge center topic "Dates and Times" for more information (KC path Platform > Plex APIs > HTTP Data Source API > Dates and Times). The date and time until when recorded workcenter status transaction are to be retrieved.

true

List Workcenters

v1
Method
Get
Description
Retrieve list of workcenters and their information.
Resource URL
https://connect.plex.com/production/v1/production-definitions/workcenters
Query Parameters
Name
Type
Format
Description
Default Value
Required
workcenterCode
string
A short name associated to the workcenterID, and a unique code to reference the workcenter. This code displays throughout the system, primarily on the Control Panel. 50 characters max.

false
name
string
The full name associated to the workcenterID that serves as a more descriptive name for the workcenter. Depending on your settings, this name can also display on the Control Panel and sequence board. 100 characters max.

false
workcenterType
string
A grouping of similar workcenters. The workcenter type influences how the workcenter behaves and is also available as a filter in different areas of the application, such as the Control Panel, Production Statuses, and more. Setup table "Workcenter Type" (part.dbo.workcenter_type). 50 characters max.

false
workcenterGroup
string
50 characters max. Setup table "Workcenter Group" (part.dbo.workcenter_group). An optional grouping of similar workcenters. This is helpful when there are many workcenters that can be chosen from the Control Panel. If the Workcenter Group setting is on, the Control Panel will display the Workcenter Groups then once a group is chosen, the list of workcenters only in that group will display.

false
buildingId
string
uuid	
UUID of the building the workcenter is located in.

false
tankSilo
boolean
Boolean indicating whether the results should be filtered to only return tank/silo results.

false
plcName
string
The PLC Name of the workcenter. 50 characters max.

false
ipAddress
string
The IP Address of the workcenter. 15 characters max.

false

Search Workcenter Setups

v1
Method
Post
Description
Searches for Workcenter Setups by Workcenter ID, Workcenter Code, Workcenter Type, and Workcenter Group.
Resource URL
https://connect.plex.com/production/v1/control/workcenter-setups
Request Body
Schema

{
workcenterIds:[
0:"00000000-0000-0000-0000-000000000000"
]
workcenterCodes:[
0:"string"
]
workcenterTypes:[
0:"string"
]
workcenterGroups:[
0:"string"
]
}

Set Workcenter Status

v1
Method
Post
Description
Updates workcenter status of a workcenter by ID.
Resource URL
https://connect.plex.com/production/v1/control/workcenters/{workcenterId}/status
URL Parameters
Name
Type
Format
Description
Default Value
Required
workcenterId
*
string
uuid	
true
Request Body
Schema

{
workcenterStatusId:"00000000-0000-0000-0000-000000000000"
workcenterEventId:"00000000-0000-0000-0000-000000000000"
accountId:"00000000-0000-0000-0000-000000000000"
}