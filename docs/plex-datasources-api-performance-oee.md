ID22870
Data Source NameWorkcenter_Performance_Simple_Get (Part)
Input Parameters
Name
Data Type
Required
Nullable
Workcenter_Key	int		
Output Data
Name
Data Type
Output Type
Nullable
Standard Object (Identifier Column)
Part_No_Revision	varchar	Column		
Performance	decimal	Column		
Performance_Color	varchar	Column		
Workcenter_Code	varchar	Column		
Workcenter_Status	varchar	Column		
Workcenter_Status_Color	varchar	Column	

ID18765
Data Source NameDaily_Performance_Report_Get (Part)
Input Parameters
Name
Data Type
Required
Nullable
Name
Data Type
Required
Nullable
Area_Key	int	 	
Building_Key	int	 	
Children_Only	bit	 	
Department_No	int	 	
End_Date	datetime	 	
Master_Key	int	 	
Parents_Only	bit	 	
Part_Key	int	 	
Part_Status	varchar	 	
Shift	varchar	 	
Start_Date	datetime	 	
Use_Standard_Rate	smallint	 	
Workcenter_Code	varchar	 	
Workcenter_Group	varchar	 	
Workcenter_Keys	varchar	 	
Workcenter_Type	varchar	 	
Output Data
Name
Data Type
Output Type
Nullable
Standard Object (Identifier Column)
Downtime_DB	decimal	Column		
Multiple	int	Column		
Number_Of_Children	int	Column		
Operation_Code	varchar	Column		
Part_Key	int	Column		
Part_No	varchar	Column		
Planned_Production_Time	decimal	Column		
Production_Qty	int	Column		
Reject_Qty	int	Column		
Scrap_Qty	int	Column		
Standard_Production_Rate	decimal	Column		
Throwouts	int	Column		
Workcenter_Code	varchar	Column		
Workcenter_Key	int	Column		
