SELECT
       Application_ID
    , Student_First_Name + ' ' + Student_Last_Name AS Student_FullName
    , SIS_Student_Id
    , Student_Birth_Date
    , StudentAddress_Street_1 + ', ' + StudentAddress_City + ', ' + StudentAddress_State AS StudentAddress
    , Current_School
    , IIF(Priorities LIKE '%Sibling Attending%','Y',NULL) AS Sibling
    , ' ' AS other_app
    , Grade_Applying_To
    , Application_Status
    , Waitlist_Number
    , CONVERT(varchar(10),Last_Status_Change,101) AS Last_Updated
    , datediff(day, CONVERT(varchar(10),Last_Status_Change,101), current_timestamp) AS Days_in_Current_Status
    , IIF(Application_Status = 'RC', datediff(day, CONVERT(varchar(10),Offered_Date,101), CONVERT(varchar(10),Registration_Completed_Date,101)),datediff(day, CONVERT(varchar(10),Offered_Date,101), current_timestamp)) AS Age_of_Reg_Pipeline
    , lk.SiteID_Galaxy AS ID
FROM custom.schoolmint_ApplicationData_raw raw
LEFT JOIN custom.lkSchools lk
    ON raw.School_Applying_to = lk.SchoolMint_ID
WHERE SchoolYear4Digit = 2025