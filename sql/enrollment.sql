SELECT
       Application_ID
    , Student_First_Name + ' ' + Student_Last_Name AS Student_FullName
    , SIS_Student_Id
    , Student_Birth_Date
    , StudentAddress_Street_1 + ', ' + StudentAddress_City + ', ' + StudentAddress_State AS StudentAddress
    , Current_School
    , Current_Grade
    , Application_Status
    , Waitlist_Number
    , ID
FROM custom.schoolmint_ApplicationData_raw raw
LEFT JOIN custom.lkSchools lk
    ON raw.School_Applying_to = lk.SchoolMint_ID
WHERE SchoolYear4Digit = 2025
    AND School_Applying_to = 160