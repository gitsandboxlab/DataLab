# Sample Code

-- Assuming your table is called 'SourceTable' and column is 'JsonData'
SELECT 
    JSON_VALUE(item.value, '$.id') AS PrincipalId, -- If pulling from root
    App.*
FROM SourceTable
CROSS APPLY OPENJSON(JsonData) 
WITH (
    id UNIQUEIDENTIFIER '$.id',
    displayName NVARCHAR(255) '$.displayName',
    endDateTime DATETIME2 '$.endDateTime',
    usage NVARCHAR(50) '$.usage'
) AS App;

Active Membership
`GET https://graph.microsoft.com/v1.0/users/{user-id}/transitiveMemberOf/microsoft.graph.group?$select=id,displayName`

Get Eligible Memberships
`GET https://graph.microsoft.com/v1.0/identityGovernance/privilegedAccess/group/eligibilitySchedules?$filter=principalId eq '{user-id}'`
