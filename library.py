
# 

# 1. Connect to Microsoft Graph
# You need 'AppRoleAssignment.ReadWrite.All' to grant permissions to others
Connect-MgGraph -Scopes "AppRoleAssignment.ReadWrite.All", "Application.Read.All"

# 2. Paste your Logic App's Object ID here
$managedIdentityObjectId = "YOUR_LOGIC_APP_OBJECT_ID_HERE"

# 3. This is the constant ID for the Microsoft Graph API itself
$graphAppId = "00000003-0000-0000-c000-000000000000"
$graphServicePrincipal = Get-MgServicePrincipal -Filter "AppId eq '$graphAppId'"

# 4. Find the specific 'Application.Read.All' permission ID
$appReadRole = $graphServicePrincipal.AppRoles | Where-Object {$_.Value -eq "Application.Read.All"}

# 5. Assign the role to your Logic App
$params = @{
    "PrincipalId" = $managedIdentityObjectId
    "ResourceId"  = $graphServicePrincipal.Id
    "AppRoleId"   = $appReadRole.Id
}

New-MgServicePrincipalAppRoleAssignment -ServicePrincipalId $managedIdentityObjectId @params

Write-Host "Success! Permission Application.Read.All has been granted." -ForegroundColor Green



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
