
# 
# 1. Connect as a Global Admin
Connect-MgGraph -Scopes "AppRoleAssignment.ReadWrite.All", "Application.Read.All"

# 2. Paste your User-Assigned Identity's PRINCIPAL ID here
$userManagedIdentityId = "YOUR_USER_ASSIGNED_PRINCIPAL_ID"

# 3. Get the Microsoft Graph Service Principal
$graphAppId = "00000003-0000-0000-c000-000000000000"
$graphServicePrincipal = Get-MgServicePrincipal -Filter "AppId eq '$graphAppId'"

# 4. Find the 'Application.Read.All' role ID
$appReadRole = $graphServicePrincipal.AppRoles | Where-Object {$_.Value -eq "Application.Read.All"}

# 5. Assign the permission
$params = @{
    "PrincipalId" = $userManagedIdentityId
    "ResourceId"  = $graphServicePrincipal.Id
    "AppRoleId"   = $appReadRole.Id
}

New-MgServicePrincipalAppRoleAssignment -ServicePrincipalId $userManagedIdentityId @params

Write-Host "Success! Your User-Assigned Identity can now read all App Secrets." -ForegroundColor Green



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
