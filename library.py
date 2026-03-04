# Sample Code

Active Membership
`GET https://graph.microsoft.com/v1.0/users/{user-id}/transitiveMemberOf/microsoft.graph.group?$select=id,displayName`

Get Eligible Memberships
`GET https://graph.microsoft.com/v1.0/identityGovernance/privilegedAccess/group/eligibilitySchedules?$filter=principalId eq '{user-id}'`
