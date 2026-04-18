AuditLogs
| where ActivityDisplayName == "Add user"
| where TargetResources[0].userPrincipalName == "newuser@yourdomain.com"
| extend Creator = tostring(InitiatedBy.user.userPrincipalName),
         CreatorIP = tostring(InitiatedBy.user.ipAddress),
         AppCreator = tostring(InitiatedBy.app.displayName)
| project TimeGenerated, OperationName, TargetUser = TargetResources[0].userPrincipalName, Creator, AppCreator, CreatorIP, Result
