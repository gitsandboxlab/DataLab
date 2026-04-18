SigninLogs
| where UserPrincipalName == "user@yourdomain.com"
| extend DeviceId = tostring(DeviceDetail.deviceId), 
         DeviceName = tostring(DeviceDetail.displayName),
         OS = tostring(DeviceDetail.operatingSystem),
         Browser = tostring(DeviceDetail.browser)
| project TimeGenerated, UserPrincipalName, DeviceName, DeviceId, OS, Browser, IPAddress, AppDisplayName
| order by TimeGenerated desc
