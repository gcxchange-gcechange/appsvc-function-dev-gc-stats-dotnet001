# appsvc-function-dev-gc-stats-dotnet001
## Summary
This application saves various tenant metrics to Azure blob storage, then transfers them to a Fabric Warehouse.
## Version
![dotnet 10](https://img.shields.io/badge/net10.0-blue.svg)
## API permissions
| API / Permissions name | Type | Reason
| - | - | - |
| AuditLog.Read.All | Application | Determine user & group activity
| Group.Read.All | Application | Read group information
| Reports.Read.All | Application | Read SharePoint and Teams reports
| User.Read.All | Application | Collection of user data
| Sites.Read.All | Application | Collect metrics for site pages/files

## Application Settings
In order for the application to run you will need the following settings in your  `environment variables` of the deployed function app, or in the `local.settings.json` file in your local project.
| Name | Description
| - | - 
| AzureWebJobsStorage | Connection string for the storage account
| clientId | The application (client) ID of the app registration
| tenantId | The Id of the Azure tenant 
| keyVaultUrl | The address for the key vault
| secretName | The secret name that holds the API secret value
| storageAccountUrl | The address for the storage account
| fabricWarehouseServer | The SQL connection string of the fabric warehouse
| fabricWarehouseDatabase | The name of the fabric warehouse
| workspaceId | The workspace Id for log analytics
| exceptionUsersArray | A comma separated string of user Ids that will be ignored for all user related metrics
| exceptionGroupsArray | A comma separated string of group Ids that will be ignored for all group related metrics 
| isLocal | Optional. Should be set to `true` if you want to use `AzureCliCredential` for authentication. (use `az login` before running locally)