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
| cmSiteId | The siteId of Career Marketplace 
| cmJobOpportunityListId | The listId of the JobOpportunity list in the Career Marketplace
| cmDepartmentListId | The listId of the Department list in the Career Marketplace
| cmClassificationCodeListId | The listId of the ClassificationCode list in the Career Marketplace
| cmClassificationLevelListId | The listId of the ClassificationLevel list in the Career Marketplace
| cmDurationListId | The listId of the Duration list in the Career Marketplace
| cmWorkScheduleListId | The listId of the WorkSchedule list in the Career Marketplace
| cmSecurityClearanceListId | The listId of the SecurityClearance list in the Career Marketplace
| cmLanguageRequirementListId | The listId of the LanguageRequirement list in the Career Marketplace
| cmWorkArrangementListId | The listId of the WorkArrangement list in the Career Marketplace
| cmCityListId | The listId of the City list in the Career Marketplace
| cmRegionListId | The listId of the Region list in the Career Marketplace
| cmProvinceListId | The listId of the Province lsit in the Career Marketplace
| cmSkillsListId | The listId of the Skills list in the Career Marketplace
| cmJobTypeTermSetId | The term set ID for JobType (found in the SP term store)
| cmProgramAreaTermSetId | Ther term set ID for ProgramArea (found in the SP term store)
| isLocal | Optional. Should be set to `true` if you want to use `AzureCliCredential` for authentication. (use `az login` before running locally)