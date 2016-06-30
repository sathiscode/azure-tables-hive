This is a copy of https://github.com/mooso/azure-tables-hadoop source.
Only change is in POM file which referenecs latest libraries.

Sample to expose Azure metrics table as a Hive table

CREATE EXTERNAL TABLE AzureMetricsHourPrimaryTransactionsBlob
(PartitionKey String, 
	Rowkey String, 
	`Timestamp` Timestamp, 
	TotalRequests bigint, 
	....)
STORED BY 'com.microsoft.hadoop.azure.hive.AzureTableHiveStorageHandler'
TBLPROPERTIES(
"azure.table.name"="$MetricsHourPrimaryTransactionsBlob",
"azure.table.account.uri"="http://<account>.table.core.windows.net",  
"azure.table.storage.key"="<storage key>");
