# The access key is deliberately not an output: it would land in plain text in state and CLI logs.

output "storage_account_name" {
  value = azurerm_storage_account.lake.name
}

output "dfs_endpoint" {
  value = azurerm_storage_account.lake.primary_dfs_endpoint
}

output "filesystem_name" {
  value = azurerm_storage_data_lake_gen2_filesystem.retailflow.name
}
