terraform {
  required_version = ">= 1.5"
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 4.0"
    }
  }
}

# Subscription comes from `az login` plus the ARM_SUBSCRIPTION_ID env var.
provider "azurerm" {
  features {}
}

resource "azurerm_resource_group" "retailflow" {
  name     = var.resource_group_name
  location = var.location
}

resource "azurerm_storage_account" "lake" {
  name                     = var.storage_account_name
  resource_group_name      = azurerm_resource_group.retailflow.name
  location                 = azurerm_resource_group.retailflow.location
  account_kind             = "StorageV2"
  account_tier             = "Standard"
  account_replication_type = "LRS"
  min_tls_version          = "TLS1_2"

  # Hierarchical namespace is what turns Blob storage into ADLS Gen2.
  is_hns_enabled = true

  allow_nested_items_to_be_public = false
}

resource "azurerm_storage_data_lake_gen2_filesystem" "retailflow" {
  name               = "retailflow"
  storage_account_id = azurerm_storage_account.lake.id
}
