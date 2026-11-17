variable "location" {
  description = "Azure region for all resources."
  type        = string
  default     = "southeastasia"
}

variable "resource_group_name" {
  description = "Name of the resource group."
  type        = string
  default     = "rg-retailflow"
}

variable "storage_account_name" {
  description = "Globally unique storage account name (3-24 lowercase letters and digits)."
  type        = string

  validation {
    condition     = can(regex("^[a-z0-9]{3,24}$", var.storage_account_name))
    error_message = "storage_account_name must be 3-24 lowercase letters and digits."
  }
}
