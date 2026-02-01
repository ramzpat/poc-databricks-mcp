variable "env_file" {
  type        = string
  description = "Path to the .env file used for secrets and provider auth."
  default     = "${path.module}/../.env"
}

variable "secret_scope_name" {
  type        = string
  description = "Secret scope name for app secrets."
  default     = "databricks-mcp-app"
}

variable "secret_keys" {
  type        = list(string)
  description = "Keys read from .env and stored in the secret scope."
  default = [
    "DATABRICKS_HOST",
    "DATABRICKS_HTTP_PATH",
    "DATABRICKS_WAREHOUSE_ID",
    "DATABRICKS_CLIENT_ID",
    "DATABRICKS_CLIENT_SECRET",
    "DATABRICKS_TOKEN_URL"
  ]
}
