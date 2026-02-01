data "external" "dotenv" {
  program = [
    "python",
    "${path.module}/scripts/read_env.py",
    var.env_file
  ]
}

locals {
  env              = data.external.dotenv.result
  databricks_host  = local.env["DATABRICKS_HOST"]
  databricks_token = local.env["DATABRICKS_TOKEN"]

  secret_values = {
    for key in var.secret_keys :
    key => local.env[key]
    if contains(keys(local.env), key)
  }

  missing_secret_keys = [
    for key in var.secret_keys : key
    if !contains(keys(local.env), key)
  ]
}

provider "databricks" {
  host  = local.databricks_host
  token = local.databricks_token
}

resource "databricks_secret_scope" "app" {
  name = var.secret_scope_name

  lifecycle {
    precondition {
      condition     = length(local.missing_secret_keys) == 0
      error_message = format(
        "Missing secret keys in .env: %s",
        join(", ", local.missing_secret_keys)
      )
    }
  }
}

resource "databricks_secret" "app" {
  for_each     = local.secret_values
  key          = each.key
  string_value = each.value
  scope        = databricks_secret_scope.app.name
}
