output "secret_scope" {
  description = "Name of the secret scope created for the app."
  value       = databricks_secret_scope.app.name
}

output "secret_references" {
  description = "Databricks secret references for app env configuration."
  value = {
    for key in keys(local.secret_values) :
    key => format(
      "{{secrets/%s/%s}}",
      databricks_secret_scope.app.name,
      key
    )
  }
}
