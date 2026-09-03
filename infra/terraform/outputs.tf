output "component_blueprint" {
  description = "Deployment component blueprint."
  value = {
    for name, component in null_resource.component_blueprint :
    name => component.triggers
  }
}

output "secret_contracts" {
  description = "Secret contract names expected by the platform."
  value       = null_resource.secret_contracts.triggers
  sensitive   = true
}

