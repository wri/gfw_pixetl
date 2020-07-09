output "job_definition_arn" {
  value = aws_batch_job_definition.default.arn
}

output "job_queue_arn" {
  value = aws_batch_job_queue.default.arn
}

output "compute_environment_arn" {
  value = module.compute_environment_ephemeral_storage.arn
}

output "image_url" {
  value = module.container_registry.repository_url
}