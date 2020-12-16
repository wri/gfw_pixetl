resource "aws_batch_job_definition" "default" {
  name                 = replace("${local.project}${local.name_suffix}", ".", "_")
  type                 = "container"
  container_properties = data.template_file.container_properties.rendered
}

resource "aws_batch_job_queue" "default" {
  name                 = replace("${local.project}-job-queue${local.name_suffix}", ".", "_")
  state                = "ENABLED"
  priority             = 1
  compute_environments = [module.compute_environment_ephemeral_storage.arn]
  depends_on           = [module.compute_environment_ephemeral_storage.arn]
}
