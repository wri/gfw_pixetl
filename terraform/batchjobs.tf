resource "aws_batch_job_definition" "default" {
  name                 = "${local.project}${local.name_suffix}"
  type                 = "container"
  container_properties = data.template_file.container_properties.rendered
}

resource "aws_batch_job_queue" "default" {
  name                 = "${local.project}-job-queue${local.name_suffix}"
  state                = "ENABLED"
  priority             = 1
  compute_environments = [module.compute_environment_ephemeral_storage.arn]
}