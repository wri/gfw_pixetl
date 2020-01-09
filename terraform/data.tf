data "terraform_remote_state" "core" {
  backend = "s3"
  config = {
    bucket = local.tf_state_bucket
    region = "us-east-1"
    key    = "core.tfstate"
  }
}

data "template_file" "container_properties" {
  template = file("${path.root}/templates/container_properties.json")
  vars = {
    image_url    = module.ecr_push_dockerfile.repository_url
    environment  = var.environment
    job_role_arn = aws_iam_role.aws_ecs_service_role.arn
  }
}

data "local_file" "ecs-task_assume" {
  filename = "${path.root}/templates/ecs-task_assume.json"
}