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
    image_url      = module.container_registry.repository_url
    environment    = var.environment
    job_role_arn   = aws_iam_role.aws_ecs_service_role.arn
    clone_role_arn = aws_iam_role.aws_ecs_service_role_clone.arn
    cpu            = 48
    memory         = 380000
    hardULimit     = 1024
    softULimit     = 1024
    maxSwap        = 600000
    swappiness     = 60
  }
}

data "template_file" "iam_trust_entity" {
  template = file("${path.root}/templates/iam_trust_entity.json")
  vars = {
    role_arn = aws_iam_role.aws_ecs_service_role.arn
  }
}

data "template_file" "iam_assume_role" {
  template = file("${path.root}/templates/iam_assume_role.json")
  vars = {
    role_arn = aws_iam_role.aws_ecs_service_role_clone.arn
  }
}

data "local_file" "ecs-task_assume" {
  filename = "${path.root}/templates/ecs-task_assume.json"
}
