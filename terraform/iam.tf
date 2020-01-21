resource "aws_iam_role" "aws_ecs_service_role" {
  name               = substr("${local.project}-ecs_service_role${local.name_suffix}", 0, 64)
  assume_role_policy = data.local_file.ecs-task_assume.content
}

//resource "aws_iam_instance_profile" "aws_ecs_service_role" {
//  name = substr("${local.project}-ecs_service_role${local.name_suffix}", 0, 64)
//  role = aws_iam_role.aws_ecs_service_role.name
//}

//resource "aws_iam_role_policy_attachment" "aws_ecs_service_role" {
//  role       = aws_iam_role.aws_ecs_service_role.name
//  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSServiceRoleForECS"
//}


resource "aws_iam_role_policy_attachment" "s3_read_only" {
  role       = aws_iam_role.aws_ecs_service_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
}

resource "aws_iam_role_policy_attachment" "s3_write_data-lake" {
  role       = aws_iam_role.aws_ecs_service_role.name
  policy_arn = data.terraform_remote_state.core.outputs.iam_policy_s3_write_data-lake_arn
}

resource "aws_iam_role_policy" "test_policy" {
  name   = substr("${local.project}-ecs_service_role_assume${local.name_suffix}", 0, 64)
  role   = aws_iam_role.aws_ecs_service_role.name
  policy = data.template_file.iam_assume_role.rendered
}

## Clone role, and allow orginal to assume clone. -> Needed to get credentials for GDALWarp

resource "aws_iam_role" "aws_ecs_service_role_clone" {
  name               = substr("${local.project}-ecs_service_role_clone${local.name_suffix}", 0, 64)
  assume_role_policy = data.template_file.iam_trust_entity.rendered
}

resource "aws_iam_role_policy_attachment" "s3_read_only_clone" {
  role       = aws_iam_role.aws_ecs_service_role_clone.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
}

resource "aws_iam_role_policy_attachment" "s3_write_data-lake_clone" {
  role       = aws_iam_role.aws_ecs_service_role_clone.name
  policy_arn = data.terraform_remote_state.core.outputs.iam_policy_s3_write_data-lake_arn
}
