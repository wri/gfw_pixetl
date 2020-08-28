resource "aws_iam_role" "aws_ecs_service_role" {
  name               = substr("${local.project}-ecs_service_role${local.name_suffix}", 0, 64)
  assume_role_policy = data.local_file.ecs-task_assume.content
}

resource "aws_iam_role_policy_attachment" "s3_read_only" {
  role       = aws_iam_role.aws_ecs_service_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
}

resource "aws_iam_role_policy_attachment" "s3_write_data-lake" {
  role       = aws_iam_role.aws_ecs_service_role.name
  policy_arn = data.terraform_remote_state.core.outputs.iam_policy_s3_write_data-lake_arn
}

resource "aws_iam_role_policy_attachment" "gfw-gee-export_key" {
  role       = aws_iam_role.aws_ecs_service_role.name
  policy_arn = data.terraform_remote_state.core.outputs.secrets_read-gfw-gee-export_policy_arn
}