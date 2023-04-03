resource "aws_s3_bucket" "bucket_dl" {
    bucket = "bucket-lucas-las"
    acl="private"

    server_side_encryption_configuration {
        rule{
            apply_server_side_encryption_by_default{
                sse_algorithm = "AES256"
            }
        }
    }
}

provider "aws"{
    region="us-east-1"
}


