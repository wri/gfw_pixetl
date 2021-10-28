from botocore.exceptions import ClientError

from gfw_pixetl.utils.aws import get_s3_client


def check_s3_file_present(bucket, keys):
    s3_client = get_s3_client()

    for key in keys:
        try:
            s3_client.head_object(Bucket=bucket, Key=key)
        except ClientError:
            raise AssertionError(f"Object {key} doesn't exist in bucket {bucket}!")


def delete_s3_files(bucket, prefix):
    s3_client = get_s3_client()
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    for obj in response.get("Contents", list()):
        print("Deleting", obj["Key"])
        s3_client.delete_object(Bucket=bucket, Key=obj["Key"])


def compare_multipolygons(multi1, multi2):
    # A bit ugly, but YOU try comparing MultiPolygons!
    settified_multi1_coords = [set(geom.exterior.coords) for geom in multi1.geoms]
    settified_multi2_coords = [set(geom.exterior.coords) for geom in multi2.geoms]
    assert len(settified_multi1_coords) == len(settified_multi2_coords)

    for coord_set in settified_multi1_coords:
        assert coord_set in settified_multi2_coords

    for coord_set in settified_multi2_coords:
        assert coord_set in settified_multi1_coords
