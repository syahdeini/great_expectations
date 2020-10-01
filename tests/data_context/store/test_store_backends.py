import pytest
import os
import boto3
from moto import mock_s3
from mock import patch

from great_expectations.exceptions import StoreBackendError

from great_expectations.data_context.store import (
    InMemoryStoreBackend,
    TupleFilesystemStoreBackend,
    TupleS3StoreBackend,
    TupleGCSStoreBackend,
    TupleAzureBlobStoreBackend,
)
from great_expectations.util import gen_directory_tree_str


def test_StoreBackendValidation():
    backend = InMemoryStoreBackend()

    backend._validate_key(("I", "am", "a", "string", "tuple"))

    with pytest.raises(TypeError):
        backend._validate_key("nope")

    with pytest.raises(TypeError):
        backend._validate_key(("I", "am", "a", "string", 100))

    with pytest.raises(TypeError):
        backend._validate_key(("I", "am", "a", "string", None))

    # zero-length tuple is allowed
    backend._validate_key(())


def test_InMemoryStoreBackend():

    my_store = InMemoryStoreBackend()

    my_key = ("A",)
    with pytest.raises(KeyError):
        my_store.get(my_key)

    my_store.set(my_key, "aaa")
    assert my_store.get(my_key) == "aaa"

    my_store.set(("B",), {"x": 1})

    assert my_store.has_key(my_key) is True
    assert my_store.has_key(("B",)) is True
    assert my_store.has_key(("A",)) is True
    assert my_store.has_key(("C",)) is False
    assert my_store.list_keys() == [("A",), ("B",)]

    with pytest.raises(NotImplementedError):
        my_store.get_url_for_key(my_key)


def test_tuple_filesystem_store_filepath_prefix_error(tmp_path_factory):
    path = str(
        tmp_path_factory.mktemp("test_tuple_filesystem_store_filepath_prefix_error")
    )
    project_path = str(tmp_path_factory.mktemp("my_dir"))

    with pytest.raises(StoreBackendError) as e:
        TupleFilesystemStoreBackend(
            root_directory=os.path.abspath(path),
            base_directory=project_path,
            filepath_prefix="invalid_prefix_ends_with/",
        )
    assert "filepath_prefix may not end with" in e.value.message

    with pytest.raises(StoreBackendError) as e:
        TupleFilesystemStoreBackend(
            root_directory=os.path.abspath(path),
            base_directory=project_path,
            filepath_prefix="invalid_prefix_ends_with\\",
        )
    assert "filepath_prefix may not end with" in e.value.message


def test_FilesystemStoreBackend_two_way_string_conversion(tmp_path_factory):
    path = str(
        tmp_path_factory.mktemp(
            "test_FilesystemStoreBackend_two_way_string_conversion__dir"
        )
    )
    project_path = str(tmp_path_factory.mktemp("my_dir"))

    my_store = TupleFilesystemStoreBackend(
        root_directory=os.path.abspath(path),
        base_directory=project_path,
        filepath_template="{0}/{1}/{2}/foo-{2}-expectations.txt",
    )

    tuple_ = ("A__a", "B-b", "C")
    converted_string = my_store._convert_key_to_filepath(tuple_)
    print(converted_string)
    assert converted_string == "A__a/B-b/C/foo-C-expectations.txt"

    recovered_key = my_store._convert_filepath_to_key(
        "A__a/B-b/C/foo-C-expectations.txt"
    )
    print(recovered_key)
    assert recovered_key == tuple_

    with pytest.raises(ValueError):
        tuple_ = ("A/a", "B-b", "C")
        converted_string = my_store._convert_key_to_filepath(tuple_)
        print(converted_string)


def test_TupleFilesystemStoreBackend(tmp_path_factory):
    path = "dummy_str"
    project_path = str(tmp_path_factory.mktemp("test_TupleFilesystemStoreBackend__dir"))

    my_store = TupleFilesystemStoreBackend(
        root_directory=os.path.abspath(path),
        base_directory=project_path,
        filepath_template="my_file_{0}",
    )

    # OPPORTUNITY: potentially standardize error instead of allowing each StoreBackend to raise its own error types
    with pytest.raises(FileNotFoundError):
        my_store.get(("AAA",))

    my_store.set(("AAA",), "aaa")
    assert my_store.get(("AAA",)) == "aaa"

    my_store.set(("BBB",), "bbb")
    assert my_store.get(("BBB",)) == "bbb"

    print(my_store.list_keys())
    assert set(my_store.list_keys()) == {("AAA",), ("BBB",)}

    print(gen_directory_tree_str(project_path))
    assert (
        gen_directory_tree_str(project_path)
        == """\
test_TupleFilesystemStoreBackend__dir0/
    my_file_AAA
    my_file_BBB
"""
    )


def test_TupleFilesystemStoreBackend_ignores_jupyter_notebook_checkpoints(
    tmp_path_factory,
):
    project_path = str(tmp_path_factory.mktemp("things"))

    checkpoint_dir = os.path.join(project_path, ".ipynb_checkpoints")
    os.mkdir(checkpoint_dir)
    assert os.path.isdir(checkpoint_dir)
    nb_file = os.path.join(checkpoint_dir, "foo.json")

    with open(nb_file, "w") as f:
        f.write("")
    assert os.path.isfile(nb_file)
    my_store = TupleFilesystemStoreBackend(
        root_directory=os.path.abspath("dummy_str"), base_directory=project_path,
    )

    my_store.set(("AAA",), "aaa")
    assert my_store.get(("AAA",)) == "aaa"

    assert (
        gen_directory_tree_str(project_path)
        == """\
things0/
    AAA
    .ipynb_checkpoints/
        foo.json
"""
    )

    print(my_store.list_keys())
    assert set(my_store.list_keys()) == {("AAA",)}


@mock_s3
def test_TupleS3StoreBackend():
    """
    What does this test test and why?

    We will exercise the store backend's set method twice and then verify
    that the we calling get and list methods will return the expected keys.

    We will also check that the objects are stored on S3 at the expected location.

    """
    bucket = "leakybucket"
    prefix = "this_is_a_test_prefix"

    # create a bucket in Moto's mock AWS environment
    conn = boto3.resource("s3", region_name="us-east-1")
    conn.create_bucket(Bucket=bucket)

    my_store = TupleS3StoreBackend(
        filepath_template="my_file_{0}", bucket=bucket, prefix=prefix,
    )

    # We should be able to list keys, even when empty
    keys = my_store.list_keys()
    assert len(keys) == 0

    my_store.set(("AAA",), "aaa", content_type="text/html; charset=utf-8")
    assert my_store.get(("AAA",)) == "aaa"

    obj = boto3.client("s3").get_object(Bucket=bucket, Key=prefix + "/my_file_AAA")
    assert obj["ContentType"] == "text/html; charset=utf-8"
    assert obj["ContentEncoding"] == "utf-8"

    my_store.set(("BBB",), "bbb")
    assert my_store.get(("BBB",)) == "bbb"

    assert set(my_store.list_keys()) == {("AAA",), ("BBB",)}
    assert set(
        [
            s3_object_info["Key"]
            for s3_object_info in boto3.client("s3").list_objects(
                Bucket=bucket, Prefix=prefix
            )["Contents"]
        ]
    ) == {"this_is_a_test_prefix/my_file_AAA", "this_is_a_test_prefix/my_file_BBB"}


def test_TupleGCSStoreBackend():
    pytest.importorskip("google-cloud-storage")
    """
    What does this test test and why?

    Since no package like moto exists for GCP services, we mock the GCS client
    and assert that the store backend makes the right calls for set, get, and list.

    """
    bucket = "leakybucket"
    prefix = "this_is_a_test_prefix"
    project = "dummy-project"

    my_store = TupleGCSStoreBackend(
        filepath_template="my_file_{0}", bucket=bucket, prefix=prefix, project=project
    )

    my_store_with_no_filepath_template = TupleGCSStoreBackend(
        filepath_template=None, bucket=bucket, prefix=prefix, project=project
    )

    with patch("google.cloud.storage.Client", autospec=True) as mock_gcs_client:

        mock_client = mock_gcs_client.return_value
        mock_bucket = mock_client.get_bucket.return_value
        mock_blob = mock_bucket.blob.return_value

        my_store.set(("AAA",), "aaa", content_type="text/html")

        mock_gcs_client.assert_called_once_with("dummy-project")
        mock_client.get_bucket.assert_called_once_with("leakybucket")
        mock_bucket.blob.assert_called_once_with("this_is_a_test_prefix/my_file_AAA")
        mock_blob.upload_from_string.assert_called_once_with(
            b"aaa", content_encoding="utf-8", content_type="text/html"
        )

    with patch("google.cloud.storage.Client", autospec=True) as mock_gcs_client:
        mock_client = mock_gcs_client.return_value
        mock_bucket = mock_client.get_bucket.return_value
        mock_blob = mock_bucket.blob.return_value

        my_store_with_no_filepath_template.set(
            ("AAA",), b"aaa", content_encoding=None, content_type="image/png"
        )

        mock_gcs_client.assert_called_once_with("dummy-project")
        mock_client.get_bucket.assert_called_once_with("leakybucket")
        mock_bucket.blob.assert_called_once_with("this_is_a_test_prefix/AAA")
        mock_blob.upload_from_string.assert_called_once_with(
            b"aaa", content_type="image/png"
        )

    with patch("google.cloud.storage.Client", autospec=True) as mock_gcs_client:

        mock_client = mock_gcs_client.return_value
        mock_bucket = mock_client.get_bucket.return_value
        mock_blob = mock_bucket.get_blob.return_value
        mock_str = mock_blob.download_as_string.return_value

        my_store.get(("BBB",))

        mock_gcs_client.assert_called_once_with("dummy-project")
        mock_client.get_bucket.assert_called_once_with("leakybucket")
        mock_bucket.get_blob.assert_called_once_with(
            "this_is_a_test_prefix/my_file_BBB"
        )
        mock_blob.download_as_string.assert_called_once()
        mock_str.decode.assert_called_once_with("utf-8")

    with patch("google.cloud.storage.Client", autospec=True) as mock_gcs_client:

        mock_client = mock_gcs_client.return_value

        my_store.list_keys()

        mock_client.list_blobs.assert_called_once_with(
            "leakybucket", prefix="this_is_a_test_prefix"
        )


def test_TupleAzureBlobStoreBackend():
    pytest.importorskip("azure-storage-blob")
    """
    What does this test test and why?

    Since no package like moto exists for Azure-Blob services, we mock the Azure-blob client
    and assert that the store backend makes the right calls for set, get, and list.

    """
    connection_string = "this_is_a_test_conn_string"
    prefix = "this_is_a_test_prefix"
    container = "dummy-container"

    my_store = TupleAzureBlobStoreBackend(
        connection_string=connection_string, prefix=prefix, container=container
    )

    with patch(
        "azure.storage.blob.BlobServiceClient", autospec=True
    ) as mock_azure_blob_client:

        mock_container_client = mock_azure_blob_client.get_container_client.return_value

        my_store.set(("AAA",), "aaa")

        mock_azure_blob_client.from_connection_string.assert_called_once()
        mock_container_client.assert_called_once()
        mock_container_client.upload_blob.assert_called_once_with(
            name="AAA", data=b"aaa", encoding="utf-8"
        )

    with patch(
        "azure.storage.blob.BlobServiceClient", autospec=True
    ) as mock_azure_blob_client:

        mock_container_client = mock_azure_blob_client.get_container_client.return_value

        my_store.set(("BBB",), b"bbb")

        mock_azure_blob_client.from_connection_string.assert_called_once()
        mock_container_client.assert_called_once()
        mock_container_client.upload_blob.assert_called_once_with(
            name="AAA", data=b"aaa"
        )

    with patch(
        "azure.storage.blob.BlobServiceClient", autospec=True
    ) as mock_azure_blob_client:

        mock_container_client = mock_azure_blob_client.get_container_client.return_value

        my_store.get(("BBB",))

        mock_azure_blob_client.from_connection_string.assert_called_once()
        mock_container_client.assert_called_once()
        mock_container_client.download_blob.assert_called_once_with("BBB")

    with patch(
        "azure.storage.blob.BlobServiceClient", autospec=True
    ) as mock_azure_blob_client:

        mock_container_client = mock_azure_blob_client.get_container_client.return_value

        my_store.list_keys()

        mock_azure_blob_client.from_connection_string.assert_called_once()
        mock_container_client.assert_called_once()
        mock_container_client.list_blobs.assert_called_once_with("this_is_a_prefix")
