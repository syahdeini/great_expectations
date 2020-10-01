import os
import random
import re
import logging

# PYTHON 2 - py2 - update to ABC direct use rather than __metaclass__ once we drop py2 support
from abc import ABCMeta

from six import string_types

from great_expectations.data_context.store.store_backend import StoreBackend
from great_expectations.data_context.util import safe_mmkdir
from great_expectations.exceptions import StoreBackendError

logger = logging.getLogger(__name__)


class TupleStoreBackend(StoreBackend):
    __metaclass__ = ABCMeta
    """
    If filepath_template is provided, the key to this StoreBackend abstract class must be a tuple with 
    fixed length equal to the number of unique components matching the regex r"{\d+}"

    For example, in the following template path: expectations/{0}/{1}/{2}/prefix-{2}.json, keys must have
    three components.
    """

    def __init__(
        self,
        filepath_template=None,
        filepath_prefix=None,
        filepath_suffix=None,
        forbidden_substrings=None,
        platform_specific_separator=True,
        fixed_length_key=False,
    ):
        super().__init__(fixed_length_key=fixed_length_key)
        if forbidden_substrings is None:
            forbidden_substrings = ["/", "\\"]
        self.forbidden_substrings = forbidden_substrings
        self.platform_specific_separator = platform_specific_separator

        if filepath_template is not None and filepath_suffix is not None:
            raise ValueError(
                "filepath_suffix may only be used when filepath_template is None"
            )

        self.filepath_template = filepath_template
        if filepath_prefix and len(filepath_prefix) > 0:
            # Validate that the filepath prefix does not end with a forbidden substring
            if filepath_prefix[-1] in self.forbidden_substrings:
                raise StoreBackendError(
                    "Unable to initialize TupleStoreBackend: filepath_prefix may not end with a "
                    "forbidden substring. Current forbidden substrings are "
                    + str(forbidden_substrings)
                )
        self.filepath_prefix = filepath_prefix
        self.filepath_suffix = filepath_suffix

        if filepath_template is not None:
            # key length is the number of unique values to be substituted in the filepath_template
            self.key_length = len(set(re.findall(r"{\d+}", filepath_template)))

            self.verify_that_key_to_filepath_operation_is_reversible()
            self._fixed_length_key = True

    def _validate_key(self, key):
        super()._validate_key(key)

        for key_element in key:
            for substring in self.forbidden_substrings:
                if substring in key_element:
                    raise ValueError(
                        "Keys in {0} must not contain substrings in {1} : {2}".format(
                            self.__class__.__name__, self.forbidden_substrings, key,
                        )
                    )

    def _validate_value(self, value):
        if not isinstance(value, string_types) and not isinstance(value, bytes):
            raise TypeError(
                "Values in {0} must be instances of {1} or {2}, not {3}".format(
                    self.__class__.__name__, string_types, bytes, type(value),
                )
            )

    def _convert_key_to_filepath(self, key):
        # NOTE: This method uses a hard-coded forward slash as a separator,
        # and then replaces that with a platform-specific separator if requested (the default)
        self._validate_key(key)
        if self.filepath_template:
            converted_string = self.filepath_template.format(*list(key))
        else:
            converted_string = "/".join(key)

        if self.filepath_prefix:
            converted_string = self.filepath_prefix + "/" + converted_string
        if self.filepath_suffix:
            converted_string += self.filepath_suffix
        if self.platform_specific_separator:
            converted_string = os.path.normpath(converted_string)

        return converted_string

    def _convert_filepath_to_key(self, filepath):
        if self.platform_specific_separator:
            filepath = os.path.normpath(filepath)

        if self.filepath_prefix:
            if (
                not filepath.startswith(self.filepath_prefix)
                and len(filepath) >= len(self.filepath_prefix) + 1
            ):
                # If filepath_prefix is set, we expect that it is the first component of a valid filepath.
                raise ValueError(
                    "filepath must start with the filepath_prefix when one is set by the store_backend"
                )
            else:
                # Remove the prefix before processing
                # Also remove the separator that was added, which may have been platform-dependent
                filepath = filepath[len(self.filepath_prefix) + 1 :]

        if self.filepath_suffix:
            if not filepath.endswith(self.filepath_suffix):
                # If filepath_suffix is set, we expect that it is the last component of a valid filepath.
                raise ValueError(
                    "filepath must end with the filepath_suffix when one is set by the store_backend"
                )
            else:
                # Remove the suffix before processing
                filepath = filepath[: -len(self.filepath_suffix)]

        if self.filepath_template:
            # filepath_template is always specified with forward slashes, but it is then
            # used to (1) dynamically construct and evaluate a regex, and (2) split the provided (observed) filepath
            if self.platform_specific_separator:
                filepath_template = os.path.join(*self.filepath_template.split("/"))
                filepath_template = filepath_template.replace("\\", "\\\\")
            else:
                filepath_template = self.filepath_template

            # Convert the template to a regex
            indexed_string_substitutions = re.findall(r"{\d+}", filepath_template)
            tuple_index_list = [
                "(?P<tuple_index_{0}>.*)".format(i,)
                for i in range(len(indexed_string_substitutions))
            ]
            intermediate_filepath_regex = re.sub(
                r"{\d+}", lambda m, r=iter(tuple_index_list): next(r), filepath_template
            )
            filepath_regex = intermediate_filepath_regex.format(*tuple_index_list)

            # Apply the regex to the filepath
            matches = re.compile(filepath_regex).match(filepath)
            if matches is None:
                return None

            # Map key elements into the appropriate parts of the tuple
            new_key = [None] * self.key_length
            for i in range(len(tuple_index_list)):
                tuple_index = int(
                    re.search(r"\d+", indexed_string_substitutions[i]).group(0)
                )
                key_element = matches.group("tuple_index_" + str(i))
                new_key[tuple_index] = key_element

            new_key = tuple(new_key)
        else:
            filepath = os.path.normpath(filepath)
            new_key = tuple(filepath.split(os.sep))

        return new_key

    def verify_that_key_to_filepath_operation_is_reversible(self):
        def get_random_hex(size=4):
            return "".join(
                [random.choice(list("ABCDEF0123456789")) for _ in range(size)]
            )

        key = tuple([get_random_hex() for _ in range(self.key_length)])
        filepath = self._convert_key_to_filepath(key)
        new_key = self._convert_filepath_to_key(filepath)
        if key != new_key:
            raise ValueError(
                "filepath template {0} for class {1} is not reversible for a tuple of length {2}. "
                "Have you included all elements in the key tuple?".format(
                    self.filepath_template, self.__class__.__name__, self.key_length,
                )
            )


class TupleFilesystemStoreBackend(TupleStoreBackend):
    """Uses a local filepath as a store.

    The key to this StoreBackend must be a tuple with fixed length based on the filepath_template,
    or a variable-length tuple may be used and returned with an optional filepath_suffix (to be) added.
    The filepath_template is a string template used to convert the key to a filepath.
    """

    def __init__(
        self,
        base_directory,
        filepath_template=None,
        filepath_prefix=None,
        filepath_suffix=None,
        forbidden_substrings=None,
        platform_specific_separator=True,
        root_directory=None,
        fixed_length_key=False,
    ):
        super().__init__(
            filepath_template=filepath_template,
            filepath_prefix=filepath_prefix,
            filepath_suffix=filepath_suffix,
            forbidden_substrings=forbidden_substrings,
            platform_specific_separator=platform_specific_separator,
            fixed_length_key=fixed_length_key,
        )
        if os.path.isabs(base_directory):
            self.full_base_directory = base_directory
        else:
            if root_directory is None:
                raise ValueError(
                    "base_directory must be an absolute path if root_directory is not provided"
                )
            elif not os.path.isabs(root_directory):
                raise ValueError(
                    "root_directory must be an absolute path. Got {0} instead.".format(
                        root_directory
                    )
                )
            else:
                self.full_base_directory = os.path.join(root_directory, base_directory)

        safe_mmkdir(str(os.path.dirname(self.full_base_directory)))

    def _get(self, key):
        filepath = os.path.join(
            self.full_base_directory, self._convert_key_to_filepath(key)
        )
        with open(filepath, "r") as infile:
            return infile.read()

    def _set(self, key, value, **kwargs):
        if not isinstance(key, tuple):
            key = key.to_tuple()
        filepath = os.path.join(
            self.full_base_directory, self._convert_key_to_filepath(key)
        )
        path, filename = os.path.split(filepath)

        safe_mmkdir(str(path))
        with open(filepath, "wb") as outfile:
            if isinstance(value, string_types):
                # Following try/except is to support py2, since both str and bytes objects pass above condition
                try:
                    outfile.write(value.encode("utf-8"))
                except UnicodeDecodeError:
                    outfile.write(value)
            else:
                outfile.write(value)
        return filepath

    def list_keys(self, prefix=()):
        key_list = []
        for root, dirs, files in os.walk(
            os.path.join(self.full_base_directory, *prefix)
        ):
            for file_ in files:
                full_path, file_name = os.path.split(os.path.join(root, file_))
                relative_path = os.path.relpath(full_path, self.full_base_directory,)
                if relative_path == ".":
                    filepath = file_name
                else:
                    filepath = os.path.join(relative_path, file_name)

                if self.filepath_prefix and not filepath.startswith(
                    self.filepath_prefix
                ):
                    continue
                elif self.filepath_suffix and not filepath.endswith(
                    self.filepath_suffix
                ):
                    continue
                key = self._convert_filepath_to_key(filepath)
                if key and not self.is_ignored_key(key):
                    key_list.append(key)

        return key_list

    def get_url_for_key(self, key, protocol=None):
        path = self._convert_key_to_filepath(key)
        full_path = os.path.join(self.full_base_directory, path)
        if protocol is None:
            protocol = "file:"
        url = protocol + "//" + full_path

        return url

    def _has_key(self, key):
        return os.path.isfile(
            os.path.join(self.full_base_directory, self._convert_key_to_filepath(key))
        )


class TupleS3StoreBackend(TupleStoreBackend):
    """
    Uses an S3 bucket as a store.

    The key to this StoreBackend must be a tuple with fixed length based on the filepath_template,
    or a variable-length tuple may be used and returned with an optional filepath_suffix (to be) added.
    The filepath_template is a string template used to convert the key to a filepath.
    """

    def __init__(
        self,
        bucket,
        prefix="",
        filepath_template=None,
        filepath_prefix=None,
        filepath_suffix=None,
        forbidden_substrings=None,
        platform_specific_separator=False,
        fixed_length_key=False,
    ):
        super().__init__(
            filepath_template=filepath_template,
            filepath_prefix=filepath_prefix,
            filepath_suffix=filepath_suffix,
            forbidden_substrings=forbidden_substrings,
            platform_specific_separator=platform_specific_separator,
            fixed_length_key=fixed_length_key,
        )
        self.bucket = bucket
        self.prefix = prefix

    def _get(self, key):
        s3_object_key = os.path.join(self.prefix, self._convert_key_to_filepath(key))

        import boto3

        s3 = boto3.client("s3")
        s3_response_object = s3.get_object(Bucket=self.bucket, Key=s3_object_key)
        return (
            s3_response_object["Body"]
            .read()
            .decode(s3_response_object.get("ContentEncoding", "utf-8"))
        )

    def _set(
        self, key, value, content_encoding="utf-8", content_type="application/json"
    ):
        s3_object_key = os.path.join(self.prefix, self._convert_key_to_filepath(key))

        import boto3

        s3 = boto3.resource("s3")
        result_s3 = s3.Object(self.bucket, s3_object_key)
        if isinstance(value, string_types):
            # Following try/except is to support py2, since both str and bytes objects pass above condition
            try:
                result_s3.put(
                    Body=value.encode(content_encoding),
                    ContentEncoding=content_encoding,
                    ContentType=content_type,
                )
            except TypeError:
                result_s3.put(Body=value, ContentType=content_type)
        else:
            result_s3.put(Body=value, ContentType=content_type)
        return s3_object_key

    def list_keys(self):
        key_list = []

        import boto3

        s3 = boto3.client("s3")

        s3_objects = s3.list_objects(Bucket=self.bucket, Prefix=self.prefix)
        if "Contents" in s3_objects:
            objects = s3_objects["Contents"]
        elif "CommonPrefixes" in s3_objects:
            logger.warning(
                "TupleS3StoreBackend returned CommonPrefixes, but delimiter should not have been set."
            )
            objects = []
        else:
            # No objects found in store
            objects = []

        for s3_object_info in objects:
            s3_object_key = s3_object_info["Key"]
            s3_object_key = os.path.relpath(s3_object_key, self.prefix,)
            if self.filepath_prefix and not s3_object_key.startswith(
                self.filepath_prefix
            ):
                continue
            elif self.filepath_suffix and not s3_object_key.endswith(
                self.filepath_suffix
            ):
                continue
            key = self._convert_filepath_to_key(s3_object_key)
            if key:
                key_list.append(key)

        return key_list

    def get_url_for_key(self, key, protocol=None):
        import boto3

        location = boto3.client("s3").get_bucket_location(Bucket=self.bucket)[
            "LocationConstraint"
        ]
        if location is None:
            location = "s3"
        else:
            location = "s3-" + location
        s3_key = self._convert_key_to_filepath(key)
        return "https://%s.amazonaws.com/%s/%s%s" % (
            location,
            self.bucket,
            self.prefix,
            s3_key,
        )

    def _has_key(self, key):
        all_keys = self.list_keys()
        return key in all_keys


class TupleGCSStoreBackend(TupleStoreBackend):
    """
    Uses a GCS bucket as a store.

    The key to this StoreBackend must be a tuple with fixed length based on the filepath_template,
    or a variable-length tuple may be used and returned with an optional filepath_suffix (to be) added.

    The filepath_template is a string template used to convert the key to a filepath.
    """

    def __init__(
        self,
        bucket,
        prefix,
        project,
        filepath_template=None,
        filepath_prefix=None,
        filepath_suffix=None,
        forbidden_substrings=None,
        platform_specific_separator=False,
        fixed_length_key=False,
    ):
        super().__init__(
            filepath_template=filepath_template,
            filepath_prefix=filepath_prefix,
            filepath_suffix=filepath_suffix,
            forbidden_substrings=forbidden_substrings,
            platform_specific_separator=platform_specific_separator,
            fixed_length_key=fixed_length_key,
        )
        self.bucket = bucket
        self.prefix = prefix
        self.project = project

    def _get(self, key):
        gcs_object_key = os.path.join(self.prefix, self._convert_key_to_filepath(key))

        from google.cloud import storage

        gcs = storage.Client(project=self.project)
        bucket = gcs.get_bucket(self.bucket)
        gcs_response_object = bucket.get_blob(gcs_object_key)
        return gcs_response_object.download_as_string().decode("utf-8")

    def _set(
        self, key, value, content_encoding="utf-8", content_type="application/json"
    ):
        gcs_object_key = os.path.join(self.prefix, self._convert_key_to_filepath(key))

        from google.cloud import storage

        gcs = storage.Client(project=self.project)
        bucket = gcs.get_bucket(self.bucket)
        blob = bucket.blob(gcs_object_key)
        if isinstance(value, string_types):
            # Following try/except is to support py2, since both str and bytes objects pass above condition
            try:
                blob.upload_from_string(
                    value.encode(content_encoding),
                    content_encoding=content_encoding,
                    content_type=content_type,
                )
            except TypeError:
                blob.upload_from_string(value, content_type=content_type)
        else:
            blob.upload_from_string(value, content_type=content_type)
        return gcs_object_key

    def list_keys(self):
        key_list = []

        from google.cloud import storage

        gcs = storage.Client(self.project)

        for blob in gcs.list_blobs(self.bucket, prefix=self.prefix):
            gcs_object_name = blob.name
            gcs_object_key = os.path.relpath(gcs_object_name, self.prefix,)
            if self.filepath_prefix and not gcs_object_key.startswith(
                self.filepath_prefix
            ):
                continue
            elif self.filepath_suffix and not gcs_object_key.endswith(
                self.filepath_suffix
            ):
                continue
            key = self._convert_filepath_to_key(gcs_object_key)
            if key:
                key_list.append(key)

        return key_list

    def get_url_for_key(self, key, protocol=None):
        path = self._convert_key_to_filepath(key)
        return "https://storage.googleapis.com/" + self.bucket + "/" + path

    def _has_key(self, key):
        all_keys = self.list_keys()
        return key in all_keys


class TupleAzureBlobStoreBackend(TupleStoreBackend):
    """
    Uses an Azure Blob as a store.

    The key to this StoreBackend must be a tuple with fixed length based on the filepath_template,
    or a variable-length tuple may be used and returned with an optional filepath_suffix (to be) added.
    The filepath_template is a string template used to convert the key to a filepath.
    
    TODO put this into doc
    You need to setup the connection string environment variable 
    https://docs.microsoft.com/en-us/azure/storage/blobs/storage-quickstart-blobs-python
    """

    # We will use blobclient here
    def __init__(
        self,
        connection_string,
        prefix,
        container,
        filepath_template=None,
        filepath_prefix=None,
        filepath_suffix=None,
        forbidden_substrings=None,
        platform_specific_separator=False,
        fixed_length_key=False,
    ):
        super().__init__(
            filepath_template=filepath_template,
            filepath_prefix=filepath_prefix,
            filepath_suffix=filepath_suffix,
            forbidden_substrings=forbidden_substrings,
            platform_specific_separator=platform_specific_separator,
            fixed_length_key=fixed_length_key,
        )
        self.connection_string = connection_string
        self.prefix = prefix
        self.container = container

    def _get_blob_client(self):

        from azure.storage.blob import BlobServiceClient

        if self.connection_string:
            return BlobServiceClient.from_connection_string(
                self.connection_string
            ).get_container_client(self.container)
        else:
            raise StoreBackend(
                "Unable to initialze BlobClient, connection string and container should be provided"
            )

    def _get(self, key):
        az_blob_key = os.path.join(self.prefix, self._convert_key_to_filepath(key))
        return (
            self._get_blob_client().download_blob(az_blob_key).readall().decode("utf-8")
        )

    def _set(self, key, value, content_encoding="utf-8", **kwargs):
        az_object_key = os.path.join(self.prefix, self._convert_key_to_filepath(key))

        if isinstance(value, string_types):
            # Following try/except is to support py2, since both str and bytes objects pass above condition
            try:
                self._get_blob_client.upload_blob(
                    name=az_object_key, data=value, encoding=content_encoding
                )
            except TypeError:
                self._get_blob_client.upload_blob(name=az_object_key, data=value)
        else:
            self._get_blob_client.upload_blob(name=az_object_key, data=value)
        return az_object_key

    def list_keys(self):
        key_list = []
        for obj in self._get_blob_client().list_blobs(name_starts_with=self.prefix):
            az_blob_key = os.path.relpath(obj.name)
            if self.filepath_prefix and not az_blob_key.startswith(
                self.filepath_prefix
            ):
                continue
            elif self.filepath_suffix and not az_blob_key.endswith(
                self.filepath_suffix
            ):
                continue
            key = self._convert_filepath_to_key(az_blob_key)
            if key:
                key_list.append(key)
        return key_list

    def get_url_for_key(self, key, protocol=None):
        az_obj_key = self._convert_key_to_filepath(key)
        az_obj_path = os.path.join(self.container, self.prefix, az_obj_key)

        return "https://%s.blob.core.windows.net/%s" % (
            self._get_blob_client().account_name,
            az_obj_path,
        )

    def _has_key(self, key):
        all_keys = self.list_keys()
        return key in all_keys
