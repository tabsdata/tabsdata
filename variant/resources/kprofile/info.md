configs/apiserver.storage.yaml:
  storage.mounts:
    help: >
      Each mount maps a logical path prefix onto a storage location given by its
      `uri`. The optional `options` map holds backend-specific settings, keyed by
      the object store configuration names for the mount's `uri` scheme. Every
      value under `options` is treated as a secret: it is never stored in clear
      text and is supplied at runtime through the environment.


      For Amazon S3 (uri: s3://<bucket>/...), the configuration keys are listed at
      https://docs.rs/object_store/0.13.1/object_store/aws/enum.AmazonS3ConfigKey.html:

        - id: S3
          path: /s3
          uri: s3://my-bucket/prefix/
          options:
            aws_region: eu-west-1
            aws_access_key_id: AKIAIOSFODNN7EXAMPLE
            aws_secret_access_key: wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
            aws_endpoint: http://localhost:9000

      For Azure Blob Storage (uri: az://<container>/...), the configuration keys
      are listed at
      https://docs.rs/object_store/0.13.1/object_store/azure/enum.AzureConfigKey.html:

        - id: AZURE
          path: /azure
          uri: az://my-container/prefix/
          options:
            azure_storage_account_name: mystorageaccount
            azure_storage_account_key: bXlfYWNjb3VudF9rZXlfYmFzZTY0X2VuY29kZWQ=

      For Google Cloud Storage (uri: gs://<bucket>/...), the configuration keys
      are listed at
      https://docs.rs/object_store/0.13.1/object_store/gcp/enum.GoogleConfigKey.html:

        - id: GCS
          path: /gcs
          uri: gs://my-bucket/prefix/
          options:
            google_service_account_key: |
              {
                "type": "service_account",
                "project_id": "my-project",
                "private_key_id": "a1b2c3",
                "private_key": "-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----\n",
                "client_email": "svc@my-project.iam.gserviceaccount.com",
                "client_id": "1234567890"
              }

pods/apiserver_kminus.yaml:
  spec.volumes.resources:
    help: >
      This is a writable scratch volume. An init container extracts connector
      icons into the `ui/connectors` subfolder of the mount, and the apiserver
      then serves them over its resources endpoint.

pods/apiserver_docker.yaml:
  spec.volumes.resources:
    help: >
      This is a writable scratch volume. An init container extracts connector
      icons into the `ui/connectors` subfolder of the mount, and the apiserver
      then serves them over its resources endpoint.
