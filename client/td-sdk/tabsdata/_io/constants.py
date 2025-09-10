#
# Copyright 2025 Tabs Data Inc.
#

from enum import Enum

AZURE_SCHEME = "az"
FILE_SCHEME = "file"
GCS_SCHEME = "gs"
MARIADB_SCHEME = "mariadb"
MYSQL_SCHEME = "mysql"
ORACLE_SCHEME = "oracle"
POSTGRES_SCHEMES = ("postgres", "postgresql")
S3_SCHEME = "s3"

URI_INDICATOR = "://"


# TODO: Consider making this a list calculated at runtime from existing regions.
#   However, since they don't change that often, for now this should be good enough.
class SupportedAWSS3Regions(Enum):
    Ohio = "us-east-2"
    NorthVirginia = "us-east-1"
    NorthCalifornia = "us-west-1"
    Oregon = "us-west-2"
    CapeTown = "af-south-1"
    HongKong = "ap-east-1"
    Hyderabad = "ap-south-2"
    Jakarta = "ap-southeast-3"
    Malaysia = "ap-southeast-5"
    Melbourne = "ap-southeast-4"
    Mumbai = "ap-south-1"
    Osaka = "ap-northeast-3"
    Seoul = "ap-northeast-2"
    Singapore = "ap-southeast-1"
    Sydney = "ap-southeast-2"
    Tokyo = "ap-northeast-1"
    CanadaCentral = "ca-central-1"
    Calgary = "ca-west-1"
    Frankfurt = "eu-central-1"
    Ireland = "eu-west-1"
    London = "eu-west-2"
    Milan = "eu-south-1"
    Paris = "eu-west-3"
    Spain = "eu-south-2"
    Stockholm = "eu-north-1"
    Zurich = "eu-central-2"
    TelAviv = "il-central-1"
    Bahrain = "me-south-1"
    UAE = "me-central-1"
    SaoPaulo = "sa-east-1"
    GovCloudUSEast = "us-gov-east-1"
    GovCloudUSWest = "us-gov-west-1"
