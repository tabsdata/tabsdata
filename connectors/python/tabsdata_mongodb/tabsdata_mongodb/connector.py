#
# Copyright 2025 Tabs Data Inc.
#

import datetime
import json
import logging
import os
from timeit import default_timer as timer
from typing import List, Literal
from urllib.parse import quote_plus

import polars as pl

from tabsdata.credentials import UserPasswordCredentials
from tabsdata.io.plugin import DestinationPlugin
from tabsdata.tabsserver.function.store_results_utils import _get_matching_files

try:
    import pymongo
    from pymongo.errors import ConnectionFailure

    MISSING_LIBRARIES = False
except ImportError:
    MISSING_LIBRARIES = True

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


MONGODB_ID_COLUMN = "_id"


class MongoDBDestination(DestinationPlugin):

    def __init__(
        self,
        uri: str,
        collections_with_ids: tuple[str, str | None] | List[tuple[str, str | None]],
        credentials: UserPasswordCredentials = None,
        connection_options: dict = None,
        if_collection_exists: Literal["append", "replace"] = "append",
        use_trxs: bool = False,
        docs_per_trx: int = 1000,
        maintain_order: bool = False,
        update_existing: bool = True,
        fail_on_duplicate_key: bool = True,
        log_intermediate_files: bool = False,
        **kwargs,
    ):
        """
        Initializes the MongoDBDestination with the configuration desired to store
            the data.

        Args:
            uri (str): The URI of the MongoDB database.
            collections_with_ids (tuple[str, str] | List[tuple[str, str]]): A tuple
                or list of tuples with the collection and the name of the
                field that will be used as the unique identifier. For example, if you
                want to store the data in a collection called 'my_collection' in
                database 'my_database' and use the field 'username' as the unique
                identifier, you would provide the following tuple: (
                'my_database.my_collection', 'username'). If you wanted MongoDB to
                autogenerate the id, you would provide the following tuple:
                ('my_database.my_collection', None).
            credentials (UserPasswordCredentials, optional): The credentials to connect
                with the database. If None, no credentials will be used.
            connection_options(dict, optional): A dictionary with the options to pass
                to the pymongo.MongoClient constructor. For example, if you want to
                set the timeout to 1000 milliseconds, you would provide the following
                dictionary: {'serverSelectionTimeoutMS': 1000}.
            if_collection_exists (Literal["append", "replace"], optional): The action
                to take if the collection already exists. If 'append', the data will be
                appended to the existing collection. If 'replace', the existing
                collection will be replaced with the new data. Defaults to 'append'.
            use_trxs (bool, optional): Whether to use a transaction when storing
                the data in the database. If True, the data will be stored in a
                transaction, which will ensure that all the data is stored or none of
                it is (requires that the database is configured with a replica set). If
                False, the data will be stored without a transaction, which may lead
                to inconsistent data in the database. Defaults to False.
            docs_per_trx (int, optional): The maximum number of documents
                to store in a single transaction. If the number of documents to store
                exceeds this number, the data will be stored in multiple transactions.
            maintain_order (bool, optional): Whether to maintain the order of the
                documents when storing them in the database. If True, the documents
                will be stored in the same order as they are in the TableFrame. If
                False, the documents will be stored in the order that they are
                processed. Defaults to False.
            update_existing (bool, optional): Whether to update the existing documents
                in the database. If True, the documents will be updated if they already
                exist in the database. If False, the documents will be inserted without
                updating the existing documents, and if a document with the same id
                already exists execution will fail. Defaults to True.
            fail_on_duplicate_key (bool, optional): Whether to raise an exception if a
                document with the same id already exists in the collection. If True, an
                exception will be raised. If False, the operation will continue without
                raising an exception. Defaults to True.
            log_intermediate_files (bool, optional): Whether to log when each batch
                of data is stored in the database. If True, a message will be logged for
                each batch of data stored. If False, no message will be logged until all
                the data for a single collection has been stored. Defaults to False.

        """
        if MISSING_LIBRARIES:
            raise ImportError(
                "The 'tabsdata_mongodb' package is missing some dependencies. You "
                "can get them by installing 'tabsdata['mongodb']'"
            )
        self.uri = uri
        self.collections_with_ids = collections_with_ids
        self.credentials = credentials
        self.connection_options = connection_options
        self.if_collection_exists = if_collection_exists
        self.use_trxs = use_trxs
        self.maintain_order = maintain_order
        self.update_existing = update_existing
        self.docs_per_trx = docs_per_trx
        self.fail_on_duplicate_key = fail_on_duplicate_key
        self.log_intermediate_files = log_intermediate_files
        self.kwargs = kwargs
        # We start with the support options, only used when debugging a major issue
        self._suport_start_session = self.kwargs.get("support_start_session", {})
        self._suport_get_collection = self.kwargs.get("support_get_collection", {})
        self._suport_start_transaction = self.kwargs.get(
            "support_start_transaction", {}
        )
        self._suport_commit_transaction = self.kwargs.get(
            "support_commit_transaction", {}
        )
        self._support_abort_transaction = self.kwargs.get(
            "support_abort_transaction", {}
        )
        self._support_insert_one = self.kwargs.get("support_insert_one", {})
        self._support_bulk_write = self.kwargs.get("support_bulk_write", {})
        self._support_update_one = self.kwargs.get("support_update_one", {})

    @property
    def collections_with_ids(self) -> List[tuple[str, str | None]]:
        return self._collections_with_ids

    @collections_with_ids.setter
    def collections_with_ids(self, collections_with_ids):
        if isinstance(collections_with_ids, tuple):
            self._collections_with_ids = [collections_with_ids]
        elif isinstance(collections_with_ids, list):
            self._collections_with_ids = collections_with_ids
        else:
            raise TypeError(
                "The 'collections_with_ids' parameter must be a tuple or a list of "
                f"tuples, got '{type(collections_with_ids)}0 instead."
            )

    @property
    def credentials(self) -> UserPasswordCredentials | None:
        return self._credentials

    @credentials.setter
    def credentials(self, credentials):
        if credentials is None:
            self._credentials = None
        elif isinstance(credentials, UserPasswordCredentials):
            self._credentials = credentials
        else:
            raise TypeError(
                "The credentials must be an instance of UserPasswordCredentials or "
                f"None, got {type(credentials)} instead."
            )

    @property
    def uri(self) -> str:
        return self._uri

    @uri.setter
    def uri(self, uri):
        if not isinstance(uri, str):
            raise TypeError(f"The uri must be a string, got {type(uri)} instead.")
        self._uri = uri

    @property
    def connection_options(self) -> dict:
        return self._connection_options

    @connection_options.setter
    def connection_options(self, connection_options):
        if connection_options is None:
            self._connection_options = {}
        elif isinstance(connection_options, dict):
            self._connection_options = connection_options
        else:
            raise TypeError(
                "The connection_options must be a dictionary or None, "
                f"got {type(connection_options)} instead."
            )

    @property
    def if_collection_exists(self) -> Literal["append", "replace"]:
        return self._if_collection_exists

    @if_collection_exists.setter
    def if_collection_exists(self, if_collection_exists):
        if if_collection_exists not in ["append", "replace"]:
            raise ValueError(
                "The if_collection_exists parameter must be either 'append' or "
                f"'replace', got '{if_collection_exists}' instead."
            )
        self._if_collection_exists = if_collection_exists

    def trigger_output(self, working_dir: str, *args):
        results = args
        if len(results) != len(self.collections_with_ids):
            raise ValueError(
                f"The number of results ({len(results)}) does not match the number of "
                f"collections provided ({len(self.collections_with_ids)}. Please make "
                "sure that the number of results matches the number of collections."
            )
        uri = self.uri
        self.working_dir = working_dir
        if self.credentials:
            logger.debug("Using credentials to connect to the database")
            user = quote_plus(self.credentials.user.secret_value)
            password = quote_plus(self.credentials.password.secret_value)
            uri = uri.replace("://", f"://{user}:{password}@")
        else:
            logger.debug("No credentials provided to connect to the database")
        self.client = pymongo.MongoClient(uri, **self.connection_options)
        # Verify that the client is properly connected
        try:
            # The ping command is cheap and does not require auth.
            self.client.admin.command("ping")
        except ConnectionFailure as e:
            logger.error(
                "Unable to connect to the database. This is most likely due "
                "to an issue with the URI or credentials provided."
            )
            raise Exception(
                "Unable to connect to the database. This is most likely due "
                "to an issue with the URI or credentials provided."
            ) from e
        with self.client.start_session(**self._suport_start_session) as session:
            if not self.use_trxs:
                session = None
            for index, (result, (collection, id_field)) in enumerate(
                zip(results, self.collections_with_ids)
            ):
                if result is None:
                    logger.warning(
                        f"Result for collection '{collection}' is None. Skipping"
                    )
                else:
                    start = timer()
                    self._store_result_in_collection(
                        result, collection, id_field, session, index
                    )
                    end = timer()
                    time_taken = end - start
                    logger.info(
                        f"Time taken to store collection '{collection}': "
                        f"{str(datetime.timedelta(seconds=time_taken))}"
                    )
            logger.info("All results stored")

    def _store_result_in_collection(
        self, result: pl.LazyFrame, collection, id_field, session, index
    ):
        database_name, collection_name = collection.split(".")
        logger.info(
            f"Storing results in collection '{collection_name}' in database "
            f"'{database_name}'"
        )
        database = self.client[database_name]
        collection = database.get_collection(
            collection_name, **self._suport_get_collection
        )
        if self.if_collection_exists == "replace":
            _drop_collection(collection)
        file_name = f"intermediate_{index}"
        file_name += "_{part}.jsonl"
        intermediate_destination_file_pattern = os.path.join(
            self.working_dir, file_name
        )
        logger.debug(f"Sinking the data to {intermediate_destination_file_pattern}")
        result.sink_ndjson(
            pl.PartitionMaxSize(
                intermediate_destination_file_pattern,
                max_size=self.docs_per_trx,
            ),
            maintain_order=True,
        )
        logger.debug(
            f"Loading the data from {intermediate_destination_file_pattern} in "
            f"streaming mode and storing it in the collection '{collection.name}'"
        )
        # Given the pattern that we gave polars.sink_ndjson, we need to get the
        # list of files that match the pattern. In this case, we match the {part}
        # section to a wildcard that can take any value.
        for file in _get_matching_files(
            intermediate_destination_file_pattern.replace("{part}", "*")
        ):
            self._store_and_control_errors_single_file(
                session, file, collection, id_field
            )
        logger.info(f"Results stored in collection '{collection.name}'")

    def _store_and_control_errors_single_file(
        self, session, file, collection, id_field
    ):
        if self.use_trxs:
            session.start_transaction(**self._suport_start_transaction)
        try:
            self._store_single_file_in_collection(file, collection, id_field, session)
            if self.use_trxs:
                if self.log_intermediate_files:
                    logger.debug(
                        f"Commiting transaction for intermediate file '{file}' "
                        f"in collection '{collection.name}'"
                    )
                session.commit_transaction(**self._suport_commit_transaction)
                if self.log_intermediate_files:
                    logger.debug("Transaction commited")
            elif self.log_intermediate_files:
                logger.debug(
                    f"Stored intermediate file '{file}' "
                    f"in collection '{collection.name}'"
                )
        except pymongo.errors.BulkWriteError as e:
            self._process_bulk_write_error(e, collection, session, file)

    def _process_bulk_write_error(self, e, collection, session, file):
        # We check if it is a duplicate key error, and if it is, we do not raise
        # an exception if the 'fail_on_duplicate_key' parameter is false
        if self.use_trxs:
            session.abort_transaction(**self._support_abort_transaction)
            logger.warning(
                f"Transaction aborted for file '{file}' in collection"
                f" '{collection.name}'"
            )
        for error in e.details["writeErrors"]:
            if error["code"] == 11000:
                if self.fail_on_duplicate_key:
                    raise e
                else:
                    logger.warning(
                        "A document with the same id already exists in the "
                        f"collection '{collection.name}', and the "
                        "'fail_on_duplicate_key' parameter is set to False. "
                        "Continuing without raising an exception."
                    )
            else:
                raise e

    def _store_single_file_in_collection(self, file, collection, id_field, session):
        operations = []
        with open(file, "r") as f:
            for line in f:
                document = json.loads(line)
                if id_field is not None:
                    try:
                        document[MONGODB_ID_COLUMN] = document[id_field]
                    except KeyError:
                        raise KeyError(
                            "The field indicated as the id column is not present in "
                            "the document. "
                            "Please make sure that the id field provided in the "
                            "corresponding tuple in "
                            "'collections_with_ids' parameter is present in the "
                            f"document. The collection name is '{collection.name}', "
                            f"and the id column provided is '{id_field}'. However, the "
                            "document only has the following "
                            f"columns: {[key for key in document.keys()]}. If you want "
                            "the id to be autogenerated by MongoDB, provide None as "
                            "the id field in the tuple in 'collections_with_ids'"
                        )
                    if self.update_existing:
                        # If we update existing records, we must use UpdateOne with
                        # upsert=True so that if they exist they are updated, and if
                        # they don't they are inserted
                        operations.append(
                            pymongo.UpdateOne(
                                {MONGODB_ID_COLUMN: document[MONGODB_ID_COLUMN]},
                                {"$set": document},
                                upsert=True,
                                **self._support_update_one,
                            )
                        )
                    else:
                        # If we don't update existing records, we must use insert_one so
                        # that if the record already exists, the operation fails
                        operations.append(
                            pymongo.InsertOne(document, **self._support_insert_one)
                        )
                else:
                    # If MongoDB auto-generates the ID there will never be a
                    # collision, so there is no need to upsert
                    operations.append(
                        pymongo.InsertOne(document, **self._support_insert_one)
                    )
        collection.bulk_write(
            operations,
            session=session,
            ordered=self.maintain_order,
            **self._support_bulk_write,
        )


def _drop_collection(collection):
    # Hack to verify the collection exists
    if collection.count_documents({}):
        logger.debug("Dropping the collection to replace it with the new data")
        collection.drop()
        logger.debug("Collection dropped")
    # If the collection does not exist, we need to insert a document to
    # ensure that the collection is created, since they can't be created
    # inside a transaction
    collection.insert_one({})
    # We need to delete the document we just inserted to ensure that the
    # collection is empty
    collection.delete_one({})
