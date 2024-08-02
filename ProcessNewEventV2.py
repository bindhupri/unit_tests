from pyspark.sql.functions import lit, col, concat_ws, flatten
from offer_bank.spark_config.spark import get_spark
from offer_bank.database.db_operations import insert_into_database
from offer_bank.database.query_db_v2 import delete_unprocessed_offer
import offer_bank.bigquery.bigquery_operations as bq_operations
from offer_bank.blob_storage.blob_operations import read_from_blob_v2
import offer_bank.utility.common_code_v2 as common_code
from offer_bank.exceptions.cache_insertion_failed import CacheInsertionFailed
from offer_bank.exceptions.db_insertion_failed import DbInsertionFailed
from offer_bank.exceptions.blob_read_exception import BlobReadException
from offer_bank.exceptions.big_query_exception import BigQueryException
from offer_bank.exceptions.dataframe_transform_failed import DataframeTransformException
import offer_bank.utility.map_creation as map_creation
import offer_bank.cache.offer_cache as offer_cache
import offer_bank.cache.member_cache as member_cache
from pyspark.sql import Row
from pyspark.sql.types import StringType, IntegerType, DoubleType, LongType, ArrayType
from offer_bank.utility import constants
from time import time
import sys

spark = get_spark()

def delete_unprocessed_offer_from_db_and_bigquery(offer_id, connection_pool):
    delete_unprocessed_offer(offer_id, connection_pool)
    bq_operations.bigquery_delete_offer(offer_id)
    bq_operations.bigquery_delete_offer_items(offer_id)

def insert_offer_and_offer_items_in_db(offers_df, offer_item_df, club_override_df, offer_id, connection_pool):
    try:
        print("Inserting offer in offers table in the database:")
        insert_into_database('offers',offers_df)

        print("Inserting items in offer_items table in the database:")
        insert_into_database('offer_items_v2', offer_item_df)

        if not club_override_df.rdd.isEmpty():
            print("Inserting cluboverrides in offer_club_overrides table in the database:")
            insert_into_database('club_overrides', club_override_df)
    except (Exception) as error:
            delete_unprocessed_offer(offer_id, connection_pool)
            print(f"Error occurred while inserting offer details in db: {error}")
            raise DbInsertionFailed(error)

def insert_offer_and_offer_items_in_bigquery(offers_df, offer_item_df, club_override_df, offer_id, connection_pool):
    try:
        print("Inserting offer in offers table of BigQuery:")
        bq_operations.insert_into_bigquery('offers',offers_df)

        print("Inserting items in offer_items table of BigQuery:")
        bq_operations.insert_into_bigquery('offer_items', offer_item_df)

        if not club_override_df.rdd.isEmpty():
            print("Inserting cluboverrides in offer_club_overrides table of BigQuery:")
            bq_operations.insert_into_bigquery('offer_club_overrides', club_override_df)
    except (Exception) as error:
            delete_unprocessed_offer_from_db_and_bigquery(offer_id, connection_pool)
            print(f"Error occurred while inserting offer details in BigQuery: {error}")
            raise BigQueryException(error)

def insert_offer_and_offer_items_in_cache(items_df, event_df, club_override_df, offer_id, connection_pool):
    items_list = items_df.select('item_number').where(items_df.item_type == constants.DISCOUNTED_ITEM).rdd.flatMap(lambda x: x).collect()
    eligible_items_list = items_df.select('item_number').where(items_df.item_type == constants.ELIGIBLE_ITEM).rdd.flatMap(lambda x: x).collect()

    print("Inserting offer details in offer cache:")
    offer_cache_map = map_creation.create_offer_cache_map_v2(event_df, club_override_df, offer_id, items_list, eligible_items_list)

    try:
        print('Inserting offer in cache :')
        offer_cache.insert_into_cache(offer_cache_map)
    except (Exception) as error:
        print(f"Error occurred while inserting offer in Cache: {error}")
        delete_unprocessed_offer_from_db_and_bigquery(offer_id, connection_pool)
        raise CacheInsertionFailed(constants.OFFER_CACHE)

def insert_member_offer_in_db(member_item_df, offer_id, connection_pool):
    try:
        print("Inserting members list in member_offers table in the database:")
        insert_into_database('member_offers', member_item_df)
    except (Exception) as error:
        delete_unprocessed_offer_from_db_and_bigquery(offer_id, connection_pool)
        print(f"Error inserting data to database: {error}")
        raise DbInsertionFailed(error)

def insert_member_offer_in_bigquery(member_item_df, offer_id, connection_pool):
    try:
        print("Inserting members list in member_offers table in BigQuery:")
        bq_operations.insert_into_bigquery('member_offers', member_item_df)
    except (Exception) as error:
        delete_unprocessed_offer_from_db_and_bigquery(offer_id, connection_pool)
        print(f"Error inserting data to BigQuery: {error}")
        raise BigQueryException(error)

def insert_member_offer_in_cache(members_uuid_list_df, offer_id, connection_pool):

    """Creating member-offer mapping"""
    member_offers_cache_map = map_creation.create_member_offers_cache_map(members_uuid_list_df)

    try:
        print('Inserting (membership_uuid => offer_list) mapping in cache :')
        member_cache.insert_into_cache(member_offers_cache_map)
    except (Exception) as error:
        print(f"Error occurred while inserting the member offers in Cache: {error}")
        delete_unprocessed_offer_from_db_and_bigquery(offer_id, connection_pool)
        bq_operations.bigquery_delete_member_offers(offer_id)
        raise CacheInsertionFailed(constants.MEMBER_CACHE)

def insert_broadreach_offer_in_cache(offer_id, connection_pool):
    print("Inserting  broadreach offer_id's list in member cache:")
    br_offer_list_cache_map = map_creation.create_br_offer_list_cache_map()
    try:
        member_cache.insert_into_cache(br_offer_list_cache_map)
    except (Exception) as error:
        print(f"Error occurred while inserting broadreach in member Cache: {error}")
        delete_unprocessed_offer_from_db_and_bigquery(offer_id, connection_pool)
        raise CacheInsertionFailed(constants.MEMBER_CACHE)

def create_member_item_df(event_df, offer_source):
    member_list_location = event_df.first()['payload']['memberDetails']['membersPayload'][0]['memberBigListURL']

    print('Fetching memberships from blob storage :')
    try:
        members_df = read_from_blob_v2(member_list_location, offer_source, constants.MEMBER_BLOB)
        print("-------------------------------------------members_df DataFrame-------------------------------------------")
        members_df.printSchema()
#         members_df.show(truncate=False)
    except Exception as e:
        raise BlobReadException(constants.MEMBER_BLOB, member_list_location, e)

    if(offer_source.casefold() == 'Tetris'.casefold()):
        members_df = members_df.withColumnRenamed('MembershipNumber','membership_id')\
                     .withColumnRenamed('Propensity','propensity')
    else:
        members_df = members_df.toDF('membership_id')

    print("-------------------------------------------members_df DataFrame-------------------------------------------")
    members_df.printSchema()
#     members_df.show(truncate=False)

#     discount_type = event_df.first()['discountType']
    print('Getting membership number to membership uuid mapping for the following members:')
    try:
        members_uuid_df = common_code.get_membership_uuid_from_membership_id(members_df, offer_source)
    except BigQueryException as e:
        raise BigQueryException(e)

    """Adding offer_id to the members_uuid_df"""
    offer_id = event_df.first()["payload"]["promotionNumber"]
    member_item_df = members_uuid_df.withColumn("offer_id", lit(offer_id))

    members_uuid_list_df = members_uuid_df.select(col("membership_uuid")).cache()

    print("-------------------------------------------members_uuid_list_df DataFrame-------------------------------------------")
    members_uuid_list_df.printSchema()
#     members_uuid_list_df.show(truncate=False)
    print("-------------------------------------------member_item_df DataFrame-------------------------------------------")
    member_item_df.printSchema()
#     member_item_df.show(truncate=False)

    return members_uuid_list_df, member_item_df

def process_new_event(connection_pool, event):
    process_start = time()
    if "tetris" in event["payload"]["labels"]:
        event["payload"]["offerSource"] = "TETRIS"
    elif "tio" in event["payload"]["labels"]:
        event["payload"]["offerSource"] = "TIO"
    else:
        event["payload"]["offerSource"] = "BROADREACH"
    offer_source = event["payload"]["offerSource"]
    offer_id = event["payload"]["promotionNumber"]
    timezone = event["payload"]["timeZone"]

    """Preprocess date and time before creating dataframe"""
    event["payload"]["startDate"] = event["payload"]["startDate"] + " " + event["payload"]["startTime"]
    event["payload"]["endDate"] = event["payload"]["endDate"] + " " + event["payload"]["endTime"]
    event["payload"]["eventTimeStamp"] = event["payload"]["eventTimeStamp"][:19].replace("T", " ")

    """Null check for timezone and setting default value to CST"""
    if timezone == "":
        event["payload"]["timeZone"] = "CST"

    """Resolve club list data"""
    event = common_code.resolve_club_list(event, offer_source)

    """Process the incoming event as map to dataframe"""
    print(f"Process the incoming event as map to dataframe - {event}")
    try:
        event_df = common_code.process_event(event)
    except (Exception) as error:
        raise DataframeTransformException(error)

    """Generate offer and items dataframe and insert into the db"""
    offers_df = common_code.create_offer_df(event_df)
    print("-------------------------------------------offers_df DataFrame-------------------------------------------")
    offers_df.printSchema()
#     offers_df.show()
    items_df, offer_item_df = common_code.create_offer_item_df(event_df)
    print("-------------------------------------------items_df DataFrame-------------------------------------------")
    items_df.printSchema()
#     items_df.show()
    print("-------------------------------------------offer_item_df DataFrame-------------------------------------------")
    offer_item_df.printSchema()
#     offer_item_df.show()
    club_override_df = common_code.create_club_override_df(event_df)
    print("-------------------------------------------club_override_df DataFrame-------------------------------------------")
    club_override_df.printSchema()
#     club_override_df.show()


    insert_offer_and_offer_items_in_db(offers_df, offer_item_df, club_override_df, offer_id, connection_pool)
    insert_offer_and_offer_items_in_bigquery(offers_df, offer_item_df, club_override_df, offer_id, connection_pool)
    insert_offer_and_offer_items_in_cache(items_df, event_df, club_override_df, offer_id, connection_pool)

    if(offer_source.casefold() == constants.BROADREACH):
        insert_broadreach_offer_in_cache(offer_id, connection_pool)
    else:
        try:
            members_uuid_list_df, member_item_df = create_member_item_df(event_df, offer_source)
        except BlobReadException as e:
            delete_unprocessed_offer_from_db_and_bigquery(offer_id, connection_pool)
            raise BlobReadException(constants.MEMBER_BLOB, event_df.first()['memberBigListLocation'], e)
        except BigQueryException as e:
            delete_unprocessed_offer_from_db_and_bigquery(offer_id, connection_pool)
            raise BigQueryException(e)
        except (Exception) as error:
            delete_unprocessed_offer_from_db_and_bigquery(offer_id, connection_pool)
            print(f"Error occurred while processing memberlist: {error}")
            raise Exception(f"Error occurred while processing memberlist: {error}")

        member_item_df = member_item_df.withColumn("membership_id",col("membership_id").cast(StringType()))
        member_item_df = member_item_df.withColumn("offer_id",col("offer_id").cast(IntegerType()))

        insert_member_offer_in_db(member_item_df, offer_id, connection_pool)
        insert_member_offer_in_bigquery(member_item_df, offer_id, connection_pool)
        insert_member_offer_in_cache(members_uuid_list_df, offer_id, connection_pool)

    spark.catalog.clearCache()
    process_end = time()
    print(f"Runtime for processing new event is {process_end - process_start} seconds")
    return f"{offer_source} offer {offer_id} processed successfully."
