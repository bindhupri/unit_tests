from offer_bank.database.query_db_v2 import create_connection_pool, close_connection_pool, query_offer_details
from offer_bank.ProcessNewEventV2 import process_new_event
from offer_bank.ProcessExistingActiveOfferV2 import process_updated_offer_event
from offer_bank.DeleteExpiredEventV2 import delete_offers
import offer_bank.utility.common_code_v2 as common_code
from offer_bank.utility import constants
from datetime import datetime
from pyspark.sql.functions import to_timestamp
import offer_bank.bigquery.bigquery_operations as bq_operations
from offer_bank.exceptions.big_query_exception import BigQueryException

timestamp_format = constants.TIMESTAMP_FORMAT

valid_offer_status = {"live", "published", "deleted", "disabled"}
def navigate_event(offer_event, connection_pool):
    offer_id = offer_event["payload"]["promotionNumber"]
    offer_status = offer_event["payload"]["status"]
    offer_timestamp = offer_event["payload"]["eventTimeStamp"]
    offer_timestamp = datetime.strptime(offer_timestamp, '%Y-%m-%dT%H:%M:%S.%fZ').strftime("%Y-%m-%d %H:%M:%S")
    is_event_expired = common_code.check_event_expiry(offer_event)

    db_offer = query_offer_details(str(offer_id), connection_pool)
    print(db_offer)

    print("***************************************************")
    if len(db_offer) == 0:
        if offer_status.lower() == "live" or offer_status.lower() == "published":
            print("Received a new offer event")
            return process_new_event(connection_pool, offer_event)
        else:
            print("Received an invalid offer!")
            return f"Offer {offer_id} is invalid."
    else:
        existing_offer_timestamp = db_offer[0][3]
        offer_timestamp = datetime.strptime(offer_timestamp, '%Y-%m-%d %H:%M:%S')

        if (offer_status.lower() in valid_offer_status) and (not is_event_expired) \
            and (offer_timestamp > existing_offer_timestamp):

            print("Received an update for existing active offer")
            return process_updated_offer_event(connection_pool, offer_event, db_offer)
        elif is_event_expired:
            print("Received an existing expired offer")
            delete_expired_offer = delete_offers(offer_id, connection_pool)
            try:
                """Deleting entry from bq tables for expired offer event"""
                print(f"Deleting offer members for offer_id {offer_id} from BigQuery table.")
                bq_operations.bigquery_delete_member_offers(offer_id)
                print(f"Deleting offer items for offer_id {offer_id} from BigQuery table:")
                bq_operations.bigquery_delete_offer_items(offer_id)
                print(f"Deleting club overrides for offer_id {offer_id} from BigQuery table:")
                bq_operations.bigquery_delete_club_overrides(offer_id)
                print(f"Deleting offer with offer_id {offer_id} from BigQuery table:")
                bq_operations.bigquery_delete_offer(offer_id)
            except (Exception) as error:
                print(f"Error occured while deleting offer details from BigQuery tables: {error}")
                raise BigQueryException(error)
            return delete_expired_offer
        else:
            print("Received an older version of existing offer.")
            return f"Skipping {offer_id} as the offer is having older version"
