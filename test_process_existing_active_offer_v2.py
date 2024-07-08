import pytest
import sys
import mock
from offer_bank.exceptions.db_insertion_failed import DbInsertionFailed
from offer_bank.exceptions.blob_read_exception import BlobReadException
from offer_bank.exceptions.dataframe_transform_failed import DataframeTransformException
# import offer_bank.bigquery.bigquery_operations as bq_operations
from offer_bank.exceptions.big_query_exception import BigQueryException
from sams_offer_bank.offer_bank.exceptions.big_query_exception import BigQueryException
from sams_offer_bank.offer_bank.exceptions.dataframe_transform_failed import DataframeTransformException
import tests.conftest as conftest

sys.modules['offer_bank.utility.secrets'] = mock.MagicMock()
sys.modules['offer_bank.spark_config.spark'] = conftest

import offer_bank.ProcessExistingActiveOfferV2 as update_event
from pyspark.sql.types import StringType, Row
from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql.types import StructType,StructField, StringType

#   57-58, 74-75, 104-106, 110-112, 116-118, 123-125, 129-131, 142-143

@pytest.mark.usefixtures("spark_session")
def test_process_updated_br_offer_list_against_list_update(spark_session, mocker):
    event = {
           "offerId": 1210,
          "payload": {
            "version": "1.0",
            "eventTimeStamp": "2023-11-13T14:07:03.072Z",
            "status": "Published",
            "promotionNumber": "14693833",
            "promotionName": "DairyCampaignUpdated2",
            "startDate": "2023-10-17",
            "endDate": "2023-11-30",
            "startTime": "13:00:00",
            "endTime": "23:59:59",
            "offerSource": "BROADREACH",
            "timeZone": "CST",
            "clubLocalTime": True,
            "salesBranding": {
              "salesBrandingBadge": "default_IS_branding",
              "foregroundColor": "#FFFFFF",
              "backgroundColor": "#b00000",
              "outlineColor": "#b00000",
              "cornerRadius": "2"
            },
            "multimatchType": "SIMPLE",
            "channels": [
              "CLUB",
              "CPU"
            ],
            "labels": [],
            "membershipType": [
              "ALL"
            ],
            "createdBy": "VN56B8I",
            "modifiedBy": "S0K08LG",
            "itemDetails": {
              "itemListCount": 1,
              "itemsPayload": [
                {
                  "itemType": "discountedItems",
                  "mpq": "1",
                  "itemListType": "list",
                  "itemList": [
                    "635921"
                  ],
                  "refId": 1,
                  "repeatable": False
                }
              ]
            },
            "clubDetails": {
              "clubListCount": 1,
              "clubsPayload": [
                {
                  "clubType": "Standard",
                  "clubList": [],
                  "clubBigListURL": "ALL",
                  "clubInclusion": True
                }
              ],
              "clubOverride": [
                {
                  "clubNumber": "4749",
                  "startDate": "2023-09-02",
                  "startTime": "15:00:00",
                  "endDate": "2023-09-30",
                  "endTime": "19:00:00",
                  "timeZone": "CST"
                }
              ]
            },
            "awardList": [
              {
                "promotionItemNumber": "980443813",
                "gs1Code": "409804438130",
                "awardType": "DISCOUNT_ITEM_PRICE",
                "value": "1",
                "discountMethod": "DOLLAR_OFF_EACH",
                "fundingPercent": {
                  "vendorFundingPercent": "0.00",
                  "memberFundingPercent": "0.00",
                  "samsFundingPercent": "100.0"
                },
                "itemDiscount": "1",
                "itemRefId": [
                  1
                ]
              }
            ],
            "isUpdate": True,
            "created": "2023-10-30T05:53:37.017Z",
            "lastModified": "2023-11-13T14:07:03.072Z"
          }
        }

    item_list = [9801212, 9802125, 9807235]
    l = map(lambda x : Row(x), item_list)
    item_data = [
      Row(item_number=1952722234, item_type="EligibleItem"),
      Row(item_number=1976814607, item_type="EligibleItem"),
      Row(item_number=1929557463, item_type="EligibleItem"),
      Row(item_number=1913024673, item_type="EligibleItem"),
      Row(item_number=1965299361, item_type="EligibleItem"),
      Row(item_number=1931567481, item_type="DiscountedItem"),
      Row(item_number=1930989934, item_type="DiscountedItem"),
      Row(item_number=1960598310, item_type="DiscountedItem"),
      Row(item_number=990313391, item_type="DiscountedItem"),
    ]

    item_list_DF = spark_session.createDataFrame(item_data)

    club_list = [9801, 9802, 9807]
    l1 = map(lambda x : Row(x), club_list)
    club_list_DF = spark_session.createDataFrame(l1,['club'])

    existing_offer_data = [
        ("1121", "1239", "campaign_98", "LIVE", "2022-09-01 09:07:21", "2022-12-01 09:07:21", "CST", "BROADREACH", "$offdiscount", "10.000", "[CPU, D2H, DFC]", "[610994, 130459]", "new_location", "Megaoff", "", "2022-12-01 09:07:21", "2022-12-01 09:07:21", "2022-12-01 09:07:21", "[SAVINGS]", "234")
    ]
    offer_schema = ["offer_id","campaign_id","campaign_name","status","start_datetime","end_datetime","time_zone","offer_source","discount_type","discount_value","applicable_channel","club_list","member_list_location","member_list_name","item_list_location","create_datetime","modified_datetime","event_timestamp","membership_type","offer_set_id"]
    existing_offer_DF =  spark_session.createDataFrame(data=existing_offer_data, schema = offer_schema)

    #offer_details = [(14693833, 'TETRIS', '')]
    
    mocker.patch('offer_bank.utility.common_code_v2.resolve_club_list', return_value= '')
    mocker.patch('offer_bank.utility.common_code_v2.process_event', return_value= item_list_DF)
    mocker.patch('offer_bank.utility.common_code_v2.create_offer_df', return_value= existing_offer_DF)
    mocker.patch('offer_bank.utility.common_code_v2.create_offer_item_df', return_value= (item_list_DF,existing_offer_DF))
    mocker.patch('offer_bank.utility.common_code_v2.create_club_override_df', return_value= item_list_DF)
    mocker.patch('offer_bank.bigquery.bigquery_operations.bigquery_delete_offer', return_value = '')
    mocker.patch('offer_bank.bigquery.bigquery_operations.bigquery_delete_club_overrides', return_value = '')
    mocker.patch('offer_bank.bigquery.bigquery_operations.insert_into_bigquery', return_value = '')
    mocker.patch('offer_bank.bigquery.bigquery_operations.bigquery_delete_offer_items', return_value = '')
    mocker.patch('offer_bank.ProcessExistingActiveOfferV2.read_database', return_value= existing_offer_DF )
    mocker.patch('offer_bank.ProcessExistingActiveOfferV2.delete_club_overrides', return_value= existing_offer_DF )
    mocker.patch('offer_bank.ProcessExistingActiveOfferV2.update_offer_details', return_value= '')
    mocker.patch('offer_bank.ProcessExistingActiveOfferV2.insert_into_database', return_value= '')
    mocker.patch('offer_bank.ProcessExistingActiveOfferV2.delete_items', return_value= '')
    mocker.patch('offer_bank.utility.map_creation.create_offer_cache_map_v2', return_value= '')


    offer_details = [(14693833, 'InvalidOfferSource', '')]
    mocker.patch('offer_bank.database.query_db_v2.query_offer', return_value= offer_details)
    assert update_event.process_updated_offer_event("connection_pool", event, offer_details) == "Invalid update payload. Offer source InvalidOfferSource of offer 14693833 cannot be updated once created."



    event["payload"]["timeZone"] = ""
    event["payload"]["offerSource"] = "TETRIS"
    event["payload"]["labels"] = ["tetris"]
    offer_details = [(14693833, 'TETRIS', '')]
    mocker.patch('offer_bank.database.query_db_v2.query_offer', return_value= offer_details)

    assert update_event.process_updated_offer_event("connection_pool", event, offer_details) == "Offer 14693833 updated successfully."

    event["payload"]["offerSource"] = "TIO"
    event["payload"]["labels"] = ["tio"]
    offer_details = [(14693833, 'TIO', '')]
    mocker.patch('offer_bank.database.query_db_v2.query_offer', return_value= offer_details)

    assert update_event.process_updated_offer_event("connection_pool", event, offer_details) == "Offer 14693833 updated successfully."
    
    event["payload"]["offerSource"] = "BROADREACH"
    event["payload"]["labels"] = [""]
    offer_details = [(14693833, 'BROADREACH', '')]
    mocker.patch('offer_bank.database.query_db_v2.query_offer', return_value= offer_details)

    assert update_event.process_updated_offer_event("connection_pool", event, offer_details) == "Offer 14693833 updated successfully."


    # mocker.patch('offer_bank.utility.common_code_v2.process_event', side_effect= Exception('mocked error'))
    # with pytest.raises(Exception) as pytest_wrapped_e:
    #     update_event.process_updated_offer_event("connection_pool", event, offer_details) 
    #     assert pytest_wrapped_e.type == DataframeTransformException
    #     assert pytest_wrapped_e.value.code == 42



    # mocker.patch('offer_bank.utility.common_code_v2.create_offer_item_df', side_effect= Exception('mocked error'))
    # with pytest.raises(BlobReadException) as pytest_wrapped_e:
    #     update_event.process_updated_offer_event("connection_pool", event, offer_details) 
    #     assert pytest_wrapped_e.type == BlobReadException
    #     #assert pytest_wrapped_e.value.code == 42


    # mocker.patch('offer_bank.bigquery.bigquery_operations.insert_into_bigquery', side_effect= Exception('mocked error'))
    # with pytest.raises(Exception) as pytest_wrapped_e:
    #     update_event.process_updated_offer_event("connection_pool", event, offer_details) 
    #     assert pytest_wrapped_e.type == BigQueryException
    #     #assert pytest_wrapped_e.value.code == 42


    # mocker.patch('offer_bank.database.db_operations.insert_into_database', side_effect= Exception('mocked error'))
    # with pytest.raises(Exception) as pytest_wrapped_e:
    #     update_event.process_updated_offer_event("connection_pool", event, offer_details) 
    #     assert pytest_wrapped_e.type == DbInsertionFailed
    #     #assert pytest_wrapped_e.value.code == 42
