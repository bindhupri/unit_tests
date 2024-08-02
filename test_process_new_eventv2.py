import pytest
import sys
import mock
import tests.conftest as conftest

sys.modules['offer_bank.utility.secrets'] = mock.MagicMock()
sys.modules['offer_bank.spark_config.spark'] = conftest
import offer_bank.ProcessNewEventV2 as new_event
from pyspark.sql.types import StringType, Row
from pyspark.sql.functions import lit, col
from pyspark.sql.types import LongType
from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql.types import StructType,StructField, StringType
    
    
@pytest.mark.usefixtures("spark_session")    
def test_process_new_event_br_success(spark_session, mocker):
    event = {
      "payload": {
        "eventTimeStamp": "2023-09-21T21:45:11.792369Z",
        "status": "Published",
        "promotionNumber": "1203",
        "promotionName": "TestBuyTogether%OffQuestEvents",
        "startDate": "2023-09-22",
        "endDate": "2023-11-11",
        "startTime": "00:00:00",
        "endTime": "23:59:59",
        "timeZone": "CST",
        "isClubLocalTime": True,
        "salesBranding": {
          "salesBrandingBadge": "default_IS_branding"
        },
        "multimatchType": "COMPLEX",
        "channels": [
          "CPU"
        ],
        "labels": [],
        "membershipType": [
          "ALL"
        ],
        "createdBy": "S0K08LG",
        "modifiedBy": "S0K08LG",
        "itemDetails": {
          "itemListCount": 2,
          "itemsPayload": [
            {
              "itemType": "eligibleItems",
              "mpq": "1",
              "itemListType": "list",
              "itemList": [
                "1952722234",
                "1976814607",
                "1929557463",
                "1913024673",
                "1965299361"
              ],
              "repeatable": False,
              "refId": 0
            },
            {
              "itemType": "discountedItems",
              "mpq": "1",
              "itemListType": "list",
              "itemList": [
                "1931567481",
                "1930989934",
                "1960598310",
                "990313391"
              ],
              "repeatable": False,
              "refId": 1
            }
          ]
        },
        "clubDetails": {
          "clubListCount": 1,
          "clubsPayload": [
            {
              "clubType": "Standard",
              "clubList": [],
              "bigListName": "ALL",
              "clubBigListURL": "",
              "clubInclusion": True
            }
          ],
          "clubOverride": [
            {
              "clubNumber": "4949",
              "startDate": "2023-10-22",
              "startTime": "22:22:22",
              "endDate": "2023-10-23",
              "endTime": "23:23:23",
              "timeZone": "CST"
            }
          ]
        },
        "awardList": [
          {
            "promotionItemNumber": "980441637",
            "gs1Code": "409804416374",
            "awardType": "DISCOUNT_ITEM_PRICE",
            "value": "2",
            "discountMethod": "DOLLAR_OFF_EACH",
            "discountLimit": "1",
            "discountSortOrder": "MOST_EXPENSIVE",
            "fundingPercent": {
              "vendorFundingPercent": "0.00",
              "memberFundingPercent": "0.00",
              "samsFundingPercent": "100.0"
            },
            "itemRefId": [0, 1]
          }
        ],
        "isUpdate": False,
        "created": "2023-09-21T21:44:09.088Z",
        "lastModified": "2023-09-21T21:44:48.598Z"
      }
    }
    
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
    item_list_DF = item_list_DF.withColumn("item_number",col("item_number").cast(LongType()))
    
    club_list = [9801, 9802, 9807]
    l1 = map(lambda x : Row(x), club_list)
    club_list_DF = spark_session.createDataFrame(l1,['club_id'])
    
    mocker.patch('offer_bank.ProcessNewEventV2.read_from_blob_v2', return_value= club_list_DF)
    mocker.patch('offer_bank.utility.common_code_v2.read_from_blob_v2', return_value= club_list_DF)
    mocker.patch('offer_bank.utility.common_code_v2.query_club_finder_table', return_value= club_list_DF) 
    mocker.patch('offer_bank.utility.common_code_v2.filter_tire_items', return_value= item_list_DF) 
    mocker.patch('offer_bank.ProcessNewEventV2.insert_into_database', return_value= '')
    mocker.patch('offer_bank.utility.map_creation.create_offer_cache_map', return_value= {1203 :"offer details"})
    mocker.patch('offer_bank.cache.offer_cache.insert_into_cache', return_value= [])
    mocker.patch('offer_bank.utility.map_creation.create_br_offer_list_cache_map', return_value= {"BR_OFFER_IDS" :"BR offer ids"})
    mocker.patch('offer_bank.cache.member_cache.insert_into_cache', return_value= [])
    mocker.patch('offer_bank.bigquery.bigquery_operations.insert_into_bigquery', return_value= '')
    mocker.patch('offer_bank.bigquery.bigquery_operations.bigquery_delete_offer', return_value= '')
    mocker.patch('offer_bank.bigquery.bigquery_operations.bigquery_delete_offer_items', return_value= '')
    mocker.patch('offer_bank.bigquery.bigquery_operations.bigquery_delete_member_offers', return_value= '')

    assert new_event.process_new_event("connection_pool", event) == "BROADREACH offer 1203 processed successfully."
        
