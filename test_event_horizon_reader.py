import sys
import mock
import tests.conftest as conftest

sys.modules['offer_bank.utility.secrets'] = mock.MagicMock()
sys.modules['offer_bank.spark_config.spark'] = conftest

import os
import unittest
from unittest.mock import patch, MagicMock
import pytest
from pydantic import ValidationError
from offer_bank.utility import constants
from eventhorizon import Environment
from offer_bank.EventHorizonReader import set_channel_alias, set_event_horizon_env, process_event_data, ItemsPayload, ItemDetails, Award, MembersPayload, Member, Payload


class TestItemDetails(unittest.TestCase):

    def test_valid_item_details(self):
        payload = ItemsPayload(
            itemType="type1",
            itemListType="list",
            itemList=["item1", "item2"],
            itemBigListURL=None,
            refId=1
        )
        item_details = ItemDetails(
            itemListCount=2,
            itemsPayload=[payload]
        )
        self.assertEqual(item_details.itemListCount, 2)
        self.assertEqual(len(item_details.itemsPayload), 1)
        self.assertEqual(item_details.itemsPayload[0].itemType, "type1")

    def test_empty_itemsPayload(self):
        item_details = ItemDetails(
            itemListCount=0,
            itemsPayload=None
        )
        self.assertEqual(item_details.itemListCount, 0)
        self.assertIsNone(item_details.itemsPayload)

    def test_invalid_itemsPayload(self):
        with self.assertRaises(ValidationError):
            ItemsPayload(
                itemType="type1",
                itemListType="invalid",
                itemList=["item1", "item2"],
                itemBigListURL=None,
                refId=3
            )

    def test_item_details_with_invalid_payload(self):
        with self.assertRaises(ValidationError):
            payload = ItemsPayload(
                itemType="type1",
                itemListType="list",
                itemList=[],
                itemBigListURL=None,
                refId=1
            )
            ItemDetails(
                itemListCount=1,
                itemsPayload=[payload]
            )

    def test_item_details_with_biglist_payload(self):
        with self.assertRaises(ValidationError):
            payload = ItemsPayload(
                itemType="type1",
                itemListType="biglist",
                itemList=[],
                itemBigListURL=None,
                refId=1
            )
            ItemDetails(
                itemListCount=1,
                itemsPayload=[payload]
            )

class TestMembersPayload(unittest.TestCase):

    def test_valid_members_payload_list(self):
        payload = MembersPayload(
            memberType="list",
            memberList=["member1", "member2"],
            memberBigListURL=''
        )
        self.assertEqual(payload.memberType, "list")
        self.assertEqual(payload.memberList, ["member1", "member2"])

    def test_valid_members_payload_biglist(self):
        payload = MembersPayload(
            memberType="biglist",
            memberList=[],
            memberBigListURL="http://example.com/biglist"
        )
        self.assertEqual(payload.memberType, "biglist")
        self.assertEqual(payload.memberBigListURL, "http://example.com/biglist")

    def test_invalid_memberType(self):
        with self.assertRaises(ValidationError):
            MembersPayload(
                memberType="invalid",
                memberList=["member1", "member2"],
                memberBigListURL=''
            )

    def test_empty_memberList_for_list(self):
        with self.assertRaises(ValidationError):
            MembersPayload(
                memberType="list",
                memberList=[],
                memberBigListURL=''
            )

    def test_empty_memberBigListURL_for_biglist(self):
        with self.assertRaises(ValidationError):
            MembersPayload(
                memberType="biglist",
                memberList=["member1", "member2"],
                memberBigListURL=''
            )

class TestMember(unittest.TestCase):

    def test_valid_member(self):
        payload = MembersPayload(
            memberType="list",
            memberList=["member1", "member2"],
            memberBigListURL=''
        )
        member = Member(memberListCount=2, membersPayload=[payload])
        self.assertEqual(len(member.membersPayload), 1)
        self.assertEqual(member.membersPayload[0].memberType, "list")

    def test_invalid_member_payload(self):
        with self.assertRaises(ValidationError):
            payload = MembersPayload(
                memberType="list",
                memberList=[],
                memberBigListURL=''
            )
            Member(membersPayload=[payload])


def test_valid_award():
    award = Award(
        awardType=constants.ITEM_DISCOUNT,
        value="10",
        discountMethod=constants.DOLLAR_OFF
    )
    assert award.awardType == constants.ITEM_DISCOUNT
    assert award.value == "10"
    assert award.discountMethod == constants.DOLLAR_OFF

def test_invalid_award():
    with pytest.raises(ValidationError):
        Award(
            awardType=constants.ITEM_DISCOUNT,
            value="10",
            discountMethod="INVALID"
        )

def test_valid_members_payload_list():
    payload = MembersPayload(
        memberType="list",
        memberList=["member1", "member2"],
        memberBigListURL=''
    )
    assert payload.memberType == "list"
    assert payload.memberList == ["member1", "member2"]
    assert payload.memberBigListURL == ''

def test_valid_members_payload_biglist():
    payload = MembersPayload(
        memberType="biglist",
        memberList=[],
        memberBigListURL="http://example.com/biglist"
    )
    assert payload.memberType == "biglist"
    assert payload.memberBigListURL == "http://example.com/biglist"
    assert payload.memberList == []

def test_invalid_member_type():
    with pytest.raises(ValidationError):
        MembersPayload(
            memberType="invalid",
            memberList=["member1", "member2"],
            memberBigListURL=''
        )

def test_empty_member_list_for_list():
    with pytest.raises(ValidationError):
        MembersPayload(
            memberType="list",
            memberList=[],
            memberBigListURL=''
        )

def test_empty_memberBigListURL_for_biglist():
    with pytest.raises(ValidationError):
        MembersPayload(
            memberType="biglist",
            memberList=["member1", "member2"],
            memberBigListURL=''
        )

def test_valid_member():
    payload = MembersPayload(
        memberType="list",
        memberList=["member1", "member2"],
        memberBigListURL=''
    )
    member = Member(memberListCount=2, membersPayload=[payload])
    assert len(member.membersPayload) == 1
    assert member.membersPayload[0].memberType == "list"

def test_invalid_member_payload():
    with pytest.raises(ValidationError):
        payload = MembersPayload(
            memberType="list",
            memberList=[],
            memberBigListURL=''
        )
        Member(membersPayload=[payload])

def test_valid_payload():
    award = Award(
        awardType=constants.ITEM_DISCOUNT,
        value="10",
        discountMethod=constants.DOLLAR_OFF
    )
    payload = Payload(
        eventTimeStamp="2023-01-01T00:00:00Z",
        status=constants.PUBLISHED,
        promotionNumber="PROMO123",
        startDate="2023-01-01",
        endDate="2023-01-10",
        startTime="09:00",
        endTime="18:00",
        timeZone=constants.EST,
        channels=["online", "instore"],
        labels=[constants.TETRIS],
        awardList=[award],
        memberDetails=None,
        itemDetails=None,
        membershipType=["gold", "silver"]
    )
    assert payload.status == constants.PUBLISHED
    assert payload.timeZone == constants.EST

def test_invalid_status_in_payload():
    with pytest.raises(ValidationError):
        Payload(
            eventTimeStamp="2023-01-01T00:00:00Z",
            status="INVALID",
            promotionNumber="PROMO123",
            startDate="2023-01-01",
            endDate="2023-01-10",
            startTime="09:00",
            endTime="18:00",
            timeZone=constants.EST,
            channels=["online", "instore"],
            labels=[constants.TETRIS],
            awardList=[],
            memberDetails=None,
            itemDetails=None,
            membershipType=["gold", "silver"]
        )

def test_invalid_timeZone_in_payload():
    with pytest.raises(ValidationError):
        Payload(
            eventTimeStamp="2023-01-01T00:00:00Z",
            status=constants.PUBLISHED,
            promotionNumber="PROMO123",
            startDate="2023-01-01",
            endDate="2023-01-10",
            startTime="09:00",
            endTime="18:00",
            timeZone=None,
            channels=["online", "instore"],
            labels=[constants.TETRIS],
            awardList=[],
            memberDetails=None,
            itemDetails=None,
            membershipType=["gold", "silver"]
        )


def test_multiple_offers_in_payload():
    with pytest.raises(ValidationError):
        Payload(
            eventTimeStamp="2023-01-01T00:00:00Z",
            status=constants.PUBLISHED,
            promotionNumber="PROMO123",
            startDate="2023-01-01",
            endDate="2023-01-10",
            startTime="09:00",
            endTime="18:00",
            timeZone="CST",
            channels=["online", "instore"],
            labels=[constants.TETRIS, constants.TIO],
            awardList=[],
            memberDetails=None,
            itemDetails=None,
            membershipType=["gold", "silver"]
        )


@patch('offer_bank.EventHorizonReader.send_teams_failure_message')
@patch('offer_bank.EventHorizonReader.send_failure_mail')
@patch('offer_bank.EventHorizonReader.update_offer_event_status')
@patch('offer_bank.EventHorizonReader.UpdateOfferMembersEvent')
@patch('offer_bank.EventHorizonReader.audit_offer_event')
def test_process_event_data(mock_audit_offer_event, mock_update_off_members_event, mock_update_offer_event_status, mock_send_failure_mail, mock_send_teams_failure_message):

    connection_pool = MagicMock()


    event = {
        "offerIdList": ["12345"],
        "memberBigListLocation": ""
    }
    # cant test 231, 233-238, 245,
    process_event_data(event, connection_pool) # 228-231 -> logic is wrong. cant test : 245


    event = {
        "payload": {
            "promotionNumber": "67890",
            "labels": ["tio"]
        }
    }
    process_event_data(event, connection_pool)

    event["payload"]["labels"] = "tetris"
    process_event_data(event, connection_pool)

    event["payload"]["labels"] = "unknown"
    process_event_data(event, connection_pool)

    event['payload'].update({
        'memberDetails': {
            'memberListCount': 0
        }
    })
    process_event_data(event, connection_pool)

    event = {
        "payload": {
            "promotionNumber": "67890",
            "labels": ["join"]
        }
    }
    process_event_data(event, connection_pool)

    event = {
        "payload": {
            "promotionNumber": 143888,
            "labels": ["tio"]
        }
    }
    process_event_data(event, connection_pool)

    event = {
        "payload": {
            "eventTimeStamp": "1",
            "status": "status",
            "promotionNumber": "sd",
            "startDate": "12",
            "endDate": "12",
            "startTime": "12",
            "endTime": "12",
            "timeZone": "CST",
            "channels": [],
            "memberDetails": {},
            "itemDetails": None,
            "membershipType": "",
            "promotionNumber": "",
            "labels": ["tio"],
            "awardList": [
                {
                    "awardType": "DISCOUNT_ITEM_PRICE"
                }
            ]
        }
    }
    process_event_data(event, connection_pool)

    event = {
        "payload": {
            "promotionNumber": "",
            "memberBigListLocation": "",
            "labels": ["tio"],
            "awardList": [
                {
                    "awardType": "DISCOUNT_ITEM_PRICE_1"
                }
            ]
        }
    }
    process_event_data(event, connection_pool)


    from offer_bank.exceptions.cache_insertion_failed import CacheInsertionFailed
    from offer_bank.exceptions.db_connection_failed import DbConnectionFailed
    from offer_bank.exceptions.secret_fetch_failed import SecretFetchFailed
    from offer_bank.exceptions.db_insertion_failed import DbInsertionFailed
    from offer_bank.exceptions.big_query_exception import BigQueryException
    from offer_bank.exceptions.blob_read_exception import BlobReadException
    from offer_bank.exceptions.dataframe_transform_failed import DataframeTransformException

    event = {
        "offerIdList": ["12345"]
    }

    mock_audit_offer_event.side_effect = DataframeTransformException("MockingException")
    process_event_data(event, connection_pool)

    with patch('offer_bank.EventHorizonReader.sys') as mock_sys:
        mock_sys.exit.side_effect = MagicMock()
        mock_audit_offer_event.side_effect = BigQueryException("MockingException")
        process_event_data(event, connection_pool)

    import psycopg2 # :: 253
    with patch('offer_bank.EventHorizonReader.sys') as mock_sys:
        mock_sys.exit.side_effect = MagicMock()
        mock_audit_offer_event.side_effect = psycopg2.DatabaseError("MockingException")
        process_event_data(event, connection_pool)

    with patch('offer_bank.EventHorizonReader.sys') as mock_sys:
        mock_sys.exit.side_effect = MagicMock()
        mock_audit_offer_event.side_effect = DbConnectionFailed("MockingException")
        process_event_data(event, connection_pool)

    mock_audit_offer_event.side_effect = BlobReadException("MockBlobType", "MockURL", "MockingException")
    process_event_data(event, connection_pool)


@patch('offer_bank.EventHorizonReader.send_teams_failure_message')
@patch('offer_bank.EventHorizonReader.send_failure_mail')
@patch('offer_bank.EventHorizonReader.json.loads')
@patch('offer_bank.EventHorizonReader.close_connection_pool')
@patch('offer_bank.EventHorizonReader.process_event_data')
@patch('offer_bank.EventHorizonReader.create_connection_pool')
@patch('offer_bank.EventHorizonReader.fetch_postgres_secrets')
def test_preprocess_event(mock_fetch_postgres_secrets, mock_create_connection_pool, mock_process_event_data, mock_close_connection_pool, mock_json_loads, mock_send_failure_mail, mock_send_teams_failure_message):

    from offer_bank.EventHorizonReader import preprocess_event

    event = b'{"offerIdList": ["12345"], "payload": {"promotionNumber": "54321", "labels": ["test"]}}'       
    preprocess_event(event)

    # raise exception
    mock_json_loads.side_effect = Exception("Json Exception")
    preprocess_event(event)


class TestSetEventHorizonEnv(unittest.TestCase):

    import os

    @patch.dict(os.environ, {'ENV': constants.DEV})
    def test_dev_environment(self):
        self.assertEqual(set_event_horizon_env(), Environment.DEV)

    @patch.dict(os.environ, {'ENV': constants.STAGE})
    def test_stage_environment(self):
        self.assertEqual(set_event_horizon_env(), Environment.STAGE)

    @patch.dict(os.environ, {'ENV': constants.PROD})
    def test_prod_environment(self):
        self.assertEqual(set_event_horizon_env(), Environment.PROD)

    @patch.dict(os.environ, {'ENV': 'unknown'})
    def test_unknown_environment(self):
        self.assertIsNone(set_event_horizon_env())

    @patch.dict(os.environ, {}, clear=True)
    def test_no_environment(self):
        self.assertIsNone(set_event_horizon_env())


class TestSetChannelAlias(unittest.TestCase):

    @patch.dict(os.environ, {'ENV': constants.DEV})
    def test_dev_environment(self):
        self.assertEqual(set_channel_alias(), constants.OB_DEV_CHANNEL_ALIAS)

    @patch.dict(os.environ, {'ENV': constants.STAGE})
    def test_stage_environment(self):
        self.assertEqual(set_channel_alias(), constants.QUEST_CHANNEL_ALIAS)

    @patch.dict(os.environ, {'ENV': constants.PROD})
    def test_prod_environment(self):
        self.assertEqual(set_channel_alias(), constants.QUEST_CHANNEL_ALIAS)

    @patch.dict(os.environ, {'ENV': 'unknown'})
    def test_unknown_environment(self):
        self.assertIsNone(set_channel_alias())

    @patch.dict(os.environ, {}, clear=True)
    def test_no_environment(self):
        self.assertIsNone(set_channel_alias())



if __name__ == '__main__':
    unittest.main()
