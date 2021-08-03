import sys
import unittest
from unittest import mock
import asyncio

assert sys.version_info >= (3, 0) # Bomb out if not running Python3

from asl_workflow_engine.rest_api import RestAPI as sync_RestAPI
from asl_workflow_engine.rest_api_asyncio import RestAPI as async_RestAPI

class health_checker(unittest.TestCase):

    # Setup to mock necessary components
    def setUp(self):
        self.event_dispatcher = mock.MagicMock()
        self.session = mock.MagicMock()
        # Default to True, changed in false tests
        self.session.is_open = mock.MagicMock(return_value=True)
        self.event_dispatcher.session = self.session

        # Default to sync RestAPI, changed in async tests
        self.rest_api = sync_RestAPI(mock.MagicMock(), self.event_dispatcher, {})

    # Test for sync session open 
    #   No value changed
    def test_session_open(self):
        app = self.rest_api.create_app()
        test_rest_client = app.test_client()
        Response = test_rest_client.get("/health")
        self.assertEqual(Response.status_code, 200)

    # Test for sync session closed
    #   Session channel open value changed
    def test_session_closed(self):
        self.session.is_open = mock.MagicMock(return_value=False)
        app = self.rest_api.create_app()
        test_rest_client = app.test_client()
        Response = test_rest_client.get("/health")
        self.assertEqual(Response.status_code, 503)        

    # Test for async session closed 
    #   Sesssion channel open value changed
    #   API mock changed to async
    def test_async_session_closed(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self.session.is_open = mock.MagicMock(return_value=False)
        self.rest_api = async_RestAPI(mock.MagicMock(), self.event_dispatcher, {})
        app = self.rest_api.create_app()
        test_rest_client = app.test_client()
        Response = asyncio.run(test_rest_client.get("/health"))
        self.assertEqual(Response.status_code, 503) 


    # Test for async session open
    #   API mock changed to async
    def test_async_session_open(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self.rest_api = async_RestAPI(mock.MagicMock(), self.event_dispatcher, {})
        app = self.rest_api.create_app()
        test_rest_client = app.test_client()
        Response = asyncio.run(test_rest_client.get("/health"))
        self.assertEqual(Response.status_code, 200) 

    if __name__ == '__main__':
        unittest.main()