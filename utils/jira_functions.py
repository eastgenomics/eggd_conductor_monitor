import json
import os
from urllib import response
import requests
import sys
from collections import defaultdict
from urllib3.util import Retry

import requests
from requests.adapters import HTTPAdapter
from requests.auth import HTTPBasicAuth


class Jira:
    """
    Jira related functions for getting a sequencing run ticket for a given run
    """

    def __init__(self, queue_url, issue_url, token, email) -> None:
      self.queue_url = queue_url
      self.issue_url = issue_url
      self.token = token
      self.email = email
      self.headers = {
            "Accept": "application/json",
            "Content-Type": "application/json"
        }
      self.auth = HTTPBasicAuth(self.email, self.token)
      self.http = self.create_session()


    def create_session(self):
        """
        Create session adapter object

        Returns
        -------
        session : http session object
        """

        http = requests.Session()
        retries = Retry(total=5, backoff_factor=10, allowed_methods=["POST"])
        http.mount("https://", HTTPAdapter(max_retries=retries))
        return http
      
    def query_all_tickets(self):
      """
      Get all tickets in a queue from a given URL
      
      Returns
      -------
      list : list of all tickets in the queue
      """

      start = 0
      limit = 50
      response_data = []


      while True:
            
            params = {
                "start": start,
                "limit": limit
        }

            response = self.http.get(
                url=self.queue_url,
                headers=self.headers,
                auth=self.auth,
                params=params
            )

            # check if the response request ok, otherwise exit
            if not response.ok:
                sys.exit(1)
            else:
                response = response.json()

            if response["size"] == 0:
                  break
            
            response_data.extend(response["values"])
            start += 50
            
      return response_data
    
    def create_jira_ticket_dict(self, api_response): 
        """
        Create a dictionary of Jira tickets with ticket ID as key and summary as value
    
        Parameters
        ----------
        api_response : list
            list of tickets from Jira API response
    
        Returns
        -------
        dict
            dictionary of Jira tickets with ticket ID as key and summary as value
        """

        jira_ticket_dict = {ticket["id"]: ticket["fields"]["summary"] for ticket in api_response}
        
        return jira_ticket_dict
    
    def filter_tickets_by_run(self, run_id, tickets) -> str:
      """
      Filter a list of tickets to find the one associated with a given run ID

      Parameters
      ----------
      run_id : str
            run ID of the sequencing run
      tickets : list
            list of tickets to filter through

      Returns
      -------
      str
            ticket ID
      """
      run_tickets = [x for x in tickets if run_id in x["fields"]["summary"]]

      return run_tickets

    def add_comment(self, comment, url, ticket=None) -> None:
        """
        Add a comment to a Jira ticket
        
        Parameters
        ----------
        comment : str
            comment to add to the ticket
        url : str
            any url to add after comment
        ticket : str, None by default
            ticket ID
        """
        if ticket is None:
            return
        
        comment_url = f"{self.issue_url}/{ticket}/comment"

        payload = json.dumps(
            {
                "body": {
                    "type": "doc",
                    "version": 1,
                    "content": [
                        {
                            "type": "paragraph",
                            "content": [
                                {"text": f"{comment}", "type": "text"},
                                {
                                    "text": f"{url}",
                                    "type": "text",
                                    "marks": [
                                        {
                                            "type": "link",
                                            "attrs": {"href": f"{url}"},
                                        }
                                    ],
                                },
                            ],
                        }
                    ],
                },
                "properties": [
                    {"key": "sd.public.comment", "value": {"internal": True}}
                ],
            }
        )

        response = self.http.post(
            url=comment_url, data=payload, headers=self.headers, auth=self.auth
        )

        if not response.ok:
            sys.exit(1)                