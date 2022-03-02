import urllib
import json
import os
from slack_sdk import WebClient

with urllib.request.urlopen("https://aicore-files.s3.amazonaws.com/google_credentials.json") as url:
    google_creds = json.loads(url.read().decode())
room_idx = 4
cell = cohort_cell = 2
group_size = 4
slack_users = os.environ['SLACK_USERS']
slack_client = WebClient(token=slack_users)
group_by = 'project'
users_not_to_message = [
                        # 'U02CKJE2N00', # Carys
                        # 'U02C0ADK9V0', # Wayne
                        # 'U02B7MZSA9L', # Pascal
                        # 'U02C30Q3333', # Miruna
                        # 'U02EB3A7S77', # Eddie Evans
                        # 'U02B5618ZDM', # James Moody
                        'U028BK9MCVC', # Simeon
                        'U02BTHWA90E', # James Deehan
                        'U029199322C', # Victor
                        # 'U028WJETKUZ', # Tamim
                        'U028Q45LABE', # Tafadz
                        'U028SDTK9MX', # Ishtyaq
                        # 'U028Q206UKW', # Jason
                        'U02529PFYAW', # Marianna
                        'U025N3A1TLJ', # Bilal
                        'U02547Y2M39', # Ana-Maria
                        'U025G0P756Z', # Harry Smith
                        'U02JGG4E76V', # Bola
                        'U02F3H2S14G', # Dan Lund
]