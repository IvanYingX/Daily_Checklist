import pandas as pd
from slack_sdk import WebClient
from utils.GoogleSheetHelper import GoogleSheetHelper as gsh
import yaml
from utils.AgendaHelper import filter_today_events
from utils.AgendaHelper import print_daily_agenda
import datetime
import time

import random

class SlackMessenger:
    '''
    Class to send messages to slack with the groups and the agendas
    for each student

    Attributes:
    ----------
    client: WebClient
        The slack client
    channel_ids: list
        The channel to send the message to

    '''
    def __init__(self, slack_config: str = 'config/slack.yaml') -> None:
        with open(slack_config, 'r') as f:
            self.config = yaml.safe_load(f)

        self.client = WebClient(token=self.config['slack_token'])
        self.channel_ids = self.config['channel_ids']

    def send_message(self, text: str, channel_id) -> None:
        '''
        Sends a message to the slack channel

        text: str
            The message to send
        '''
        self.client.chat_postMessage(channel=channel_id,
                                     text=text)

    def send_messages_to_students(self,
                                  freq: str = 'daily',
                                  credentials: str =
                                  'config/google_credentials.json') -> None:
        '''
        Sends a message to the slack channel

        text: str
            The message to send
        '''
        events_df = get_today_records(credentials)
        today_events = filter_today_events(events_df)
        for channel_id in self.channel_ids:
            name = self.client.conversations_info(channel=channel_id)['channel']['name']
            if freq == 'daily':
                today_events = self.dsh.get_events(1)
                message = print_daily_agenda(name, today_events, room_n=2)
            elif freq == 'weekly':
                weekly_events = self.dsh.get_this_week_events()
                message = print_weekly_agenda(name, weekly_events)
            self.client.chat_postMessage(channel=channel_id, text=message)

    def get_yesterday_messages(self, channel_id: str) -> list:
        '''
        Gets the messages sent yesterday to the slack channel

        channel_id: str
            The channel to get the messages from
        '''
        today = datetime.date.today()
        yesterday = today - datetime.timedelta(days=1)
        time_tuple = yesterday.timetuple()
        timestamp = time.mktime(time_tuple)
        messages = (self.client
                        .conversations_history(channel=channel_id,
                                               oldest=timestamp)['messages'])
        answered = 0
        not_answered = 0
        not_answered_list = []
        for message in messages:
            if 'bot_id' in message:
                if message['bot_id'] == 'B025SUY15LL':
                    if 'reactions' in message:
                        answered += 1
                    elif 'latest_reply' in message:
                        answered += 1
                    else:
                        not_answered += 1
                        not_answered_list.append(message['text']
                                                 .split('(')[0]
                                                 .strip())

        text = (f'From {answered + not_answered} support requests: \n' +
                f'{answered} were answered,\n' +
                f'{not_answered} were not answered\n\n' +
                f'{", ".join(not_answered_list)} are waiting for a reply')
        return text


def get_today_records(google_creds):
    demos_df = (gsh(google_creds,
                    spreadsheet_name='Events',
                    page='Demos')
                .read_content())
    presentations_df = (gsh(google_creds,
                            spreadsheet_name='Events',
                            page='Presentations')
                        .read_content())
    demos_df['Type'] = 'Demo'
    presentations_df['Type'] = 'Presentation'
    return pd.concat([demos_df, presentations_df], axis=0)
