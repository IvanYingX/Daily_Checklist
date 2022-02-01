# %%
import pandas as pd
from utils.GoogleSheetHelper import GoogleSheetHelper as gsh
from utils.AgendaHelper import filter_today_events
from utils.AgendaHelper import print_daily_agenda
from utils.group_helper import get_students_groups, generate_rooms
import os
import json
import urllib.request
from slack_sdk import WebClient
import tempfile
import datetime

def clean_dict(group: dict) -> pd.DataFrame:
    group_df = pd.DataFrame(group)
    group_df['Breakout Room'] = room_idx
    columns_of_interest = [
        'group_name',
        'Breakout Room',
        'lesson',
        'milestone',
        'task',
        'name',
        'last name',
    ]
    df = group_df[columns_of_interest]
    df.fillna('---', inplace=True)
    return df


def get_today_records():
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


def clean_checklist():
    daily_checklist = gsh(google_creds,
                          spreadsheet_name='Daily Checklist',
                          page='Main')
    daily_checklist.retrieve_and_log_daily_results()
    daily_checklist.deep_clean()
    daily_checklist.create_page(checklist)
    return daily_checklist


def populate_checklist(group, daily_checklist, cell, verbose=False):
    group_df = clean_dict(group)
    daily_checklist.update_page(group_df, verbose=verbose)
    daily_checklist.merge(f'B{cell}:B{cell + len(g) - 1}')


checklist = ['Cohort',
             'Breakout Room',
             'Last Lesson',
             'Milestone',
             'Last Task',
             'First Name',
             'Last name',
             'Is here?',
             'Has seen an instructor?',
             'Updated their project tasks',
             'Any comment?']

# Get the credentials for each application

with urllib.request.urlopen("https://aicore-files.s3.amazonaws.com/google_credentials.json") as url:
    google_creds = json.loads(url.read().decode())
sql_creds = {'RDS_PASSWORD': os.environ['RDS_PASSWORD'],
             'RDS_USER': os.environ['RDS_USER'],
             'RDS_HOST': os.environ['RDS_HOST'],
             'RDS_PORT': os.environ['RDS_PORT'],
             'RDS_DATABASE': os.environ['RDS_DATABASE']}
slack_token = os.environ['SLACK_CREDENTIALS']

slack_client = WebClient(token=slack_token)

# %%
if __name__ == '__main__':
    room_idx = 3
    cell = cohort_cell = 2
    # Get the list of students and their corresponding groups
    people, group_names = get_students_groups(sql_creds)
    # Get the list of events from the google sheet
    records_df = get_today_records()
    # Filter events that are taking place today
    today_events = filter_today_events(records_df)
    # Get a string version of the agenda
    daily_agenda = print_daily_agenda(today_events)
    # Clean the daily checklist spreadsheet
    print('Cleaning the spreadsheet')
    daily_checklist = clean_checklist()
    print('Spreadsheet cleaned!')
    for group_name in group_names:
        text, new_room_idx, groups = generate_rooms(people,
                                                    group_name,
                                                    room_idx,
                                                    instructor=False)
        text = daily_agenda + text
        if datetime.today().weekday() == 1:
            text += 'Remember that today Phil runs office hours! If you need advice on your career he will be available from 18:30 to 19:30.\n'
        for g in groups:
            populate_checklist(g, daily_checklist, cell)
            room_idx += 1
            cell += len(g)
        room_idx = new_room_idx
        daily_checklist.merge(f'A{cohort_cell}:A{cell -1}')
        cohort_cell = cell
        with tempfile.TemporaryDirectory() as tmpdir:
            with open(f'{tmpdir}/{group_name}.txt', 'w') as f:
                f.write(text)
            slack_client.files_upload(channels='G01GCULFZH6',file=f'{tmpdir}/{group_name}.txt')


# %%
