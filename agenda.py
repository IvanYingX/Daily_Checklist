# %%
import pandas as pd
from utils.GoogleSheetHelper import GoogleSheetHelper as gsh
from utils.AgendaHelper import filter_today_events
from utils.AgendaHelper import print_daily_agenda
from utils.group_helper import get_students_groups, generate_rooms, get_students_info, generate_rooms_by_project, get_students_by_project
import os
import json
import urllib.request
from slack_sdk import WebClient
import tempfile
from datetime import datetime

def clean_dict(group: dict) -> pd.DataFrame:
    group_df = pd.DataFrame(group)
    group_df['Breakout Room'] = room_idx
    columns_of_interest = [
        'project_name',
        'Breakout Room',
        'lesson',
        'milestone',
        'task',
        'preferred_name',
        'last name',
    ]
    df = group_df[columns_of_interest].sort_values(by='preferred_name')
    df.fillna('---', inplace=True)
    return df


def get_today_records():
    demos_df = (gsh(google_creds,
                    spreadsheet_name='Events',
                    page='Demos')
                .read_content())
    q_and_a = (gsh(google_creds,
                    spreadsheet_name='Events',
                    page='Q&A')
                .read_content())
    presentations_df = (gsh(google_creds,
                            spreadsheet_name='Events',
                            page='Presentations')
                        .read_content())
    demos_df['Type'] = 'Demo'
    presentations_df['Type'] = 'Presentation'
    q_and_a['Type'] = 'Q&A'
    return pd.concat([demos_df, presentations_df, q_and_a], axis=0)


def clean_checklist(spreadsheet, retrieve=True):
    if retrieve:
        spreadsheet.retrieve_and_log_daily_results()
    spreadsheet.deep_clean()
    spreadsheet.create_page(checklist)


def populate_checklist(group, daily_checklist, cell, verbose=False):
    group_df = clean_dict(group)
    daily_checklist.update_page(group_df, verbose=verbose)
    daily_checklist.merge(f'B{cell}:B{cell + len(g) - 1}')


checklist = ['Project',
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
slack_token = os.environ['SLACK_INTERNAL']
slack_users = os.environ['SLACK_USERS']
slack_client = WebClient(token=slack_users)

# %%
if __name__ == '__main__':
    room_idx = 3
    cell = cohort_cell = 2
    # Get the list of students and their corresponding groups


    people, project_names = get_students_by_project(sql_creds)

    # Get the list of events from the google sheet
    records_df = get_today_records()

    # Filter events that are taking place today
    today_events = filter_today_events(records_df)

    # Get a string version of the agenda
    daily_agenda = print_daily_agenda(today_events)

    # Clean the daily checklist spreadsheet
    print('Cleaning the spreadsheet')
    daily_checklist = gsh(google_creds,
                        spreadsheet_name='Daily Checklist',
                        page='Main')
    clean_checklist(daily_checklist, retrieve=False)
    print('Spreadsheet cleaned!')

    for project_name in project_names:

        text, new_room_idx, groups = generate_rooms_by_project(people,
                                                    project_name,
                                                    room_idx,
                                                    group_size=4,
                                                    today_events=today_events,
                                                    slack_client=slack_client)
    
        for g in groups:
            populate_checklist(g, daily_checklist, cell)
            room_idx += 1
            cell += len(g)
        room_idx = new_room_idx

        daily_checklist.merge(f'A{cohort_cell}:A{cell -1}')
        cohort_cell = cell
        # with tempfile.TemporaryDirectory() as tmpdir:
        #     with open(f'{tmpdir}/{group_name}.txt', 'w') as f:
        #         f.write(text)
        #     slack_client.files_upload(channels='G01GCULFZH6',file=f'{tmpdir}/{group_name}.txt')


# %%
