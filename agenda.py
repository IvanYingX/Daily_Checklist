# %%
import pandas as pd
from utils.GoogleSheetHelper import GoogleSheetHelper as gsh
from utils.AgendaHelper import filter_today_events
from utils.AgendaHelper import print_daily_agenda
from utils.group_helper import generate_rooms_by, get_students_by
import os
from slack_sdk import WebClient


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
        'email',
    ]
    if group_df['project_name'].nunique() > 1:
        df = group_df[columns_of_interest].sort_values(by='project_name', ascending=False)
    else:
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
    return filter_today_events(pd.concat([demos_df, presentations_df, q_and_a], axis=0))


def clean_checklist(spreadsheet, retrieve, checklist):
    if retrieve:
        spreadsheet.retrieve_and_log_daily_results()
    spreadsheet.deep_clean()
    spreadsheet.create_page(checklist)


def get_checklist(clean_sheet=True, retrieve=True, verbose=False):
    checklist = ['Project',
                'Breakout Room',
                'Last Lesson',
                'Milestone',
                'Last Task',
                'First Name',
                'Last name',
                'Email',
                'Is here?',
                'Has seen an instructor?',
                'Updated their project tasks',
                'Any comment?']
    daily_checklist = gsh(google_creds,
                        spreadsheet_name='Daily Checklist',
                        page='Main')
    if clean_sheet:
        if verbose:
            print('Cleaning sheet')
        clean_checklist(daily_checklist, retrieve, checklist)
        if verbose:
            print('Sheet cleaned')
    return daily_checklist


def populate_checklist(group, daily_checklist, cell, verbose=False):
    group_df = clean_dict(group)
    rows, cols = daily_checklist.update_page(group_df, verbose=verbose)
    daily_checklist.add_tick_box(rows=rows, columns=[cols[1] + 1, cols[1] + 3])
    daily_checklist.merge(f'B{cell}:B{cell + len(g) - 1}')
    if group_df['project_name'].nunique() > 1:
        first_project = group_df['project_name'].unique()[0]
        # Check how many users are in the different project
        diff_cells = len(group_df[group_df['project_name'] == first_project])
        return diff_cells
    elif group_df['project_name'].nunique() == 1:
        return 0
    else:
        print('Something went wrong with the group')
        print(group_df)


def generate_progress(people):
    peop = pd.DataFrame(people).sort_values(['project_name', 'project_idx', 'milestone_idx', 'task_idx'], ascending=False)
    peop = peop.rename(columns={'project_name': 'Project Name', 
                            'first_name': 'Name',
                            'last_name': 'Last Name',
                            'milestone': 'Milestone',
                            'task': 'Task'})

    cleaned_people = peop[['Name', 'Last Name', 'Project Name', 'Milestone', 'Task']]

    user_progress = gsh(google_creds,
                    spreadsheet_name='User Progress',
                    page='Main')

    user_progress.create_page(cleaned_people)


def get_people(by='project'):
    try:
        sql_creds = {'RDS_PASSWORD': os.environ['RDS_PASSWORD'],
                     'RDS_USER': os.environ['RDS_USER'],
                     'RDS_HOST': os.environ['RDS_HOST'],
                     'RDS_PORT': os.environ['RDS_PORT'],
                     'RDS_DATABASE': os.environ['RDS_DATABASE']}
        people, project_names = get_students_by(sql_creds, by=by)
    except:
        option = input('There was an error ' + 
                       'loading the latest version of the dataset.'
                       '\nDo you want to continue? [Y]/n')
        if (option is None) or (option == 'Y') or (option == 'y'):
            people, project_names = get_students_by(by=by)
        elif (option == 'n') or (option == 'N'):
            print('Aborting')
            exit()
        else:
            print('Option not recognized. Aborting')
            exit()
    
    return people, project_names


def send_agenda_instructors(today_events):
    slack_token = os.environ['SLACK_INTERNAL']
    slack_instructors = WebClient(token=slack_token)
    daily_agenda = print_daily_agenda(today_events,
                                      demo_room=3)
    daily_agenda = 'Hey team, this is what users will be doing today:\n' + daily_agenda
    daily_agenda += '\n\nPlease, make sure to open the checklist before the meetup starts!' 
    slack_instructors.chat_postMessage(channel='C033XF13HJS', text=daily_agenda)


# %%
if __name__ == '__main__':
    from config import (google_creds,
                        room_idx,
                        cell,
                        cohort_cell,
                        group_size,
                        group_by,
                        slack_client,
                        users_not_to_message)
    # Get the list of students and their corresponding groups
    people, project_names = get_people(by=group_by)

    today_events = get_today_records()
    # send_agenda_instructors(today_events)
    daily_checklist = get_checklist(clean_sheet=True, retrieve=False)
    code_room = int(len(people) / group_size + 1)
    generate_progress(people)
    
    for project_name in project_names:
        new_room_idx, groups = generate_rooms_by(people,
                                                project_name,
                                                room_idx,
                                                demo_room=3,
                                                code_room=code_room,
                                                group_size=group_size,
                                                today_events=today_events,
                                                slack_client=slack_client,
                                                by=group_by,
                                                link='https://us02web.zoom.us/meeting/register/tZwvduGhpzIqGNd50y8di9OsAahwG5S-PgMs',
                                                users_not_to_message=users_not_to_message
                                                )
        for g in groups:
            merge = populate_checklist(g, daily_checklist, cell)
            room_idx += 1
            cell += len(g)
            if merge != 0:
                cohort_cell += merge 
        room_idx = new_room_idx
        daily_checklist.merge(f'A{cohort_cell}:A{cell -1}')
        cohort_cell = cell

# %%
