#%%

import pandas as pd
from utils.GoogleSheetHelper import GoogleSheetHelper as gsh
from utils.AgendaHelper import filter_today_events
from utils.AgendaHelper import print_daily_agenda
from utils.group_helper import generate_rooms_by, get_students_by
import os
from slack_sdk import WebClient
from config import google_creds


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


people = get_people()
peop = pd.DataFrame(people[0]).sort_values(['project_name', 'project_idx', 'milestone_idx', 'task_idx'], ascending=False)
peop = peop.rename(columns={'project_name': 'Project Name', 
                            'first_name': 'Name',
                            'last_name': 'Last Name',
                            'milestone': 'Milestone',
                            'task': 'Task'})

cleaned_people = peop[['Name', 'Last Name', 'Project Name', 'Milestone', 'Task']]

# %%

user_progress = gsh(google_creds,
                    spreadsheet_name='User Progress',
                    page='Main')

# %%
user_progress.create_page(cleaned_people)

# %%
