# %%
from utils.SQLHelper import SQLHelper as sqlh
from utils.AgendaHelper import greeting_person_messages
from utils.AgendaHelper import print_daily_agenda
from datetime import datetime
import time
import pandas as pd

project_idx_map = {
        'Hangman': 0,
        'Computer Vision Rock Paper Scissors': 1,
        'Data Collection Pipeline': 2,
        'Football Match Outcome Prediction': 3,
        'Pinterest Data Processing Pipeline': 4
    }
# Columns that are probably not useful
# Comment out any columns that you might thing it's woth keeping
useless_columns = [
        'gender',
        'role',
        'bio',
        'active_pathway',
        'status',
        'experience',
        'headline',
        'display_pic',
        'receive_email',
        'github_app_installed',
        'github_username',
        'average_quiz_score',
        'proportion_quizzes_passed',
        'proportion_due_quizzes_passed',
        'proportion_due_quizzes_attempted',
        'proportion_attempted_quizzes_passed',
]


def get_students_tables(sql_creds: dict=None):
    if not sql_creds:
        students_df = pd.read_csv('students_df.csv', index_col=0)
        students_df['start_date_iso'] = pd.to_datetime(students_df['start_date_iso'])
        user_df = pd.read_csv('user_df.csv', index_col=0)
        quiz_scores = pd.read_csv('quiz_df.csv', index_col=0)
        quiz_scores['landed_at'] = pd.to_datetime(quiz_scores['landed_at'])
        quiz_scores['submitted_at'] = pd.to_datetime(quiz_scores['submitted_at'])
        lessons = pd.read_csv('lessons.csv', index_col=0)
        user_project_task = pd.read_csv('user_project_task.csv', index_col=0)
        user_projects = pd.read_csv('user_projects.csv', index_col=0)
        project_tasks_complete = pd.read_csv('project_tasks_complete.csv', index_col=0)
        project_tasks_complete['timestamp_completed'] = pd.to_datetime(project_tasks_complete['timestamp_completed'])
        project_milestone_tasks = pd.read_csv('project_milestone_tasks.csv', index_col=0)
    else:
            
        helper = sqlh(credentials=sql_creds)
        students_df = helper.df_from_table('students')
        user_df = helper.df_from_table('users')
        quiz_scores = helper.df_from_table('quiz_scores')
        lessons = helper.df_from_table('lessons')
        user_project_task = helper.df_from_query('''
                            SELECT upt.*, ptc.timestamp_completed, ptc.verified FROM user_project_task() as upt
                            JOIN project_tasks_complete AS ptc
                            ON upt.user_id=ptc.user_id AND upt.task_id=ptc.task_id
                            ''')
        user_projects = helper.df_from_query('''
                            SELECT user_id, name FROM user_projects
                            JOIN projects
                            ON projects.id=user_projects.project_id
                            WHERE active='true'
                            ''')
        project_milestone_tasks = helper.df_from_query('''
                            SELECT projects.id AS project_id, 
                                projects.name AS project, 
                                project_milestones.id AS milestone_id, 
                                project_milestones.name AS milestone,
                                project_milestones.idx AS milestone_idx,
                                project_milestones.duration AS milestone_duration,
                                project_tasks.id AS task_id,
                                project_tasks.name AS task,
                                project_tasks.idx AS task_idx
                            FROM projects
                            JOIN project_milestones
                            ON projects.id=project_milestones.project_id
                            JOIN project_tasks
                            ON project_milestones.id=project_tasks.milestone_id
                            ORDER BY projects.name, milestone_idx, task_idx
                            ''')
    return students_df, user_df, quiz_scores, lessons, user_project_task, user_projects, project_milestone_tasks


def get_students_info(sql_creds:dict=None):

    '''
    Return a dataframe with the information about enrolled
    students and their last attempted quiz
    and project task

    Parameters
    ----------
    sql_credentials: str
        Path to the credentials file for the RDS database

    Returns
    -------
    df: pd.DataFrame
        Dataframe with the information about enrolled students
    '''

    def get_next_task(x):
    # If they start today, they are not going to receive a message
        if x.start_date_iso == pd.to_datetime('today').normalize():
            return 'Starts Today'
        # If they haven't completed a task at all, this will be empty
        elif x.task == '---':
            return 'No task completed'
        # The last task they completed was the last one of a milestone
        elif x.task in (list(pmt_dict[x.project_name]['Last Tasks']['task'])):
            return 'Milestone Finished'
        # The last taks they completed was in the first milestone
        # So, they either just started the first project, or they just moved to a new project
        elif x.milestone_idx == 0:
            previous_project = user_project_task[user_project_task['user_id'] == x.user_id]
            if previous_project['project_idx'].nunique() > 1:
                return 'Second Project'
            else:
                return 'First Project, First Milestone'
        # The last task doesn't correspond to the project they are currently
        # So they haven't ticked any of the tasks in the new project
        elif x.task not in (list(pmt_dict[x.project_name]['Milestone Tasks']['task'])):
            return 'Not Ticked'
        # The last task is in the corresponding project, and it's not the first
        # milestone or the last task
        elif x.task in (list(pmt_dict[x.project_name]['Milestone Tasks']['task'])):
            previous_milestone = x.milestone_idx - 1
            user_tasks_completed = user_project_task[user_project_task['user_id'] == x.user_id]
            user_prev_milestone = user_tasks_completed[user_tasks_completed['milestone_idx'] == previous_milestone]
            return 'This Milestone'
    
    
    students_df, user_df, quiz_scores, lessons, user_project_task, user_projects, project_milestone_tasks = get_students_tables(sql_creds)
    
    pmt_dict = {x: {'Milestone Tasks' : project_milestone_tasks[
            project_milestone_tasks['project'] == x]}
            for x in project_milestone_tasks['project'].unique()}

    # Get first and last task per milestone for each project
    for _, v in pmt_dict.items():
        v['Last Tasks'] = (v['Milestone Tasks']
                                .groupby('milestone_id')
                                .tail(1))
        v['First Tasks'] = (v['Milestone Tasks']
                                .groupby('milestone_id')
                                .head(1))
                                
    pmt_dict['Last Milestone'] = (project_milestone_tasks
                                    .groupby('project')
                                    .tail(1)
                                    .reset_index(drop=True))
    # If the last completed task by a user is in last_task_milestone,
    # that means that they moved to the next milestone
    # If the last completes task by a user is the last row in the `Last Tasks`
    # table, that means that they completed the project
    # Otherwise, they are still in the same milestone

    user_projects = user_projects.rename(columns={'name': 'project_name'})
    user_df['user_id'] = user_df['user_id'].astype(str)
    students_df['user_id'] = students_df['user_id'].astype(str)
    user_project_task['user_id'] = user_project_task['user_id'].astype(str)
    students_df['active_pathway'] = students_df['active_pathway'].astype(str)
    user_projects['user_id'] = user_projects['user_id'].astype(str)


    # user_project_task = user_project_task.merge(project_tasks_complete,
    #                                             on=['user_id', 'task_id'],
    #                                             how='left')
    user_project_task['project_idx'] = (
                        user_project_task['project_name']
                        .replace(project_idx_map)
                        )
    user_project_task = (user_project_task.
                         sort_values(by=['user_id',
                                         'project_idx',
                                         'milestone_idx',
                                         'task_idx']))
    last_project_task = (user_project_task
                            .groupby('user_id')
                            .tail(1))

    enrolled_students = students_df[students_df['status'] == 'enrolled']
    active_students = user_df.merge(enrolled_students, on='user_id')
    active_students = active_students[~active_students['start_date_iso'].isna()]
    active_students = active_students[active_students['start_date_iso'] < pd.to_datetime('today').normalize()]
    # active_students = active_students[active_students['start_date_iso'] > '2021-07-01']
    # Get the last quiz per student
    last_quiz = (quiz_scores
                 .sort_values(['user_id',
                               'submitted_at'])
                 .groupby('user_id')
                 .tail(1))
    last_quiz_lesson = (last_quiz
                        .merge(lessons, on='quiz_id')
                        .drop(['quiz_id',
                               'score',
                               'max_score',
                               'landed_at',
                               'submitted_at',
                               'attempt_idx',
                               'id',
                               'module_id',
                               'description',
                               'notebook_url',
                               'study_guide',
                               'video_url'], axis=1))
    students_quizzes = (active_students
                        .merge(last_quiz_lesson,
                               on='user_id',
                               how='left'))

    students_quizzes = students_quizzes[
                        students_quizzes['last_name'] != 'Student']
    students_quizzes = students_quizzes[
                        ~students_quizzes['start_date']
                        .isin(['None'])
                        ]
    students_quizzes['name'] = students_quizzes['name'].fillna('---')

    students_quizzes = students_quizzes.drop(useless_columns, axis=1)
    cleaned_tasks = last_project_task[[
                    'user_id',
                    'milestone',
                    'task',
                    'project_idx',
                    'milestone_idx',
                    'task_idx']]

    cleaned_students = (students_quizzes
                        .merge(cleaned_tasks,
                               how='left',
                               on='user_id')
                        .merge(user_projects, 
                               on='user_id')
                        .sort_values(by=[
                            # 'start_date_iso',
                            'project_idx',
                            'milestone_idx',
                            'task_idx',
                            'idx'
                            ], 
                            ascending=False)
                        )
    cleaned_students.drop_duplicates(keep='last',
                                     subset='user_id',
                                     inplace=True)
    cleaned_students = cleaned_students.rename(columns={"name": "lesson_name"})
    cleaned_students = cleaned_students.fillna('---')
    cleaned_students['Next Task'] = cleaned_students.apply(get_next_task, axis=1)
    cleaned_students['group_name'] = cleaned_students['project_name']
    if len(cleaned_students[cleaned_students['group_name'] == 'Hangman']) < 4:
        cleaned_students.loc[cleaned_students['group_name'] == 'Hangman', ['group_name']] = 'Computer Vision Rock Paper Scissors'

    people = cleaned_students.to_dict('records')
    return people


def group_students_by(students: dict, group_size: int = 3, by_cohort: bool = False):

    groups = []
    idx = 0
    while idx < len(students):
        group = students[idx: idx + group_size]
        groups.append(group)
        idx += group_size
    if len(groups) > 1:
        if len(groups[-1]) < (group_size//2 + 1):
            groups[-2].extend(groups[-1])
            groups.pop()

    def map_student(student):
        dict_user = {
                'name': student['first_name'].capitalize(),
                'last name': student['last_name'].capitalize(),
                'preferred_name': student['preferred_name'].capitalize(),
                'slack_id': student['slack_id'],
                'lesson': student['lesson_name'],
                'task': student['task'],
                'milestone': student['milestone'],
                'email': student['email'],
            }
        
        if by_cohort:
            dict_user['group_name'] = student['group_name']
            return dict_user
        else:
            dict_user['project_name'] = student['project_name']
            return dict_user

    def map_group(group):
        return list(map(map_student, group))

    groups = list(map(map_group, groups))
    return groups


def send_message(groups, today_events, start_room_idx, demo_room, code_room, slack_client=None, link=None, users_not_to_message=None):

    room_idx = start_room_idx
    for group in groups:
        room_idx += 1
        for student in group:
            if student['preferred_name'] == '---':
                intro = greeting_person_messages(student['name'])
            else:
                intro = greeting_person_messages(student['preferred_name'])
            text = f'{intro}\n'
            text += print_daily_agenda(today_events=today_events,
                                        demo_room=demo_room,
                                        default_agenda=
                                        'agenda_files/default_agenda.yaml')
            text += f'You will go to breakout room {room_idx - 1} with '
            for other_student in group:
                if other_student['name'] != student['name']:
                    text += f'{other_student["preferred_name"]} '
            text = ' '.join(text.split(' ')[:-2]) + ' and ' + text.split(' ')[-2]
            if link:
                text += f'\nImportant! Today\'s zoom link has changed! You can access the new link here: {link}'
            if datetime.today().weekday() == 1:
                text += ('\nRemember that today Phil runs office hours!' +
                        ' If you need advice on your career he will be' +
                        ' available from 18:30 to 19:30.\n')
            if datetime.today().weekday() == 3:
                text += ('\nToday is the last day of the week!\n' +
                        'Let\'s finish it by doing something interesting.' +
                        f' From 21:00 to 21:30, some peers will go to room {code_room}' + 
                        ' to work on some LeetCode challenges. Feel free to join them to improve' +
                        ' your Python and problem solving skills!')

            text += ('\n\nOnce again, here is all the students\' tasks and milestones that they have alredy reached.' + 
                     'You can check that out in this link: https://docs.google.com/spreadsheets/d/1h9fHnDhJu23RU8V0aT08JnrKdKviJJAlEzUofiZDTq8/edit?usp=sharing \n' + 
                     'That way, you can check who has reached that milestone you are struggling with and ask them! ' +
                     '\n\nA new feature we will release today is that the support requests will be now public, so you can ask for help from anyone!' +
                     '\nYou will see the breakout room of the user that is asking for help, so we can create our own StackOverflow community!')

            if slack_client:
                channel = student['slack_id']
                if channel in users_not_to_message:
                    print('Not sending message to', student['name'])
                    continue
                if channel.startswith('U'):
                    print('Sending message to', student['name'])
                    time.sleep(1)
                    slack_client.chat_postMessage(channel=channel, text=text)
                else:
                    print(f"{student['preferred_name']} does not have a slack id")
                    print('Send the message personally, or add the slack id to the user')
            

    return room_idx


def filter_by(people, name):
    return [p for p in people if p['group_name'] == name]


def get_students_by(sql_creds: dict = None, by: str = 'project'):
    '''
    Return a list of dictionaries with
    info about each student
    groups of students'''

    people = get_students_info(sql_creds)
    if by == 'project':

        group_names = {p['group_name'] for p in people}
    elif by == 'cohort':

        def get_group_names(unique_start_dates):
            group_names = [
                get_group_name_from_start_date(date)
                for date in unique_start_dates
                if date != 'None'
            ]
            return group_names
        

        def get_group_name_from_start_date(start_date):
            start_date_to_group_name = {
                'August 21': 'Theta',
                'July 21': 'Eta',
                'June 21': 'Zeta'
            }
            if start_date in start_date_to_group_name:
                return start_date_to_group_name[start_date]
            else:
                return start_date


        unique_start_dates = {p['start_date'] for p in people}
        group_names = get_group_names(unique_start_dates)

        people = [{**p, 'group_name':
                get_group_name_from_start_date(p['start_date'])}
                for p in people]

    return people, group_names


def generate_rooms_by(people,
                      name, 
                      room_idx, 
                      group_size, 
                      today_events, 
                      demo_room,
                      code_room,
                      slack_client=None, 
                      by='project', 
                      link=None, users_not_to_message=None):
    group = filter_by(people, name)
    if by == 'project':
        groups = group_students_by(group, group_size=group_size, by_cohort=False)
    elif by == 'cohort':
        groups = group_students_by(group, group_size=group_size, by_cohort=True)
    else:
        raise ValueError('by must be either project or cohort')
    new_room = send_message(
                    groups,
                    today_events=today_events,
                    code_room=code_room,
                    slack_client=slack_client,
                    start_room_idx=room_idx,
                    demo_room=demo_room,
                    link=link,
                    users_not_to_message=users_not_to_message
                    )
    return new_room, groups

# %%


if __name__ == '__main__':
    import os
    sql_creds = {'RDS_PASSWORD': os.environ['RDS_PASSWORD'],
             'RDS_USER': os.environ['RDS_USER'],
             'RDS_HOST': os.environ['RDS_HOST'],
             'RDS_PORT': os.environ['RDS_PORT'],
             'RDS_DATABASE': os.environ['RDS_DATABASE']}
    people = get_students_info(sql_creds)
# %%
