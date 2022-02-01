# %%
from utils.SQLHelper import SQLHelper as sqlh
import random


def group_students(students: dict, group_size: int = 3):

    groups = []
    idx = 0
    while idx < len(students):
        group = students[idx: idx + group_size]
        groups.append(group)
        idx += group_size
    else:
        final_group = students[idx:]
        if len(final_group) > 0:
            students.append(final_group)

    def map_student(student):
        return {
            'name': student['first_name'].capitalize(),
            'last name': student['last_name'].capitalize(),
            'lesson': student['lesson_name'],
            'group_name': student['group_name'],
            'task': student['task'],
            'milestone': student['milestone']
        }

    def map_group(group):
        return list(map(map_student, group))

    groups = list(map(map_group, groups))
    return groups


def print_groups(groups, instructor=False, start_room_idx=2):
    text = ''

    # text += print_agenda()

    groups_intro = random.choice([
        'Here are the groups for today:',
        "Here's the groups:",
        "Here's the rooms:",
        "Here's the groups for today:",
        "Here's where we recommend you to be:",
    ])
    groups_intro += '\n'
    text += groups_intro

    room_idx = start_room_idx
    for group in groups:
        text += f'- Room {room_idx}\n'
        room_idx += 1
        for student in group:
            if instructor:
                text += f'\t- {student["name"]}. Lesson: {student["lesson"]}\n'
            else:
                text += f'\t- {student["name"]}\n'

    return text, room_idx


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


def filter_by_cohort(people, group_name):
    return [p for p in people if p['group_name'] == group_name]


def get_last_element_by_group(df, column: str = 'user_id'):
    '''
    Return a dataframe with only the last element
    by student
    '''
    g = df.groupby(column)

    return (g.tail(1)
            .drop_duplicates()
            .sort_values(column)
            .reset_index(drop=True)
            )


def get_first_element_by_group(df, column: str = 'user_id'):
    '''
    Return a dataframe with only the last element
    by student
    '''
    g = df.groupby(column)

    return (g.head(1)
            .drop_duplicates()
            .sort_values(column)
            .reset_index(drop=True)
            )


def get_students_info(sql_creds:dict):
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
    helper = sqlh(credentials=sql_creds)
    students_df = helper.df_from_table('students')
    user_df = helper.df_from_table('users')
    quiz_scores = helper.df_from_table('quiz_scores')
    lessons = helper.df_from_table('lessons')
    # pml = helper.df_from_query('SELECT * FROM pathway_module_lesson()')
    user_project_task = helper.df_from_query('''
                        SELECT * FROM user_project_task()
                        ''')
    user_df['user_id'] = user_df['user_id'].astype(str)
    students_df['user_id'] = students_df['user_id'].astype(str)
    user_project_task['user_id'] = user_project_task['user_id'].astype(str)
    students_df['active_pathway'] = students_df['active_pathway'].astype(str)

    # Sort the projects by user_id, project, and milestone

    project_idx_map = {
            'Computer Vision Rock Paper Scissors': 0,
            'Data Collection Pipeline': 1,
            'Football Match Outcome Prediction': 2,
            'Pinterest Data Processing Pipeline': 3
        }
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
                         .tail(1)
                         .sort_values(by=[
                                        'project_idx',
                                        'milestone_idx',
                                        'task_idx']))
    enrolled_students = students_df[students_df['status'] == 'enrolled']
    active_students = user_df.merge(enrolled_students, on='user_id')

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
                        ~students_quizzes['start_date']
                        .isin(['None', 'February 2022'])
                        ]
    students_quizzes = students_quizzes[
                        students_quizzes['last_name'] != 'Student']
    students_quizzes['name'] = students_quizzes['name'].fillna('---')
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
            'preferred_name',
            'slack_id',
            'receive_email',
            'github_app_installed',
            'github_username',
            'average_quiz_score',
            'proportion_quizzes_passed',
            'proportion_due_quizzes_passed',
            'proportion_due_quizzes_attempted',
            'proportion_attempted_quizzes_passed',
    ]

    students_quizzes = students_quizzes.drop(useless_columns, axis=1)
    cleaned_tasks = last_project_task[[
                    'user_id',
                    'project_name',
                    'milestone',
                    'task',
                    'project_idx',
                    'milestone_idx',
                    'task_idx']]

    cleaned_students = (students_quizzes
                        .merge(cleaned_tasks,
                               how='left',
                               on='user_id')
                        .sort_values(by=[
                            'project_idx',
                            'milestone_idx',
                            'task_idx',
                            'idx'
                            ], 
                            ascending=False)
                        )

    cleaned_students = cleaned_students.rename(columns={"name": "lesson_name"})
    people = cleaned_students.to_dict('records')
    return people


def get_students_groups(sql_creds:dict):
    '''
    Return a list of dictionaries with
    info about each student
    groups of students'''

    people = get_students_info(sql_creds)

    unique_start_dates = {p['start_date'] for p in people}
    group_names = get_group_names(unique_start_dates)

    people = [{**p, 'group_name':
              get_group_name_from_start_date(p['start_date'])}
              for p in people]

    return people, group_names


def generate_rooms(people, group_name, room_idx, instructor=False):
    cohort = filter_by_cohort(people, group_name)
    groups = group_students(cohort, )
    text, new_room = print_groups(
                    groups,
                    instructor=False,
                    start_room_idx=room_idx,
                    )
    return text, new_room, groups

# %%


if __name__ == '__main__':
    people = get_students_info()
# %%
