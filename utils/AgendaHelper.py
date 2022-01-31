import pandas as pd
from datetime import date
import random
import yaml


def greeting_group_message():
    return random.choice([
                'Hi team',
                'Hi all',
                'Hello everyone',
                "What's up team?",
                "Hi squad",
                "Hello squad"
            ])


def greeting_person_messages(name):
    return random.choice([
            f'Hi {name}!',
            f'Hey {name}',
            f'What\'s up {name}?',
            ])


def read_agenda_from_file(file_path: str) -> str:
    '''
    Reads the agenda from a file

    Parameters
    ----------
    file_path: str
        The path to the file

    Returns
    -------
    str
        The agenda
    '''
    with open(file_path, 'r') as f:
        return yaml.safe_load(f)


agenda_intro_messages = [
            "Here's the agenda for today:",
            "Here's the schedule for today:",
            "Here's what's happening today:",
            "Here's what you should be doing today:",
            "Here's what we're doing today:",
            "Here's the plan for today:"
            ]


def filter_today_events(records_df: pd.DataFrame) -> pd.DataFrame:
    '''
    Gets the events for today from the records dataframe

    Parameters
    ----------
    records_df: pd.DataFrame
        The dataframe of the google sheet

    Returns
    -------
    pd.DataFrame
        The dataframe of the events for today
    '''

    def is_today(x):
        if pd.isna(x):
            return False
        else:
            return date.today() == x.date()

    events = records_df.drop('Date', axis=1)
    events['Date'] = pd.to_datetime(records_df['Date'])
    events.sort_values(by='Date', inplace=True)

    return events[events['Date'].apply(is_today)]


def generate_dict_from_events(events: pd.DataFrame, demo_room: int) -> dict:
    '''
    Generates a dictionary that will be appended to the main agenda
    from the events dataframe.
    The dictionary has the following structure:
        Name: Takes the host, name, description, and difficulty of the demo
            or the host, name, and description in case it is a presentation
        Starts: The start time of the event
        Ends: The end time of the event

    Parameters
    ----------
    events: pd.DataFrame
        The events dataframe

    Returns
    -------
    str
        The message to send to the group
    '''
    out_dict = {}
    out_dict['Starts'] = str(events['Starts'])
    out_dict['Ends'] = str(events['Ends'])
    if events['Type'] == 'Demo':
        out_dict['Name'] = (f"Demo hosted by {events['Host']}: " +
                            f"{events['Name']}. " +
                            f"{events['Host']} will show you" +
                            f"{events['Description']}. " +
                            f"The demo will be hosted in room {demo_room}. " +
                            f"Difficulty: {events['Difficulty']}")
        return out_dict
    elif events['Type'] == 'Presentation':
        out_dict['Name'] = (f"{events['Name']} " +
                            f"hosted by {events['Host']}. " +
                            f"{events['Host']} will " +
                            f"{events['Description']}")
        return out_dict


def print_daily_agenda(today_events: pd.DataFrame,
                       name: str = None,
                       demo_room: int = 2,
                       default_agenda: str =
                       'agenda_files/default_agenda.yaml'):
    '''
    Returns the daily agenda as a string
    '''

    DEFAULT_AGENDA = read_agenda_from_file(default_agenda)

    if name:
        greeting = greeting_person_messages(name)
    else:
        greeting = greeting_group_message()

    agenda = greeting + '\n\n' + random.choice(agenda_intro_messages) + '\n'

    events_agenda = []
    for _, event in today_events.iterrows():
        events_agenda.append(generate_dict_from_events(event, demo_room))

    all_agenda_items = [
                    *DEFAULT_AGENDA,
                    *events_agenda,
                    ]
    all_agenda_items.sort(key=lambda i: (i['Starts'], i['Ends']))

    for agenda_item in all_agenda_items:
        agenda += (f"- {agenda_item['Starts']}-{agenda_item['Ends']}: " +
                   f"{agenda_item['Name']}\n")

    agenda += '\n'
    return agenda
