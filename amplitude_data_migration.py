from urllib import response
import requests
import os
import json
import gzip
import io
import zipfile

PROJECT_NUMBER = ""
PROJECT_API_KEY = ""
PATH_TO_SAVE_FILES = "/home/muhammad/Downloads"
PATH_OF_SAVED_FILES = PATH_TO_SAVE_FILES + PROJECT_NUMBER


def auth(apikey, secretkey):
    credits = (apikey, secretkey)
    return credits


def exporting_data(end_date, month, year, credits):
    start_date = "01"
    params = (
        ('start', year+month+start_date+'T00'),
        ('end', year+month+end_date+'T23'),
    )
    response = requests.get(
        'https://amplitude.com/api/2/export', params=params, auth=credits)
    zipFiles = zipfile.ZipFile(io.BytesIO(response.content))
    zipFiles.extractall(PATH_TO_SAVE_FILES)


def uploadingEventsToAmplitude(events, apiKey):
    headers = {
        'Content-Type': 'application/json',
        'Accept': '*/*'
    }
    payload = {

        "api_key": apiKey,
        "events": events
    }
    r = requests.post('https://api2.amplitude.com/batch',
                      json=payload, headers=headers)
    return(r.json())


def gettingEventsData(fileName):
    with gzip.open(fileName, 'rb') as f:
        file_content = f.readlines()

    events = []
    Failed_events = []
    for event in file_content:
        # decoding byte to string and dumps into json
        my_json = event.decode('utf8')

        # Load the JSON to a Python list & dump it back out as formatted JSON
        try:
            data = json.loads(my_json)
            eventData = json.dumps(data, indent=4, sort_keys=True)
            event = json.loads(eventData)
            try:
                if (type(event['user_id']) == str):
                    if (len(event['user_id']) > 5):
                        events.append(event)

                elif (str(event['user_id']) == 'None'):
                    events.append(event)
                else:
                    print(event['user_id'])
            except:
                pass
        except:
            Failed_events.append(my_json)
    return events, Failed_events


def ExcludedEvents(Events):
    ExcludedUserEvents = []
    ExcludedEventsId = []
    for i in Events:
        try:
            if type(i['user_id']) == str:
                if len(i['user_id']) <= 5:
                    ExcludedUserEvents.append(i['user_id'])
                    ExcludedEventsId.append(Events.index(i))
        except:
            pass
    return ExcludedUserEvents, ExcludedEventsId


def TrueEvents(events):
    Events, UnSuccessEvents = ExcludedEvents(events)
    for item in UnSuccessEvents:
        events.pop(item)
    return Events, UnSuccessEvents


def ExtractingEvents(path, eventsFiles):
    All_events = []
    Failed = []
    Error_Files = []

    main_path = path+'/'
    for i in range(len(eventsFiles)):
        try:
            events, failed_e = gettingEventsData(main_path+eventsFiles[i])
            for item in events:
                All_events.append(item)
            for item in failed_e:
                Failed.append(item)
        except:
            Error_Files.append(eventsFiles[i])
    return All_events, Failed, Error_Files


def UploadingEvents(events, NewProjectAPiKey):
    start_ = 0
    end_ = 1000
    Failed_List = []
    limit = int(len(events)/2000)
    for i in range(limit+1):
        response = uploadingEventsToAmplitude(
            events[start_:end_], NewProjectAPiKey)
        if response['code'] == 200:
            print(start_, end_, response['events_ingested'])
            start_ = end_
            end_ = end_ + 2000
        else:
            Failed_List.append(start_)
            print(start_, end_, response)
            start_ = end_
            end_ = end_ + 2000


def UploadEventsToNewProject(NewProjectApiKey):
    events_files = os.listdir(PATH_OF_SAVED_FILES)
    eventsData, failedFiles, errorFiles = ExtractingEvents(
        PATH_OF_SAVED_FILES, events_files)
    eventsToUpload, erroredEvents = TrueEvents(eventsData)
    UploadingEvents(eventsToUpload, PROJECT_API_KEY)


def run():
    UploadEventsToNewProject()
