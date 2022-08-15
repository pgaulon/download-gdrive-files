#!/usr/bin/env python

from __future__ import print_function
from pickle import load, dump
from os import path, mkdir, getcwd
from io import BytesIO
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.http import MediaIoBaseDownload
from argparse import ArgumentParser

# credentials.json comes from https://developers.google.com/workspace/guides/auth-overview
CREDENTIALS_JSON_FILE = 'credentials.json'

# If modifying these scopes, delete the file token.pickle.
SCOPES = ['https://www.googleapis.com/auth/drive.metadata.readonly', 'https://www.googleapis.com/auth/drive.readonly']
TOKEN_PICKLE = 'token-down.pickle'
DOWNLOAD_MIME_TYPES = ['application/msword',
                       'application/pdf',
                       'application/vnd.ms-excel',
                       'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                       'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
                       'application/vnd.openxmlformats-officedocument.presentationml.presentation',
                       'application/x-tex',
                       'image/png',
                       'application/vnd.jgraph.mxfile.realtime']
EXPORT_MIME_TYPES_MAPPING = {
    'application/vnd.google-apps.document': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
    'application/vnd.google-apps.spreadsheet': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    }

def download_or_export(service, file, folder='', dry=False):
    mimeType = file.get('mimeType')
    file_id = file.get('id')
    if mimeType in DOWNLOAD_MIME_TYPES:
        request = service.files().get_media(fileId=file_id)
    elif mimeType in EXPORT_MIME_TYPES_MAPPING.keys():
        request = service.files().export_media(fileId=file_id,
                                                     mimeType=EXPORT_MIME_TYPES_MAPPING[mimeType])
    else:
        print("Type {} for file {} ({}) unknown, skipping".format(mimeType, file['name'], file_id))
        return file

    if not dry:
        fh = BytesIO()
        filename = file.get('name')
        if folder:
            if not path.exists(folder):
                mkdir(folder)
            filename = folder + '/' + file.get('name')
        if not path.exists(filename):
            with open(filename, 'wb') as fd:
                downloader = MediaIoBaseDownload(fd, request)
                done = False
                while done is False:
                    status, done = downloader.next_chunk()
                    print("Download {}%.".format(int(status.progress() * 100)))
        else:
            print('Download of {} skipped, exists already.'.format(filename))
    return file

def get_children(service, folder_id, local_folder = '', dry=False):
    page_token = None
    while True:
        response = service.files().list(q="'{}' in parents".format(folder_id),
                                        spaces='drive',
                                        includeTeamDriveItems=True,
                                        supportsTeamDrives=True,
                                        fields='nextPageToken, files(id, name, mimeType)',
                                        pageToken=page_token).execute()
        for file in response.get('files', []):
            mimeType = file.get('mimeType')
            print('Found file: {} ({}, {})'.format(file.get('name'), file.get('mimeType'), file.get('id')))
            if mimeType == 'application/vnd.google-apps.folder':
                # recursion
                get_children(service, file.get('id'), local_folder + '/' + file.get('name'), dry=dry)
            else:
                download_or_export(service, file, local_folder, dry=dry)

        page_token = response.get('nextPageToken', None)
        if page_token is None:
            return folder_id

def main(folder_id, dry=False):
    """Shows basic usage of the Drive v3 API.
    Prints the names and ids of the first 10 files the user has access to.
    """
    creds = None
    # The file token.pickle stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if path.exists(TOKEN_PICKLE):
        with open(TOKEN_PICKLE, 'rb') as token:
            creds = load(token)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_JSON_FILE, SCOPES)
            creds = flow.run_local_server()
        # Save the credentials for the next run
        with open(TOKEN_PICKLE, 'wb') as token:
            dump(creds, token)

    service = build('drive', 'v3', credentials=creds)
    response = service.files().get(fileId=folder_id, supportsTeamDrives=True).execute()
    print("Starting from {}".format(response['name']))
    get_children(service, folder_id, response['name'], dry=dry)

if __name__ == '__main__':
    parser = ArgumentParser(description='Download Team Drive folder recursively.')
    parser.add_argument('--folder', required=True,
                        help='Folder ID to download from, e.g. --folder 111111ABCD11111222_abc')
    parser.add_argument('--dry', action='store_true',
                        help='Dry run mode, does not download anything.')
    args = parser.parse_args()
    main(args.folder, dry=args.dry)
