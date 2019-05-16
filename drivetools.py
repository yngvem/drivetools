from pathlib import Path
import os
from datetime import datetime, timezone
from operator import itemgetter

from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive


FOLDER_MIME_TYPE = "application/vnd.google-apps.folder"


def _create_path_stack(path):
    head, tail = os.path.split(path)
    path_stack = [tail]
    while head != '':
        head, tail = os.path.split(head)
        path_stack.append(tail)

    return path_stack


class Stack:
    def __init__(self):
        self.stack = []

    def __iadd__(self, item):
        self.stack.append(item)

    def __iter__(self):
        return self

    def __next__(self):
        if len(self.stack) == 0:
            raise StopIteration
        return self.stack.pop()


class PathStack(Stack):
    def __init__(self, path):
        self.stack = _create_path_stack(path)


def get_local_modification_date(path):
    modified_date = datetime.utcfromtimestamp(os.path.getmtime(path))
    modified_date = modified_date.replace(tzinfo=timezone.utc)
    return modified_date


def get_gdrive_modification_date(drive, file_id, file_list=None):
    if (
        file_list is not None and
        file_id in map(itemgetter('id'), file_list)
    ):
        drive_file = filter(
            lambda x: x['id'] == file_id, file_list
        ).__next__()
    else:
        drive_file = drive.CreateFile({'id': file_id})

    return datetime.strptime(
        drive_file['modifiedDate'],
        "%Y-%m-%dT%H:%M:%S.%f%z"
    )


def _create_file_params(
    local_file,
    remote_parent_id=None,
    remote_parent_path=None,
    file_id=None
):
    local_file = Path(local_file)
    file_params = {'title': local_file.name}

    if remote_parent_id is not None:
        file_params['parents'] = [{"id": remote_parent_id}]
    if file_id is not None:
        file_params['id'] = file_id

    return file_params


def get_gdrive_file_list(drive, remote_parent_id=None, file_list=None):
    if file_list is not None:
        return file_list

    if remote_parent_id is not None:
        parent = remote_parent_id
    else:
        parent = 'root'

    return drive.ListFile(
        {'q': f"'{parent}' in parents and trashed=false"}
    ).GetList()


def get_gdrive_file_id(drive, name, remote_parent_id=None, file_list=None):
    file_list = get_gdrive_file_list(
        drive,
        remote_parent_id=remote_parent_id,
        file_list=file_list
    )
    for file_ in file_list:
        if name in file_['title']:
            return file_['id']
    return None


def upload_file(
    drive,
    local_file,
    remote_parent_id=None,
    remote_parent_path=None,
    file_id=None
):
    if remote_parent_path is not None:
        assert remote_parent_id is not None
        remote_parent_id = get_gdrive_path_id(drive, remote_parent_path)

    file_params = _create_file_params(
        local_file,
        remote_parent_id=remote_parent_id,
        file_id=file_id,
    )

    drive_file = drive.CreateFile(file_params)
    drive_file.SetContentFile(str(local_file))
    drive_file.Upload()


def _gdrive_file_exists(
    drive, filename, remote_parent_id=None, file_list=None
):
    file_list = get_gdrive_file_list(
        drive, remote_parent_id=remote_parent_id, file_list=file_list
    )
    if filename in map(itemgetter('title'), file_list):
        return True
    return False


def get_gdrive_folder_id(drive, folder, parent):
    """Return the ID of a folder that matches the given name and parent.
    """
    if parent is None:
        parent = 'root'

    file_list = drive.ListFile(
        {'q': f"'{parent}' in parents and trashed=false"}
    ).GetList()
    for f in file_list:
        if f['title'] == folder and f['mimeType'] == FOLDER_MIME_TYPE:
            return f['id']
    return None


def get_gdrive_path_id(drive, path):
    parent = None
    for folder in PathStack(path):
        parent = get_gdrive_folder_id(drive, folder, parent)

    return parent


def create_gdrive_folder(drive, folder, parent):
    file_params = {
        "title": folder,
        "mimeType": "application/vnd.google-apps.folder"
    }
    if parent is not None:
        file_params["parents"] = [{"id": parent}]

    folder_id = get_gdrive_folder_id(drive, folder, parent)
    if folder_id is not None:
        return folder_id

    folder = drive.CreateFile(
        file_params
    )
    folder.Upload()
    return folder["id"]


def create_gdrive_path(drive, path):
    id_ = None
    for folder in PathStack(path):
        id_ = create_gdrive_folder(drive, folder, parent=id_)

    return id_


def sync_file(
    drive,
    local_file,
    remote_parent_id=None,
    remote_parent_path=None,
    file_list=None
):
    local_file = Path(local_file)
    file_id = get_gdrive_file_id(
        drive,
        local_file.name,
        remote_parent_id=remote_parent_id,
        file_list=file_list
    )

    if file_id is None:
        return upload_file(
            drive,
            local_file,
            remote_parent_id=remote_parent_id,
            remote_parent_path=remote_parent_path,
        )

    remote_modification_date = get_gdrive_modification_date(
        drive,
        file_id=file_id,
        file_list=file_list
    )
    local_modification_date = get_local_modification_date(local_file)
    if remote_modification_date < local_modification_date:
        return upload_file(
            drive,
            local_file,
            file_id=file_id
        )


def _sync_folder_non_recursive(
    drive,
    local_folder,
    remote_parent_id,
    file_list,
    verbose=False
):
    sub_directories = []
    # Iterate over files and add folders to stack
    for path in local_folder.iterdir():
        if verbose:
            print(path)
        if path.is_dir():
            sub_directories.append(path)
        else:
            sync_file(
                drive,
                path,
                remote_parent_id=remote_parent_id,
                file_list=file_list
            )
    return sub_directories


# TODO: Traverse file-tree first, get list of files to upload
# This way we can have a progress bar.
def sync_folder(
    drive,
    local_folder,
    remote_parent,
    verbose=False
):
    """A breadth first traversal of the file tree.
    """
    remote_parent_id = create_gdrive_path(drive, remote_parent)
    local_folder = Path(local_folder)
    file_list = get_gdrive_file_list(drive, remote_parent_id=parent_id)

    sub_directories = _sync_folder_non_recursive(
        drive,
        local_folder,
        remote_parent_id=remote_parent_id,
        file_list=file_list,
        verbose=verbose
    )

    for sub_directory in sub_directories:
        new_remote_parent = Path(remote_parent)/sub_directory.name
        sync_folder(
            drive=drive,
            local_folder=sub_directory,
            remote_parent=new_remote_parent,
            verbose=verbose
        )


def authenticate_gdrive(credentials_file, load=True, save=True):
    # Cheekily stolen from dano on SO
    # Link: https://stackoverflow.com/questions/24419188/automating-pydrive-verification-process
    gauth = GoogleAuth()
    if credentials_file.is_file() and load:
        gauth.LoadCredentialsFile(credentials_file)
    if gauth.credentials is None:
        # Authenticate if they're not there
        gauth.LocalWebserverAuth()
    elif gauth.access_token_expired:
        # Refresh them if expired
        gauth.Refresh()
    else:
        # Initialize the saved creds
        gauth.Authorize()

    if credentials_file is not None and save:
        gauth.SaveCredentialsFile(credentials_file)

    return gauth


def start_gdrive(credentials_file):
    gauth = authenticate_gdrive(credentials_file)
    return GoogleDrive(gauth)
