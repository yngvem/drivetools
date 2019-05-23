from pathlib import Path
import os
from datetime import datetime, timezone
from operator import itemgetter
from typing import List, NewType, Dict, Union, NoReturn, Text, Optional

from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive


FileParams = NewType('FileParams', Dict)
FileList = NewType('FileList', List[FileParams])
RootFileList = NewType('RootFileList', FileList)
FileID = NewType('FileID', Text)
class LocalPath(type(Path())): pass
RemotePath = NewType('RemotePath', Text)

FOLDER_MIME_TYPE = "application/vnd.google-apps.folder"


def _create_path_stack(path: RemotePath) -> List[str]:
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


def get_local_modification_date(path: Path) -> datetime:
    modified_date = datetime.utcfromtimestamp(os.path.getmtime(path))
    modified_date = modified_date.replace(tzinfo=timezone.utc)
    return modified_date


def get_gdrive_modification_date(
    drive: GoogleDrive,
    file_id: Union[FileID, None],
    file_list: Optional[FileList] = None
) -> datetime:
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
        drive_file['modifiedDate'].replace('Z', '+0000'),
        "%Y-%m-%dT%H:%M:%S.%f%z"
    )


def _create_file_params(
    local_file: Path,
    remote_parent_id: Union[FileID, None] = None,
    remote_parent_path: RemotePath = None,
    file_id: Union[FileID, None] = None
) -> FileParams:
    local_file = Path(local_file)
    file_params: FileParams = {'title': local_file.name}

    if remote_parent_id is not None:
        file_params['parents'] = [{"id": remote_parent_id}]
    if file_id is not None:
        file_params['id'] = file_id

    return file_params


def get_gdrive_file_list(
    drive: GoogleDrive,
    remote_parent_id: Union[FileID, None] = None,
    file_list: Optional[FileList] = None
) -> FileList:
    if file_list is not None:
        return file_list

    if remote_parent_id is not None:
        return drive.ListFile(
            {'q': f"'{remote_parent_id}' in parents and trashed=false"}
        ).GetList()

    return drive.ListFile(
        {'q': f"trashed=false"}
    ).GetList()


def get_gdrive_root_file_list(
    drive : GoogleDrive,
    root_file_list: Optional[FileList] = None
) -> RootFileList:
    return RootFileList(
        get_gdrive_file_list(
            drive, remote_parent_id=None, file_list=root_file_list
        )
    )


def get_gdrive_file_id(
    drive: GoogleDrive,
    name: str,
    remote_parent_id: Union[FileID, None] = None,
    file_list: Optional[FileList] = None
) -> Optional[FileID]:
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
    drive: GoogleDrive,
    local_file: Path,
    remote_parent_id: Union[FileID, None] = None,
    remote_parent_path: RemotePath = None,
    file_id: Union[FileID, None] = None,
    file_list: Optional[FileList] = None
) -> NoReturn:
    if remote_parent_path is not None:
        assert remote_parent_id is not None
        remote_parent_id = get_gdrive_path_id(
            drive, remote_parent_path, file_list=file_list
        )

    file_params = _create_file_params(
        local_file,
        remote_parent_id=remote_parent_id,
        file_id=file_id,
    )

    drive_file = drive.CreateFile(file_params)
    drive_file.SetContentFile(str(local_file))
    drive_file.Upload()


def _gdrive_file_exists(
    drive: GoogleDrive,
    filename: str,
    remote_parent_id: Union[FileID, None] = None,
    file_list: Optional[FileList] = None 
) -> bool:
    file_list = get_gdrive_file_list(
        drive, remote_parent_id=remote_parent_id, file_list=file_list
    )
    if filename in map(itemgetter('title'), file_list):
        return True
    return False


def get_gdrive_folder_id(
    drive: GoogleDrive,
    folder_name: Text,
    remote_parent_id: Union[FileID, None],
    file_list: Optional[FileList] = None
) -> Optional[FileID]:
    """Return the ID of a folder that matches the given name and parent.
    """
    if remote_parent_id is None:
        remote_parent_id = FileID('root')

    file_list = get_gdrive_file_list(
        drive, remote_parent_id=remote_parent_id, file_list=file_list
    )
    for f in file_list:
        if f['title'] == folder_name and f['mimeType'] == FOLDER_MIME_TYPE:
            return f['id']
    return None


def get_gdrive_path_id(
    drive: GoogleDrive,
    path: RemotePath,
    file_list: Optional[FileList]
) -> Optional[FileID]:
    parent = None
    for folder in PathStack(path):
        parent = get_gdrive_folder_id(drive, folder, parent, file_list)

    return parent


def create_gdrive_folder(
    drive: GoogleDrive,
    folder_name: Text,
    remote_parent_id: Union[FileID, None],
    file_list: Optional[FileList]
) -> FileID:
    file_params = FileParams({
        "title": folder_name,
        "mimeType": "application/vnd.google-apps.folder"
    })
    if remote_parent_id is not None:
        file_params["parents"] = [{"id": remote_parent_id}]

    folder_id = get_gdrive_folder_id(
        drive, folder_name, remote_parent_id, file_list
    )
    if folder_id is not None:
        return FileID(folder_id)

    drive_folder = drive.CreateFile(
        file_params
    )
    drive_folder.Upload()
    return FileID(drive_folder["id"])


def create_gdrive_path(
    drive: GoogleDrive,
    path: RemotePath,
    file_list: Optional[FileList]
) -> Optional[FileID]:
    id_ = None
    for folder in PathStack(path):
        id_ = create_gdrive_folder(
            drive, folder, remote_parent_id=id_, file_list=file_list
        )

    return id_


def sync_file(
    drive: GoogleDrive,
    local_file: LocalPath,
    remote_parent_id: Union[FileID, None] = None,
    remote_parent_path: Optional[RemotePath] = None,
    file_list: Optional[FileList] = None
) -> None:
    local_file = LocalPath(local_file)
    file_id = get_gdrive_file_id(
        drive,
        local_file.name,
        remote_parent_id=remote_parent_id,
        file_list=file_list
    )

    if file_id is None:
        upload_file(
            drive,
            local_file,
            remote_parent_id=remote_parent_id,
            remote_parent_path=remote_parent_path,
            file_list=file_list
        )
        return

    remote_modification_date = get_gdrive_modification_date(
        drive,
        file_id=file_id,
        file_list=file_list
    )
    local_modification_date = get_local_modification_date(local_file)
    if remote_modification_date < local_modification_date:
        upload_file(
            drive,
            local_file,
            file_id=file_id
        )


def _extract_file_list(
    root_file_list: RootFileList,
    remote_parent_id: Union[FileID, None]
) -> FileList:
    def id_in_parents(f):
        return remote_parent_id in map(itemgetter('id'), f['parents'])

    return FileList([f for f in root_file_list if id_in_parents(f)])


def _sync_folder_non_recursive(
    drive: GoogleDrive,
    local_folder: LocalPath,
    remote_parent_id: Union[FileID, None],
    file_list: Optional[FileList],
    verbose: bool = False
) -> List[LocalPath]:
    sub_directories = []
    # Iterate over files and add folders to stack
    for path in local_folder.iterdir():
        path = LocalPath(path)
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
    drive: GoogleDrive,
    local_folder: LocalPath,
    remote_parent_path: RemotePath,
    verbose: bool = False,
    root_file_list: RootFileList = None
):
    """A breadth first traversal of the file tree.
    """
    remote_parent_id = create_gdrive_path(
        drive, remote_parent_path, root_file_list
    )
    local_folder = LocalPath(local_folder)
    if root_file_list is None:
        root_file_list = get_gdrive_root_file_list(
            drive, root_file_list=root_file_list
        )

    file_list = _extract_file_list(root_file_list, remote_parent_id)

    sub_directories = _sync_folder_non_recursive(
        drive,
        local_folder,
        remote_parent_id=remote_parent_id,
        file_list=file_list,
        verbose=verbose
    )

    # Recursive part
    for sub_directory in sub_directories:
        new_remote_parent = RemotePath(os.path.join(
            remote_parent_path, sub_directory.name
        ))
        sync_folder(
            drive=drive,
            local_folder=sub_directory,
            remote_parent_path=new_remote_parent,
            verbose=verbose,
            root_file_list=root_file_list
        )


def authenticate_gdrive(
    credentials_file: Path,
    load: bool = True,
    save: bool = True
) -> GoogleAuth:
    # Cheekily stolen from dano on SO
    # Link: https://stackoverflow.com/questions/24419188/automating-pydrive-verification-process
    credentials_file = Path(credentials_file)
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


def start_gdrive(credentials_file: Path) -> GoogleDrive:
    gauth = authenticate_gdrive(credentials_file)
    return GoogleDrive(gauth)
