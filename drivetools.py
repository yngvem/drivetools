# TODO: Use dicts instead of lists for drive filesystem
from copy import copy
from datetime import datetime, timezone
from operator import itemgetter
import os
from pathlib import Path
from typing import List, NewType, Dict, Union, Text, Optional, Tuple, Any

from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive


FileID = Union[Text, None]
FileParams = NewType("FileParams", Dict[Text, Any])
FileList = NewType("FileList", List[FileParams])
RootFileList = NewType("RootFileList", FileList)
DriveFiles = NewType("DriveFiles", Dict[FileID, FileParams])

RemotePath = NewType("RemotePath", Text)
LocalPath = NewType("LocalPath", Path)


FOLDER_MIME_TYPE = "application/vnd.google-apps.folder"


def _create_path_stack(path: RemotePath) -> List[str]:
    head, tail = os.path.split(path)
    path_stack = [tail]
    while head != "":
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
    drive: GoogleDrive, file_id: FileID, file_index: DriveFiles
) -> Optional[datetime]:
    """Get the modification date of a Drive file, None if it doesn't exist.
    """
    drive_file = file_index[file_id]

    return datetime.strptime(
        drive_file["modifiedDate"].replace("Z", "+0000"),
        "%Y-%m-%dT%H:%M:%S.%f%z",
    )


def _create_file_params(
    local_file: Path,
    remote_parent_id: FileID = None,
    remote_parent_path: RemotePath = None,
    file_id: FileID = None,
) -> FileParams:
    """Create parameters for file to be uploaded.
    """
    local_file = Path(local_file)
    file_params = FileParams({"title": local_file.name})

    if remote_parent_id is not None:
        file_params["parents"] = [{"id": remote_parent_id}]
    if file_id is not None:
        file_params["id"] = file_id

    return file_params


def is_child(file_params: FileParams, file_id: FileID) -> bool:
    return file_id in map(itemgetter("id"), file_params["parents"])


def is_in_root(file_params: FileParams) -> bool:
    no_parents = len(file_params["parents"]) == 0
    root_parent = any(map(itemgetter("isRoot"), file_params["parents"]))
    return no_parents or root_parent


def get_children(file_index: DriveFiles, parent_id: FileID) -> DriveFiles:
    if parent_id is None:
        return DriveFiles(
            {id_: fp for id_, fp in file_index.items() if is_in_root(fp)}
        )

    return DriveFiles(
        {id_: fp for id_, fp in file_index.items() if is_child(fp, parent_id)}
    )


def get_gdrive_root_file_list(drive: GoogleDrive) -> RootFileList:
    return drive.ListFile({"q": "trashed=false"}).GetList()


def file_list_to_index(file_list: FileList) -> DriveFiles:
    return DriveFiles({file_["id"]: file_ for file_ in file_list})


def get_gdrive_file_index(drive):
    return file_list_to_index(get_gdrive_root_file_list(drive))


def upload_file(
    drive: GoogleDrive,
    local_file: Path,
    remote_parent_id: FileID,
    file_id: FileID,
):
    file_params = _create_file_params(
        local_file, remote_parent_id=remote_parent_id, file_id=file_id
    )

    drive_file = drive.CreateFile(file_params)
    drive_file.SetContentFile(str(local_file))
    drive_file.Upload()


def _gdrive_file_exists(
    filename: Text, file_index: DriveFiles, remote_parent_id: FileID = None
) -> bool:
    children = get_children(file_index, remote_parent_id)
    if filename in map(itemgetter("title"), children.values()):
        return True
    return False


def get_gdrive_file_id(
    filename: Text, remote_parent_id: FileID, file_index: DriveFiles
) -> FileID:
    children = get_children(file_index, remote_parent_id)

    for id_, file_ in children.items():
        if file_["title"] == filename:
            return id_
    return None


def get_gdrive_folder_id(
    filename: Text, remote_parent_id: FileID, file_index: DriveFiles
):
    children = get_children(file_index, remote_parent_id)

    for id_, file_ in children.items():
        if (
            file_["title"] == filename
            and file_["mimeType"] == FOLDER_MIME_TYPE
        ):
            return id_
    return None


def sync_file(
    drive: GoogleDrive,
    local_file: LocalPath,
    file_index: DriveFiles,
    remote_parent_id: FileID = None,
    remote_parent_path: Optional[RemotePath] = None,
):
    local_file = LocalPath(local_file)

    file_id = get_gdrive_file_id(
        local_file.name,
        remote_parent_id=remote_parent_id,
        file_index=file_index,
    )

    if file_id is None:
        upload_file(
            drive,
            local_file,
            remote_parent_id=remote_parent_id,
            file_id=file_id,
        )
        return

    local_modification_date = get_local_modification_date(local_file)
    remote_modification_date = get_gdrive_modification_date(
        drive, file_id=file_id, file_index=file_index
    )

    if remote_modification_date < local_modification_date:
        upload_file(
            drive,
            local_file,
            remote_parent_id=remote_parent_id,
            file_id=file_id,
        )


def _sync_folder_non_recursive(
    drive: GoogleDrive,
    local_folder: LocalPath,
    remote_parent_id: FileID,
    file_index: DriveFiles,
    verbose: bool = False,
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
                file_index=file_index,
            )
    return sub_directories


def create_gdrive_folder(
    drive: GoogleDrive, folder: Text, parent: FileID, file_index: DriveFiles
) -> Tuple[FileID, DriveFiles]:
    file_index = copy(file_index)
    file_params = FileParams({"title": folder, "mimeType": FOLDER_MIME_TYPE})
    if parent is not None:
        file_params["parents"] = [{"id": parent}]

    folder = drive.CreateFile(file_params)
    folder.Upload()
    metadata = dict(folder)
    file_index[metadata["id"]] = metadata
    return metadata["id"], file_index


def create_gdrive_path(
    drive: GoogleDrive, remote_path: RemotePath, file_index: DriveFiles
) -> Tuple[FileID, DriveFiles]:
    parent = None
    for folder in PathStack(remote_path):
        id_ = get_gdrive_folder_id(folder, parent, file_index)
        if id_ is None:
            id_, file_index = create_gdrive_folder(
                drive, folder, parent, file_index
            )
        parent = id_

    return parent, file_index


# TODO: Traverse file-tree first, get list of files to upload
# This way we can have a progress bar.
def sync_folder(
    drive: GoogleDrive,
    local_folder: LocalPath,
    remote_parent_path: RemotePath,
    file_index: Optional[DriveFiles] = None,
    verbose: bool = False,
):
    """A breadth first traversal of the file tree.
    """
    if file_index is None:
        file_index = get_gdrive_file_index(drive)

    remote_parent_id, file_index = create_gdrive_path(
        drive, remote_parent_path, file_index
    )
    local_folder = LocalPath(Path(local_folder))

    sub_directories = _sync_folder_non_recursive(
        drive,
        local_folder,
        remote_parent_id=remote_parent_id,
        file_index=file_index,
        verbose=verbose,
    )

    # Recursive part
    for sub_directory in sub_directories:
        new_remote_parent = RemotePath(
            os.path.join(remote_parent_path, sub_directory.name)
        )
        file_index = sync_folder(
            drive=drive,
            local_folder=sub_directory,
            remote_parent_path=new_remote_parent,
            verbose=verbose,
            file_index=file_index,
        )

    return file_index


def authenticate_gdrive(
    credentials_file: Path, load: bool = True, save: bool = True
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
