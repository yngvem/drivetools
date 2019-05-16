from pathlib import Path
from io import BytesIO

import pptx
import drivetools


class GooglePresentation:
    def __init__(
        self, template=None, name=None, parent=None, credentials_file=None
    ):
        self.template = template
        self.name = name
        if self.name is None:
            self.name = 'presentation.pptx'
        self.credentials_file = Path(credentials_file)
        self.parent = parent

    def __enter__(self):
        self.pres = pptx.Presentation(self.template)
        return self.pres

    def __exit__(self, type, value, traceback):
        presentation_file = BytesIO()
        self.pres.save(presentation_file)

        drive = drivetools.start_gdrive(self.credentials_file)
        file_params = self.get_file_params(drive)
        drive_file = drive.CreateFile(file_params)
        drive_file.content = presentation_file
        drive_file.Upload({'convert': True})

    def get_file_params(self, drive):
        params = {'title': self.name}
        if self.parent is not None:
            params['parents'] = [
                {"id": drivetools.create_gdrive_path(drive, self.parent)}
            ]
        return params
