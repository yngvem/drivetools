import argparse
import drivetools


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('local_path')
    parser.add_argument('remote_path')
    parser.add_argument('credentials_file')

    args = parser.parse_args()

    drive = drivetools.start_gdrive(args.credentials_file)
    drivetools.sync_folder(drive, args.local_path, args.remote_path)
