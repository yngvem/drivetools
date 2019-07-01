import argparse
import drivetools


def main(local_path, remote_path, credentials_file, verbose):
    drive = drivetools.start_gdrive(credentials_file)
    drivetools.sync_folder(drive, local_path, remote_path, verbose=verbose)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("local_path")
    parser.add_argument("remote_path")
    parser.add_argument("credentials_file")
    parser.add_argument("--verbose", type=bool, default=False)

    args = parser.parse_args()

    main(
        args.local_path, args.remote_path, args.credentials_file, args.verbose
    )
