import argparse
import drivetools


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('filename')

    args = parser.parse_args()

    drivetools.authenticate_gdrive(args.filename, load=False, save=True)
