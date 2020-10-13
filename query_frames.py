import os
import sys
import getopt
from traceback import print_exc

import utilities
from parameters import *


def get_video_stats(video_file_name):
    pass


def find_files(recording_directories, file_name_format, camera_names):
    """

    :param recording_directories:
    :param file_name_format:
    :param camera_names:
    :return: list of file names for recrodingds matching file name format
    """
    all_files = []
    for rdir in recording_directories:
        for rfile in os.listdir(rdir):
            all_files.append(os.path.join(rdir, rfile))
    # TODO: need to do regex match between file names and format
    pass


def parse_config_params(root_directory):
    """

    :param root_directory:
    :return: recording directory, file_name_format
    """
    # determine the config file path and parse that file
    config_file_path = os.path.join(root_directory, "_SESSION_CONFIG.config")
    camera_config, _, _, recording_config = utilities.parse_config_file(config_file=config_file_path)
    # get camera names for filename formatting
    cam_names = []
    for single_camera_config in camera_config:
        cam_names.append(single_camera_config['name'])
    # get the recording filename, or the default
    file_location = recording_config.get('recording_filename', DEFAULT_RECORDING_FILENAME)
    # split path location into directory and filename
    file_dir, file_name = os.path.split(file_location)
    if file_dir.startswith('./'):
        file_dir = os.path.join(root_directory, file_dir[2:])
    else:
        print("Absolute directory implied for persistent recording location.")
    # check that the file number formatter is present
    if '%d' not in file_name and not any(['%0{}d'.format(i) in file_name for i in range(10)]):
        print("Problem with recording configuration.")
        raise AttributeError("Need to include '%d' or '%0Nd' (N:0-9) in  recording filename template.")
    # check if we need to create camera-specific directories, or just one directory
    if '{cam_name}' in file_dir:
        rec_dirs = [file_dir.format(cam_name=cam_name) for cam_name in cam_names]
    elif '{cam_name}' in file_name:
        rec_dirs = [file_dir]
    else:
        # didn't find a camera name placeholder in either the file_dir or the file_name
        print("Problem with recording configuration.")
        raise AttributeError("Need to camera name placeholder ('{cam_name}') in recording filename template.")

    return rec_dirs, file_name, cam_names


def main(argv):
    usage = """
    pipeline_management.py [-h] -c <config-file> -r <session-root-directory>
    -h/--help: print usage information, then exit
    -c/--config_file: relative or absolute file path for session config file
    -s/--session_directory: path of the session directory where files are stored
    """
    try:
        opts, args = getopt.getopt(argv, 'hs:',
                                   ['help', 'session_directory='])
    except getopt.GetoptError:
        print("Usage:", usage)
        print_exc()
        sys.exit(2)
    session_directory = None
    for opt, arg in opts:
        if opt in ('-h', '--help'):
            print("Usage:", usage)
            sys.exit()
        elif opt in ('-s', '--session_directory'):
            session_directory = arg
    if session_directory is None:
        print("Must supply session directory so we can pull config file and recordings.")
        print("Usage:", usage)
        sys.exit(2)


if __name__ == '__main__':
    main(sys.argv[1:])
