import os
import sys
import getopt
import subprocess
from traceback import print_exc
import re
import csv

import utilities
from parameters import *


def get_video_stats(video_file_names):
    """

    :param video_file_names:
    :return:
    """
    if not isinstance(video_file_names, (list, tuple)):
        raise TypeError("Must provide list of tuple of video filenames.")
    frame_counts = {}
    for vfn in video_file_names:
        cmd = ["ffprobe", "-v", "error", "-select_streams", "v:0", "-show_entries", "stream=nb_frames",
               "-of", "default=nokey=1:noprint_wrappers=1", vfn]
        fcp = subprocess.run(args=cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
        try:
            frame_counts[vfn] = int(fcp.stdout)
        except ValueError:
            print("INVALID OUTPUT FROM FFPROBE COMMAND")
            print("STDOUT:", fcp.stdout)
            print("STDERR:", fcp.stderr)
            print_exc()
    return frame_counts


def find_files(recording_directories, file_name_format, camera_names):
    """

    :param recording_directories:
    :param file_name_format:
    :param camera_names:
    :return: list of file names for recrodingds matching file name format
    """
    file_name_regex = re.sub('%(0[0-9]{1})*d', '([0-9]+)', file_name_format)
    cam_file_name_regexs = [file_name_regex.replace('{cam_name}', cn) for cn in camera_names]
    print("Searching for file names matching any of:", cam_file_name_regexs)
    all_files = []
    for rdir in recording_directories:
        for rfile in os.listdir(rdir):
            all_files.append(os.path.join(rdir, rfile))
    print("Found {} files in recording directories.".format(len(all_files)))
    match_files = []
    for fl in all_files:
        if any([re.search(crx, fl) is not None for crx in cam_file_name_regexs]):
            match_files.append(fl)
    print("Found {} files matching recording file name format.".format(len(match_files)))
    return match_files


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


def write_frame_count_results(results_dict, filename, print_results=False):
    """

    :param results_dict:
    :param filename:
    :return: None
    """
    with open(filename, 'w') as f:
        writer = csv.writer(f, delimiter=';', quoting=csv.QUOTE_NONNUMERIC)
        writer.writerow(["filename", "frame-count"])
        if print_results is True:
            print("\n______FRAME COUNT RESULTS______")
            print("filename    ----    frame count")
            print("-------------------------------")
        for cn, fc in sorted(list(results_dict.items()), key=lambda x: x[0]):
            writer.writerow([cn, fc])
            if print_results is True:
                print(cn, '-->', fc)
    return


def main(argv):
    usage = """
    query_frames.py [-h] [-p] -s <session-directory>
    -h/--help: print usage information, then exit
    -s/--session_directory: path of the session directory where files are stored
    -p/--print_output: flag to print output of frame counting as it is being written to file
    """
    try:
        opts, args = getopt.getopt(argv, 'hps:',
                                   ['help', 'print_output', 'session_directory='])
    except getopt.GetoptError:
        print("Usage:", usage)
        print_exc()
        sys.exit(2)
    session_directory = None
    print_output = False
    for opt, arg in opts:
        if opt in ('-h', '--help'):
            print("Usage:", usage)
            sys.exit()
        elif opt in ('-s', '--session_directory'):
            session_directory = arg
        elif opt in ('-p', '--print_output'):
            print_output = True
    if session_directory is None:
        print("Must supply session directory so we can pull config file and recordings.")
        print("Usage:", usage)
        sys.exit(2)
    #
    recording_directories, recording_filename_format, camera_names = parse_config_params(
        root_directory=session_directory)
    matching_files = find_files(recording_directories=recording_directories, file_name_format=recording_filename_format,
                                camera_names=camera_names)
    file_frame_counts = get_video_stats(video_file_names=matching_files)
    write_frame_count_results(results_dict=file_frame_counts,
                              filename=os.path.join(session_directory, 'recording_frame_counts.csv'),
                              print_results=print_output)


if __name__ == '__main__':
    main(sys.argv[1:])
