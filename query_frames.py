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
    Run FFprobe frame count queries for recorded video segments.
    :param video_file_names: list of video file names to query for frame counts
    :return: dictionary of frame counts {video-file-name: frame-count, ...}
    """
    if not isinstance(video_file_names, (list, tuple)):
        raise TypeError("Must provide list of tuple of video filenames.")
    frame_counts = {}
    print("\nRunning video frame count queries.")
    for i, (vfn, vfi) in enumerate(video_file_names):
        if i % 100 == 0:
            print("Query number {}".format(i))
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


def find_files(recording_directories, file_name_format, camera_names, drop_last_file=False, first_file_index=0):
    """
    Determine files in recording directories that match file recording naming format.
    :param recording_directories: list of directories in which to search for files (one or many, based on file naming)
    :param file_name_format: file name format with which the persistent recording was working
    :param camera_names: list of camera names to substitute into file name format
    :param drop_last_file: flag to ignore/drop the last file in the recording sequence, per camera
    :param first_file_index: minimum recording segment number to keep files (used for checking recent files only)
    :return: list of file names for recordings matching file name format
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
    for crx in cam_file_name_regexs:
        cam_files = []
        for fl in all_files:
            rem = re.search(crx, fl)
            if rem is not None:
                # extract the first group match, which contains the segment index
                remi = int(rem.group(1))
                if remi >= first_file_index:
                    cam_files.append((fl, remi))
        # sort files by segment index and drop the last one, if requested, while adding to all matches
        if drop_last_file is True:
            match_files += sorted(cam_files, key=lambda x: x[1])[:-1]
        else:
            match_files += sorted(cam_files, key=lambda x: x[1])
    print("Found {} files matching recording file name format.".format(len(match_files)))
    return match_files


def parse_config_params(root_directory):
    """
    Determine relevant parameters from video ingest session configuration.
    :param root_directory: directory of video ingest session, which contains automatic copy of config file.
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
    Write the dictionary of frame count results to a CSV file.
    :param results_dict: dictionary of frame count results {file-name: frame-count, ...}
    :param filename: filename to which results should be written
    :param print_results: T/F print results as they're written to file
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


def plot_frame_count_results(results_dict, filename, session_info_filename):
    import matplotlib.pyplot as plt
    import datetime as dt
    cams = {}
    for rfile, count in results_dict.items():
        cm = rfile.split('_')[-2]
        rn = int(rfile.split('_')[-1].split('.')[0])
        if cm in cams:
            cams[cm].append((rn, count))
        else:
            cams[cm] = [(rn, count)]
    tref = utilities.get_session_start_time(session_info_filename)
    fig, axs = plt.subplots(3, 1, figsize=(12, 9))
    for cam, counts in cams.items():
        # TODO: this pole determination needs to be more robust in the future
        pole = int(cam.split('c')[0].split('p')[1])
        sct = sorted(counts)[:-1]
        rns, cts = zip(*sct)
        rns = [tref + dt.timedelta(minutes=rn * 10) for rn in rns]
        axs[pole - 1].plot(rns, cts, label=cam)
    w1, w2 = 0, 0
    for ax in axs:
        l1, l2 = ax.get_ylim()
        if l2 - l1 > w2 - w1:
            w1, w2 = l1, l2
    for ax in axs:
        ax.set_ylim((w1, w2))
        ax.legend()
    axs[0].set_title("Recording file frame counts")
    plt.tight_layout()
    plt.savefig(filename)


def main(argv):
    usage = """
    query_frames.py [-h] [-p] -s <session-directory>
    -h/--help: print usage information, then exit
    -d/--drop_last_file: flag to not query the last file in recording sequence, in case recording is actively occurring
    -s/--session_directory /path/to/session_directory : (required) path of the session directory where files are stored
    -o/--output_filename /path/to/output_file.csv : override output filename for results
    --print_output: flag to print output of frame counting as it is being written to file
    --plot_output: flag to plot output of frame counting, grouped by pole (same filename as output, but .pdf)
    --first_file ### : file segment index at which to start querying
    """
    try:
        opts, args = getopt.getopt(argv, 'hds:o:',
                                   ['help', 'print_output', 'plot_output', 'drop_last_file',
                                    'session_directory=', 'output_filename=', 'first_file='])
    except getopt.GetoptError:
        print("Usage:", usage)
        print_exc()
        sys.exit(2)

    # defaults for inputs
    session_directory = None
    drop_last_file = False
    results_filename = None
    first_file = 0
    print_output = False
    plot_output = False
    # parse inputs
    for opt, arg in opts:
        if opt in ('-h', '--help'):
            print("Usage:", usage)
            sys.exit()
        elif opt in ('-s', '--session_directory'):
            session_directory = arg
        elif opt in ('-d', '--drop_last_file'):
            drop_last_file = True
        elif opt in ('-o', '--output_filename'):
            results_filename = arg
        elif opt in ('--first_file',):
            first_file = int(arg)
        elif opt in ('--print_output',):
            print_output = True
        elif opt in ('--plot_output',):
            plot_output = True

    # this is the only required input
    if session_directory is None:
        print("Must supply session directory so we can pull config file and recordings.")
        print("Usage:", usage)
        sys.exit(2)
    # default to files in session directory if not specified
    if results_filename is None:
        results_filename = os.path.join(session_directory, 'frame_counts_recording.csv')
        plot_filename = os.path.join(session_directory, 'frame_counts_recording.pdf')
    else:
        plot_filename = os.path.splitext(results_filename)[0] + '.pdf'

    # go get the relevant configuration parameters
    recording_directories, recording_filename_format, camera_names = parse_config_params(
        root_directory=session_directory)
    # determine the files in the recording directory matching the filename format
    matching_files = find_files(recording_directories=recording_directories, file_name_format=recording_filename_format,
                                camera_names=camera_names, drop_last_file=drop_last_file, first_file_index=first_file)
    # run the frame count queries
    file_frame_counts = get_video_stats(video_file_names=matching_files)
    # write the frame count results to a CSV file and, optionally, a plot by pole
    write_frame_count_results(results_dict=file_frame_counts,
                              filename=results_filename, print_results=print_output)
    if plot_output is True:
        plot_frame_count_results(results_dict=file_frame_counts, filename=plot_filename,
                                 session_info_filename=os.path.join(session_directory, '_SESSION_INFO.txt'))


if __name__ == '__main__':
    main(sys.argv[1:])
