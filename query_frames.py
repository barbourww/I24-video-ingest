import os
import sys
import getopt
import subprocess
import warnings
from traceback import print_exc
import re
import csv
import time
import pickle

import utilities
from parameters import *


def get_video_frame_counts(video_file_names):
    """
    Run FFprobe frame count queries for recorded video segments.
    :param video_file_names: list of tuples (video-file-name, segment-number) to query for frame counts
    :return: dictionary of frame counts {video-file-name: frame-count, ...}
    """
    if not isinstance(video_file_names, (list, tuple)):
        raise TypeError("Must provide list of tuples (video-file-name, segment-number).")
    frame_counts = {}
    print("\nRunning video frame count queries.")
    for i, (vfn, vfi) in enumerate(video_file_names):
        if i % 500 == 0:
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


def get_video_frame_timestamps(video_file_names):
    """
    Run frame timestamp parsing for recorded video segments.
    :param video_file_names: list of video file names to query for frame counts
    :return: dictionary of list of frame timestamps {video-file-name: list-frame-timestamps, ...}
    """
    if not isinstance(video_file_names, (list, tuple)):
        raise TypeError("Must provide list of tuples (video-file-name, segment-number).")
    import cv2
    timestamps = {}
    pixel_errors = []
    for i, (vfn, vfi) in enumerate(video_file_names):
        t0 = time.time()
        cap = cv2.VideoCapture(vfn)
        assert cap.isOpened(), "Cannot open file \"{}\"".format(vfn)

        # TODO: pull out camera name and segment number extraction into a utility function
        cam = vfn.split('record_')[1].split('_')[0]
        print("Processing camera {}".format(cam))
        cam_ts = []
        i = 0
        while True:
            ret, frame = cap.read()
            if frame is None:
                print("End of video after {} frames.".format(i))
                break
            frame_ts, px_err = utilities.parse_frame_timestamp(frame_pixels=frame)
            if frame_ts is not None:
                cam_ts.append(frame_ts)
            else:
                cam_ts.append(0)
                pixel_errors.append(px_err)
            i += 1
            continue
        timestamps[vfn] = cam_ts
        cap.release()
        print("{:.1f} fps processing rate".format(i / (time.time() - t0)))
    # if we had any errors in checksum recognition, append them to the running file
    if len(pixel_errors) > 0:
        if 'errors_pixel_checksum.pkl' in os.listdir('./resources'):
            with open('./resources/pixel_errors.pkl', 'rb') as f:
                pixel_errors = pickle.load(f) + pixel_errors
        with open('./resources/pixel_errors.pkl', 'wb') as f:
            pickle.dump(pixel_errors, f)
    return timestamps


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


def write_frame_timestamp_results(results_dict, filename):
    """
    Write the results of video frame timestamp extraction to CSV file. File structure is:
        filename0; ts0; ts1; ts2; ts3; ...
        filename1; ts0; ts1; ts2; ts3; ...
    :param results_dict: dictionary of {filename: [ordered list of timestamps]
    :param filename: file path where to save results
    :return: None
    """
    with open(filename, 'w') as f:
        writer = csv.writer(f, delimiter=';', quoting=csv.QUOTE_NONNUMERIC)
        writer.writerow(["filename", "timestamps (delimited)"])
        for fn, fts in sorted(list(results_dict.items()), key=lambda x: x[0]):
            writer.writerow([fn] + fts)
    return


def read_frame_count_results(file_path):
    """
    Reads a file containing frame counts written from `write_frame_count_results()`.
    :param file_path: Path to file containing results.
    :return: dictionary of results in same format it was written
    """
    with open(file_path, 'r') as f:
        reader = csv.reader(f, delimiter=';', quoting=csv.QUOTE_NONNUMERIC)
        header = next(reader)
        results_dict = {}
        for row in reader:
            if row is not None and len(row) == 2:
                results_dict[row[0]] = row[1]
        return results_dict


def plot_frame_count_results(results_dict, filename, session_info_filename):
    """
    Generates a plot from frame count results.
    :param results_dict: dictionary of results {video_file_path: frame_count, ...}
    :param filename: file path for plot (PDF)
    :param session_info_filename: path to _SESSION_INFO.txt file in order to extract session start time for reference
    :return:
    """
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
    tref = utilities.get_session_start_time_local(session_info_filename)
    fig, axs = plt.subplots(3, 1, figsize=(12, 9))
    for cam, counts in cams.items():
        # TODO: this pole determination needs to be more robust in the future
        pole = int(cam.split('c')[0].split('p')[1])
        # sort based on segment number
        sct = sorted(counts)
        rns, cts = zip(*sct)
        segment_time = utilities.get_sesssion_recording_segment_time(session_info_filename)
        rns = [tref + dt.timedelta(minutes=rn * segment_time) for rn in rns]
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
    print(filename)
    plt.savefig(filename)


def main(argv):
    usage = """
    query_frames.py [-h] [-l] -s <session-directory>
    
    # primary behavior mode selection (must indicate one primary or alternate mode)
    -c/--count: count frames using FFmpeg container query
    -t/--timestamp: parse timestamps in frames using pixel checksum method (currently very slow, consider)
    
    # alternate behavior modes
    -h/--help: print usage information, then exit
    -l/--load_plot_output: load results file in session directory (need -s), plot the results, then exit; if non-default
        frame count results output filename, then specify it with -o/--output_filename= option
    
    # required arguments
    -s/--session_directory= /path/to/session_directory : (required) path of the session directory where files are stored
    
    # options with value required (options themselves are not required)
    -o/--output_filename= /path/to/output_file.csv : override output filename for results
    -f/--first_file= ### : file segment index at which to start querying
    -i/--input_filename= : comma-delineated list of file name fragments to narrow down video segment files; uses simple 
        `if 'fragment' in full_file_path:` check, so be specific; e.g., p2c3_00150,p3c1_00004
    -a/--append_outputs= /path/to/alt_output1.csv,path/to/alt_output2.csv : comma-delineated list of *absolute* results 
        file paths to append to -o/--output_filename= specified results (used during post-facto plotting option -l/--.)
    
    # options with no value to specify
    -d/--drop_last_file: flag to not query the last file in recording sequence, in case recording is actively occurring
    -p/--plot_output: flag to plot output of frame counting, grouped by pole (same filename as output, but .pdf)
    --print_output: flag to print output of frame counting as it is being written to file
    
    """
    try:
        opts, args = getopt.getopt(argv, 'cthldps:o:f:i:a:',
                                   ['count', 'timestamp', 'help', 'load_plot_output',
                                    'drop_last_file', 'plot_output', 'print_output',
                                    'session_directory=', 'output_filename=',
                                    'first_file=', 'input_filename=', 'append_outputs='])
    except getopt.GetoptError:
        print("Usage:", usage)
        print_exc()
        sys.exit(2)

    # defaults for mode choice
    count_frames = False
    parse_timestamps = False
    # defaults for inputs
    session_directory = None
    drop_last_file = False
    results_filename = None
    input_filename_filters = None
    append_results = None
    first_file = 0
    print_output = False
    plot_output = False
    # flag to plot output and exit (needs to capture session_directory value)
    plot_and_exit = False
    # parse inputs
    for opt, arg in opts:
        if opt in ('-h', '--help'):
            print("Usage:", usage)
            sys.exit()
        elif opt in ('-c', '--count'):
            count_frames = True
        elif opt in ('-t', '--timestamp'):
            parse_timestamps = True
        elif opt in ('-l', '--load_plot_output'):
            plot_and_exit = True
        elif opt in ('-s', '--session_directory'):
            session_directory = arg
        elif opt in ('-d', '--drop_last_file'):
            drop_last_file = True
        elif opt in ('-o', '--output_filename'):
            results_filename = arg
        elif opt in ('-a', '--append_outputs'):
            if ',' not in arg:
                warnings.warn("Only got on results file to append. If that's not right, check comma delineation.")
            append_results_unfiltered = arg.split(',')
            append_results = []
            for fn in append_results_unfiltered:
                if os.path.exists(fn):
                    append_results.append(fn)
                else:
                    warnings.warn("Path to results file in append argument does not exist: {}".format(fn))
            if len(append_results) == 0:
                append_results = None
        elif opt in ('-i', '--input_filename'):
            input_filename_filters = arg.split(',')
        elif opt in ('-f', '--first_file',):
            first_file = int(arg)
        elif opt in ('--print_output',):
            print_output = True
        elif opt in ('-p', '--plot_output',):
            plot_output = True
        else:
            warnings.warn("Got unhandled option/argument. OPTION=[{}] ARGUMENT=[{}]".format(opt, arg))

    # this is the only required input
    if session_directory is None:
        print("Must supply session directory so we can pull config file and recordings.")
        print("Usage:", usage)
        sys.exit(2)
    session_info_file_path = os.path.join(session_directory, DEFAULT_SESSION_INFO_FILENAME)
    session_number = utilities.get_session_number(session_info_filename=session_info_file_path)

    # one of these modes must be selected
    if plot_and_exit is False:
        if count_frames is False and parse_timestamps is False:
            print("Must select a mode: count frames (-c), parse timestamps (-t), or load and plot results (-l).")
            print("Usage:", usage)
            sys.exit(2)

    default_count_filename = 'frame_counts_recording.csv'
    default_plot_filename = 'frame_counts_recording.pdf'
    default_timestamp_filename = 'frame_timestamp_recording.csv'

    # default to files in session directory if not specified
    if results_filename is None:
        count_filename = os.path.join(session_directory, default_count_filename)
        plot_filename = os.path.join(session_directory, default_plot_filename)
        timestamp_filename = os.path.join(session_directory, default_timestamp_filename)
    # results filename was specified (custom), so determine how this goes to count, plot, and timestamp filenames
    else:
        if count_frames is True and parse_timestamps is True:
            # both count and timestamp indicated, so differentiate the value for results_filename, then .pdf for plot
            rfp, rfe = os.path.splitext(results_filename)
            count_filename = rfp + '-count' + rfe
            plot_filename = os.path.splitext(count_filename)[0] + '.pdf'
            timestamp_filename = rfp + '-timestamp' + rfe
        elif count_frames is True:
            # count indicated, so define that filename and the plot one
            count_filename = results_filename
            plot_filename = os.path.splitext(count_filename)[0] + '.pdf'
            timestamp_filename = None
        elif parse_timestamps is True:
            # timestamp indicated, so define that filename only
            count_filename = None
            plot_filename = None
            timestamp_filename = results_filename
        else:
            # for else case, plot_and_exit must = True
            # so change the extension from the results filename to PDF for the plot
            count_filename = None
            timestamp_filename = None
            plot_filename = os.path.splitext(results_filename)[0] + '.pdf'

    # if plot and exit requested, then do so
    if plot_and_exit is True:
        file_frame_counts = read_frame_count_results(file_path=results_filename)
        if append_results is not None:
            for arfn in append_results:
                file_frame_counts.update(read_frame_count_results(file_path=arfn))
        plot_frame_count_results(results_dict=file_frame_counts, filename=plot_filename,
                                 session_info_filename=session_info_file_path)
        sys.exit()

    # determine the config file path and parse that file
    config_file_path = os.path.join(session_directory, "_SESSION_CONFIG.config")
    camera_config, _, _, recording_config = utilities.parse_config_file(config_file=config_file_path)
    # go get the relevant configuration parameters
    recording_directories, recording_filename_format, camera_names = utilities.get_recording_params(
        session_root_directory=session_directory, session_number=session_number,
        camera_configs=camera_config, recording_config=recording_config)
    # determine the files in the recording directory matching the filename format
    matching_files = utilities.find_files(recording_directories=recording_directories,
                                          file_name_format=recording_filename_format, camera_names=camera_names,
                                          drop_last_file=drop_last_file, first_file_index=first_file,
                                          filter_filenames=input_filename_filters)

    if count_frames is True:
        # run the frame count queries
        file_frame_counts = get_video_frame_counts(video_file_names=matching_files)
        # write the frame count results to a CSV file
        write_frame_count_results(results_dict=file_frame_counts,
                                  filename=count_filename, print_results=print_output)
        # plot frame count results, if indicated
        if plot_output is True:
            plot_frame_count_results(results_dict=file_frame_counts, filename=plot_filename,
                                     session_info_filename=session_info_file_path)

    if parse_timestamps is True:
        # run the parse timestamp queries
        file_frame_timestamps = get_video_frame_timestamps(video_file_names=matching_files)
        # write the frame timestamp results to a CSV file
        write_frame_timestamp_results(results_dict=file_frame_timestamps, filename=timestamp_filename)


if __name__ == '__main__':
    main(sys.argv[1:])
