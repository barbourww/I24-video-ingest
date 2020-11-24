import os
import re
import pickle
import cv2
import ast
from numpy import array

from parameters import DEFAULT_SESSION_INFO_FILENAME

with open('./resources/timestamp_geometry_4K.pkl', 'rb') as pf:
    g = pickle.load(pf)
    w = g['w']
    h = g['h']
    x0 = g['x0']
    y0 = g['y0']
    n = g['n']
    h13 = g['h13']
    h23 = g['h23']
    h12 = g['h12']
    w12 = g['w12']
with open('./resources/timestamp_pixel_checksum_6.pkl', 'rb') as pf:
    dig_cs6 = pickle.load(pf)


def parse_frame_timestamp(frame_pixels):
    """
    Use pixel checksum method to parse timestamp from video frame. First extracts timestamp area from frame
        array. Then converts to gray-scale, then converts to binary (black/white) mask. Each digit
        (monospaced) is then compared against the pre-computed pixel checksum values for an exact match.
    :param frame_pixels: numpy array of full (4K) color video frame
    :return: timestamp (None if checksum error), pixels from error digit (if no exact checksum match)
    """
    # extract the timestamp in the x/y directions; keep the margin in both directions for now
    tsimg = frame_pixels[0:(y0+h), 0:(x0+(n*w)), :]
    # convert color to gray-scale
    tsgray = cv2.cvtColor(tsimg, cv2.COLOR_BGR2GRAY)
    # convert to black/white binary mask using fixed threshold (1/2 intensity)
    # observed gray values on the edges of some digits in some frames were well below this threshold
    ret, tsmask = cv2.threshold(tsgray, 127, 255, cv2.THRESH_BINARY)

    # parse each of the `n` digits
    ts_dig = []
    for j in range(n):
        # disregard the decimal point in the UNIX time (always reported in .00 precision)
        if j == 10:
            ts_dig.append('.')
            continue

        # extract the digit for this index, correcting for the margin that was left over
        pixels = tsmask[y0:y0 + h, x0 + j * w:x0 + (j + 1) * w]
        # compute the 6-area checksum and convert it to an array
        cs = [[int(pixels[:h13, :w12].sum() / 255), int(pixels[:h13, w12:].sum() / 255)],
              [int(pixels[h13:h23, :w12].sum() / 255), int(pixels[h13:h23, w12:].sum() / 255)],
              [int(pixels[h23:, :w12].sum() / 255), int(pixels[h23:, w12:].sum() / 255)]
              ]
        cs = array(cs)
        # compute the absolute difference between this digit and each candidate
        cs_diff = [(dig, abs(cs - cs_ref).sum()) for dig, cs_ref in dig_cs6.items()]
        pred_dig, pred_err = min(cs_diff, key=lambda x: x[1])
        # looking for a perfect checksum match; testing showed this was reliable
        if pred_err > 0:
            # if no exact match, return no timestamp and the pixel values that resulted in the error
            return None, pixels
        else:
            ts_dig.append(pred_dig)
    # convert the list of strings into a number and return successful timestamp
    return ast.literal_eval(''.join(map(str, ts_dig))), None


def parse_config_file(config_file):
    """

    :param config_file:
    :return:
    """
    camera_config = []
    image_snap_config = []
    video_snap_config = []
    recording_config = []
    block_mapping = {'__CAMERA__': camera_config,
                     '__IMAGE-SNAPSHOT__': image_snap_config,
                     '__VIDEO-SNAPSHOT__': video_snap_config,
                     '__PERSISTENT-RECORDING__': recording_config}
    # open configuration file and parse it out
    with open(config_file, 'r') as f:
        current_block = None
        block_destination = None
        for line in f:
            # ignore empty lines and comment lines
            if line is None or len(line.strip()) == 0 or line[0] == '#':
                continue
            strip_line = line.strip()
            if len(strip_line) > 2 and strip_line[:2] == '__' and strip_line[-2:] == '__':
                # this is a configuration block line
                # first check if this is the first one or not
                if block_destination is not None and len(current_block) > 0:
                    # add the block to its destination if it's non-empty
                    block_destination.append(current_block)
                # reset current block to empty and set its destination
                current_block = {}
                block_destination = block_mapping[strip_line]
            elif '==' in strip_line:
                pkey, pval = strip_line.split('==')
                current_block[pkey.strip()] = pval.strip()
            else:
                raise AttributeError("""Got a line in the configuration file that isn't a block header nor a 
                key=value.\nLine: {}""".format(strip_line))
        # add the last block of the file (if it's non-empty)
        if block_destination is not None and len(current_block) > 0:
            block_destination.append(current_block)

    # check number of configuration blocks for these configs
    if len(image_snap_config) > 1:
        raise AttributeError("More than one configuration block found for __IMAGE-SNAPSHOT__.")
    elif len(image_snap_config) == 1:     # had one config block
        image_snap_config = image_snap_config[0]
    if len(video_snap_config) > 1:
        raise AttributeError("More than one configuration block found for __VIDEO-SNAPSHOT__.")
    elif len(video_snap_config) == 1:     # had one config block
        video_snap_config = video_snap_config[0]
    if len(recording_config) > 1:
        raise AttributeError("More than one configuration block found for __PERSISTENT-RECORDING__.")
    elif len(recording_config) == 1:     # had one config block
        recording_config = recording_config[0]

    # send back configs
    return camera_config, image_snap_config, video_snap_config, recording_config


def get_session_start_time_local(session_info_filename_override=None):
    """
    Finds the local time at which the session was started, according to the _SESSION_INFO.txt file.
    :param session_info_filename:
    :return:
    """
    import datetime
    if session_info_filename_override is None:
        session_info_filename = DEFAULT_SESSION_INFO_FILENAME
    else:
        session_info_filename = session_info_filename_override
    with open(session_info_filename, 'r') as f:
        for line in f:
            if line.startswith("Session initialization time (local): "):
                ts = line.strip("Session initialization time (local): ")
                ts = datetime.datetime.strptime(ts.strip(), "%Y-%m-%d %H:%M:%S.%f")
                break
        else:
            raise ValueError("Couldn't find line with timestamp.")
    return ts


def get_manager_log_files(session_directory, log_directory=None):
    """
    Determines list of log files written by video ingest manager.
    :param session_directory: top level directory of video ingest session
    :param log_directory: location of log files from video ingest manager (overrides session_directory)
    :return: list of files matching ingest manager logs ("manager-TIMESTAMP.log)
    """
    look_in_directory = (os.path.join(session_directory, 'logs') if log_directory is None else log_directory)
    manager_logs = []
    for fn in os.listdir(look_in_directory):
        if re.search('manager-(.*)\.log', fn) is not None:
            manager_logs.append(fn)
    return manager_logs
