import os
import re


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


def get_session_start_time(session_info_filename):
    import datetime
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
