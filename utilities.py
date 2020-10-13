
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
