# directory format for an ingest session directory
# format with session number
# ------------------------------------------------
DEFAULT_SESSION_DIRECTORY_FORMAT = "ingest_session_{:05d}"

# formatter names for pipeline interpipesrc and interpipesink elements
# names are formatted with pipeline name
# --------------------------------------------------------------------
PIPE_SOURCE_NAME_FORMATTER = '{}_src'
PIPE_SINK_NAME_FORMATTER = '{}_sink'

# formatter name for pipeline filesink elements (of whatever form)
# names are formatted with pipeline name
# ----------------------------------------------------------------
PIPE_SINGLE_FILESINK_NAME_FORMATTER = '{}_filesink'         # use when filesink is not camera-specific
PIPE_CAMERA_FILESINK_NAME_FORMATTER = '{}_filesink_{}'      # use for camera-specific filesinks


# default amount of video time that is buffered on incoming streams
# so that the video snapshot can record into history on trigger
# -----------------------------------------------------------------
DEFAULT_BUFFER_TIME = 60

# default duration of video snapshot recording when it is triggered
# this time is NOT added to buffer time when recording
# -----------------------------------------------------------------
DEFAULT_VIDEO_SNAP_DURATION = 60

# default filename template for persistent multi-segment recording
# '{}' denotes camera name; %d denotes segment number (%05d is five-zeros-padded)
# -------------------------------------------------------------------------------
DEFAULT_RECORDING_FILENAME = 'recording/record_{}_%05d.mp4'

# default segment duration in minutes for persistent multi-segment recording
# --------------------------------------------------------------------------
DEFAULT_RECORDING_SEGMENT_DURATION = 15

# default number of segment files kept in storage, per camera
# set to 0 for no limit
# -----------------------------------------------------------
DEFAULT_NUMBER_STORED_SEGMENTS = 0

# assumed video bitrate, in megabytes per second (MB/s)
# -----------------------------------------------------
ESTIMATED_CAMERA_BITRATE = 5.5

# image encoder spin up time
# --------------------------
IMAGE_ENCODE_SPIN_UP = 3.0

# image snap pipeline execute time
# --------------------------------
IMAGE_SNAP_EXECUTE_TIME = 2.0

# default camera stream reporting interval
# ----------------------------------------
DEFAULT_CAMERA_REPORTING_INTERVAL = 15
