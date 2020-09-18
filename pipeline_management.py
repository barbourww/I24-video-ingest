#!/usr/bin/env python3
from parameters import *

import time
import datetime
from pygstc.gstc import *
from pygstc.logger import *
from traceback import print_exc
import subprocess
import os


class PipelineEntity(object):
    """

    """
    def __init__(self, client, name, description):
        self._name = name
        self._description = description
        self._client = client
        print("Creating pipeline: " + self._name)
        self._client.pipeline_create(self._name, self._description)
    
    def get_name(self):
        return self._name

    def play(self):
        print("Playing pipeline: " + self._name)
        self._client.pipeline_play(self._name)
    
    def stop(self):
        print("Stopping pipeline: " + self._name)
        self._client.pipeline_stop(self._name)
    
    def delete(self):
        print("Deleting pipeline: " + self._name)
        self._client.pipeline_delete(self._name)
    
    def eos(self):
        print("Sending EOS to pipeline: " + self._name)
        self._client.event_eos(self._name)
    
    def set_file_location(self, location):
        print("Setting " + self._name + " pipeline recording/snapshot location to " + location)
        filesink_name = "filesink_" + self._name
        self._client.element_set(self._name, filesink_name, 'location', location)
    
    def set_property(self, element_name, property_name, property_value):
        print("Setting {} property to {}; element {} inside pipeline {}".format(property_name, property_value, element_name, self._name))
        self._client.element_set(self._name, element_name, property_name, property_value)
    
    def listen_to(self, sink):
        print(self._name + " pipeline listening to " + sink)
        self._client.element_set(self._name, self._name + '_src', 'listen-to', sink)


class GstdManager:
    """
    Manager class for starting and stopping GStreamer Daemon.
    """
    def __init__(self, gst_log=None, gstd_log=None, force_mkdir=False, gst_debug_level=5, 
                 tcp_enable=True, tcp_address='127.0.0.1', tcp_port=5000, num_tcp_ports=1,
                 http_enable=False, http_address='127.0.0.1', http_port=5001):
        # check input arguments
        if gst_log is not None:
            print("> GStreamer log file: {}".format(gst_log))
            logdir = os.path.split(gst_log)[0]
            if not os.path.exists(logdir):
                if force_mkdir is True:
                    print(">> Directory does not exist. Creating {}")
                    os.mkdir(logdir)
                else:
                    raise OSError("'gst_log' directory does not exist. Create or force with `force_mkdir`.")
        if gstd_log is not None:
            print("> GStreamer Daemon log file: {}".format(gstd_log))
            logdir = os.path.split(gst_log)[0]
            if not os.path.exists(logdir):
                if force_mkdir is True:
                    print(">> Directory does not exist. Creating {}")
                    os.mkdir(logdir)
                else:
                    raise OSError("'gstd_log' directory does not exist. Create or force with `force_mkdir`.")
        if type(gst_debug_level) is not int or gst_debug_level > 9 or gst_debug_level < 0:
            raise AttributeError("Provide integer [0, 9] for `gst_debug_level`.")
        # assemble arguments
        self.gstd_args = ['gstd']
        if gst_log is not None:
            self.gstd_args += ['--gst-log-filename', gst_log]
        if gstd_log is not None:
            self.gstd_args += ['--gstd-log-filename', gstd_log]
        self.gstd_args += ['--gst-debug-level', str(gst_debug_level)]
        if tcp_enable is True:
            self.gstd_args += ['--enable-tcp-protocol', '--tcp-address', tcp_address, 
                               '--tcp-base-port', str(tcp_port), '--tcp-num-ports', str(num_tcp_ports)]
        if http_enable is True:
            self.gstd_args += ['--enable-http-protocol', '--http-address', http_address, '--http-port', str(http_port)]
        print("Ready to start GStreamer Daemon.\nShell: {}".format(self.gstd_args))

    def start(self, restart=True):
        self.stop()
        print("Starting GStreamer Daemon...")
        gstd_proc = subprocess.run(self.gstd_args, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, universal_newlines=True)
        if gstd_proc.returncode == 0:
            print("Success.")
        elif gstd_proc.returncode == 1:
            print("Error starting GStreamer Daemon with command {}".format(self.gstd_args))
            if restart is False:
                print("Gstreamer Daemon may already be running. Consider stopping or setting `restart=True`.")
    def stop(self):
        gstd_stop = subprocess.run(['gstd', '--kill'], stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
        if 'no running gstd found' in gstd_stop.stderr.lower():
            print("No running GStreamer Daemon for STOP command.")


class IngestSession:
    """

    """
    def __init__(self, session_root_directory, session_config_file):
        """

        :param session_root_directory:
        :return: None
        """
        self.this_session_number = self._next_session_number(session_root_directory)
        self.session_absolute_directory = os.path.join(session_root_directory,
                                                       SESSION_DIRECTORY_FORMAT.format(self.this_session_number))
        if SESSION_DIRECTORY_FORMAT.format(self.this_session_number) in os.listdir(session_root_directory):
            raise FileExistsError("""Directory overwrite conflict! 
            Check parameters.SESSION_DIRECTORY_FORMAT and self._next_session_number().""")
        os.mkdir(self.session_absolute_directory)
        # fill configuration variables
        # TODO: list all the config variables here with None initialization
        self._parse_config_file(session_config_file)
        self._copy_config_file(session_config_file)


    def _next_session_number(self, session_root_directory):
        """
        Checks what directories present in `session_root_directory` match the SESSION_DIRECTORY_FORMAT and calculates
            the next session number. Does not wrap around to zero.
        :param session_root_directory: Root directory where session-specific directories will be placed.
        :return: largest session number found in `session_root_directory` plus 1
        """
        root_list_dir = os.listdir(session_root_directory)
        present_matches = []
        for i in range(0, 99999):
            if SESSION_DIRECTORY_FORMAT.format(i) in root_list_dir:
                present_matches.append(i)
        return max(present_matches) + 1


    def _parse_config_file(self, config_file):
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
                if line is None or len(line) == 0 or line[0] == '#':
                    continue
                strip_line = line.strip()
                if len(strip_line) > 2 and strip_line[:2] == '__' and strip_line[-2:] == '__':
                    # this is a configuration block line
                    # first check if this is the first one or not
                    if block_destination is not None and len(current_block) > 0:
                        # add the block to its destination if it's non-empty
                        block_destination.append(current_block)
                    # reset current block to empty and set its destination
                    current_block = []
                    block_destination = block_mapping[strip_line]
                elif '=' in strip_line:
                    current_block.append(tuple(strip_line.split('=')))
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
        # log configs then return them
        print("Camera configuration:", camera_config)
        print("Image snapshot configuration:", image_snap_config)
        print("Video snapshot configuration:", video_snap_config)
        print("Persistent recording configuration:", recording_config)
        return camera_config, image_snap_config, video_snap_config, recording_config


    def _copy_config_file(self, config_file):
        with open(config_file, 'r') as config_orig:
            with open(os.path.join(self.session_absolute_directory, "this_session.config"), 'w') as config_copy:
                for line in config_orig:
                    config_copy.write(line)

    def _write_session_header_file(self):
        with open(os.path.join(self.session_absolute_directory, '_SESSION_INFO.txt')) as f:
            f.write("SESSION #{}".format(self.this_session_number))
            f.write("INFORMATIONAL/HEADER FILE")
            f.write("-" * 50)
            # directory information
            f.write("\nDirectory (absolute): {}".format(self.session_absolute_directory))
            # time information
            timenow = datetime.datetime.now()
            unix_timenow = (timenow - datetime.datetime(year=1970, month=1, day=1)).total_seconds()
            f.write("\nSession initialization time (local): {} (UNIX: {})".format(timenow, unix_timenow))
            utctimenow = datetime.datetime.utcnow()
            unix_utctimenow = (utctimenow - datetime.datetime(year=1970, month=1, day=1)).total_seconds()
            f.write("Session initialization time (UTC): {} (UNIX: {})".format(utctimenow, unix_utctimenow))
            # camera information
            # TODO: put in config information for cameras, recording, etc

    def initialize_gstd(self):
        pass

    def construct_pipelines(self):
        pass

    def start_cameras(self):
        pass

    def begin_persistent_recording_all_cameras(self):
        pass

    def stop_persistent_recording_all_cameras(self):
        pass

    def take_image_snapshot(self):
        pass

    def take_video_snapshot(self, duration):
        pass

    def deconstruct_all_pipelines(self):
        pass

if __name__ == '__main__':

    pipelines_cameras = []
    pipelines_video_enc = []
    pipelines_video_buffer = []
    pipelines_video_rec = []
    pipelines_snap = []

    # Start GstD
    # ----------
    manager = GstdManager(gst_log='/home/dev/Documents/ingest_pipeline/log/gst.log', gstd_log='/home/dev/Documents/ingest_pipeline/log/gstd.log', 
                          gst_debug_level=5, tcp_enable=True, http_enable=False)
    manager.start()

    # Create GstD Python client
    # -------------------------
    gstd_py_logger = CustomLogger(logname="ingest", logfile="/home/dev/Documents/ingest_pipeline/log/pygstc.log", loglevel="DEBUG")
    num_retry = 3
    for i in range(num_retry):
        try:
            client = GstdClient(ip='localhost', port=5000, logger=gstd_py_logger)
            break
        except GstcError:
            print("Problem connecting to Gstd.")
            print_exc()
            time.sleep(1)
    else:
        raise GstcError("Could not contact Gstd after {} attempts.".format(num_retry))
    client.debug_enable(True)

    try:
        # Create camera pipelines
        camera0 = PipelineEntity(client, 'camera0', 'rtspsrc location=rtsp://root:password@192.168.0.124/axis-media/media.amp ! rtph264depay ! h264parse ! queue ! interpipesink name=camera0 forward-events=true forward-eos=true sync=false')
        pipelines_cameras.append(camera0)
        camera1 = PipelineEntity(client, 'camera1', 'rtspsrc location=rtsp://root:password@192.168.0.239/axis-media/media.amp ! rtph264depay ! h264parse ! queue ! interpipesink name=camera1 forward-events=true forward-eos=true sync=false')
        pipelines_cameras.append(camera1)
        camera2 = PipelineEntity(client, 'camera2', 'rtspsrc location=rtsp://root:password@192.168.0.124/axis-media/media.amp ! rtph264depay ! h264parse ! queue ! interpipesink name=camera2 forward-events=true forward-eos=true sync=false')
        pipelines_cameras.append(camera2)
        camera3 = PipelineEntity(client, 'camera3', 'rtspsrc location=rtsp://root:password@192.168.0.239/axis-media/media.amp ! rtph264depay ! h264parse ! queue ! interpipesink name=camera3 forward-events=true forward-eos=true sync=false')
        pipelines_cameras.append(camera3)

        # Create encoding and buffering pipelines
        # ------------------------------------
        # Transcode H.264 to JPEG
        encode_jpeg = PipelineEntity(client, 'encode_jpeg',
                                     'interpipesrc name=encode_jpeg_src format=time listen-to=camera0 ! avdec_h264 ! jpegenc ! interpipesink name=encode_jpeg_sink forward-events=true forward-eos=true sync=false async=false enable-last-sample=false drop=true')
        pipelines_video_enc.append(encode_jpeg)
        # Buffer video for a certain amount of time via a queue element configured as FIFO
        min_buffer_time = 30 * 1e9                          # number of seconds * 1e9 ns/s
        overflow_time = min_buffer_time * 1.05              # set max time at 105% min time
        overflow_size = overflow_time * 11 * 1024 * 1024    # set max size with 11 MB/s (actual bitrate ~ 5.5 MB/s)
        for camera in pipelines_cameras:
            cam_name = camera.get_name()
            # Set leaky=2 for FIFO; silent=true for no events; disable number of buffers limit.
            new_buffer = PipelineEntity(client, 'buffer_h264_{}'.format(cam_name),
                                        'interpipesrc format=time listen-to={} ! queue name=fifo_queue_{} max-size-buffers=0 max-size-bytes=1073741824 leaky=2 silent=true flush-on-eos=false ! interpipesink name=buffer_{} forward-events=true forward-eos=true sync=false'.format(cam_name, cam_name, cam_name))
            new_buffer.set_property('fifo_queue_{}'.format(cam_name), 'min-threshold-time', str(int(min_buffer_time)))
            new_buffer.set_property('fifo_queue_{}'.format(cam_name), 'max-size-time', str(int(overflow_time)))
            pipelines_video_buffer.append(new_buffer)

        # Create persistent recording pipelines
        # -------------------------------------
        # H.264 recording via MPEG4 container mux to parallel streams
        max_file_time = 40 * 1e9        # number of seconds * 1e9 ns/s
        pipe_descr = ''
        for ci, camera in enumerate(pipelines_cameras):
            cam_name = camera.get_name()
            pipe_descr += ' ' + 'interpipesrc format=time allow-renegotiation=false listen-to={} ! splitmuxsink name=multisink_{} async-finalize=true muxer-pad-map=x-pad-map,video=video_0'.format(cam_name, cam_name)
        record_h264 = PipelineEntity(client, 'record_h264', pipe_descr)
        for camera in pipelines_cameras:
            record_h264.set_property('multisink_{}'.format(camera.get_name()), 'max-size-time', str(int(max_file_time)))
        pipelines_video_rec.append(record_h264)

        # Create snapshot pipelines
        # -------------------------
        # JPEG snapshot - connects to only one camera at a time via encode_jpeg pipeline and dumps a frame to file
        snap_jpeg = PipelineEntity(client, 'snap_jpeg',
                                   'interpipesrc name=snap_jpeg_src format=time listen-to=encode_jpeg_sink num-buffers=1 ! filesink name=filesink_snap_jpeg')
        pipelines_snap.append(snap_jpeg)
        # Buffered video snapshot - conencts to queue-buffers from each camera, muxes, and file-sinks
        pipe_descr = ''
        for ci, camera in enumerate(pipelines_cameras):
            cam_name = camera.get_name()
            pipe_descr += ' ' + 'interpipesrc format=time allow-renegotiation=false listen-to=buffer_{} ! snapmux.video_{}'.format(cam_name, ci)
        pipe_descr += ' ' + 'mp4mux name=snapmux ! filesink name=filesink_snap_video'
        snap_video = PipelineEntity(client, 'snap_video', pipe_descr)
        pipelines_snap.append(snap_video)

        # Play base pipelines
        for pipeline in pipelines_cameras:
            pipeline.play()
        for pipeline in pipelines_video_buffer:
            pipeline.play()
        time.sleep(10)

        # Set locations for video recordings
        for camera in pipelines_cameras:
            cam_name = camera.get_name()
            record_h264.set_property('multisink_{}'.format(cam_name), 'location',
                                     '/home/dev/Videos/ingest_pipeline/record_{}_%05d.mp4'.format(cam_name))
        # Play video recording pipelines
        for pipeline in pipelines_video_rec:
            pipeline.play()
        time.sleep(60)

        # Execute video snapshot
        snap_video.set_file_location('/home/dev/Videos/ingest_pipeline/video_snap.mp4')
        snap_video.play()
        time.sleep(20)
        snap_video.eos()

        # Execute still image snapshot
        # 5 second runtime per camera
        for camera in ('camera0', 'camera1', 'camera2', 'camera3'):
            print("Snapping {}.".format(camera))
            encode_jpeg.listen_to(camera)
            encode_jpeg.play()
            time.sleep(3)
            snap_jpeg.set_file_location('/home/dev/Videos/ingest_pipeline/snap_{}.jpeg'.format(camera))
            snap_jpeg.play()
            time.sleep(2)
            snap_jpeg.stop()

        # Send EOS event to encode pipelines for proper closing
        # EOS to recording pipelines
        for pipeline in pipelines_video_rec:
            pipeline.eos()
        for pipeline in pipelines_video_enc:
            pipeline.eos()

        time.sleep(10)

        # Stop pipelines
        for group in (pipelines_snap, pipelines_video_rec, pipelines_video_enc, pipelines_cameras):
            for pipeline in group:
                try:
                    pipeline.stop()
                except:
                    pass
    except KeyboardInterrupt:
        print_exc()
    finally:
        # Delete pipelines
        for group in (pipelines_snap, pipelines_video_rec, pipelines_video_enc, pipelines_cameras):
            for pipeline in group:
                pipeline.delete()
        manager.stop()