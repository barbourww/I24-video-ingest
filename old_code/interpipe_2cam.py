#!/usr/bin/env python3

import time
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


if __name__ == '__main__':

    pipelines_cameras = []
    pipelines_video_enc = []
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
    client = GstdClient(ip='localhost', port=5000, logger=gstd_py_logger)
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

        # Create encoding/processing pipelines
        # ------------------------------------
        # Transcode H.264 to JPEG
        encode_jpeg = PipelineEntity(client, 'encode_jpeg', 'interpipesrc name=encode_jpeg_src format=time listen-to=camera0 ! avdec_h264 ! jpegenc ! interpipesink name=encode_jpeg_sink forward-events=true forward-eos=true sync=false async=false enable-last-sample=false drop=true')
        pipelines_video_enc.append(encode_jpeg)
        # Buffer video for a certain amount of time via a queue element configured as FIFO
        buffer_time = 30 * 1e9
        for camera in pipelines_cameras:
            cam_name = camera.get_name()
            # Disable number of buffers limit; set 1GB (1024^3 B) max size for safety. 
            new_buffer = PipelineEntity(client, 'buffer_h264', 'interpipesrc name=buffer_{} format=time listen-to={} ! queue name=fifo_queue max-size-buffers=0 max-size-bytes=1073741824 leaky=2 silent=true flush-on-eos=true')
            new_buffer.set_property('fifo_queue', 'max-size-time', str(int(buffer_time)))
            pipelines_video_enc.append()

        # Create persistent recording pipelines
        # -------------------------------------
        # H.264 recording via MPEG4 container mux to parallel streams
        pipe_descr = ''
        for ci, camera in pipelines_cameras:
            cam_name = camera.get_name()
            pipe_descr += ' ' + 'interpipesrc format=time allow-renegotiation=false listen-to={} ! vidmux.video_{}'.format(cam_name, ci)
        pipe_descr += ' ' + 'mp4mux name=vidmux ! filesink name=filesink_record_h264'
        record_h264 = PipelineEntity(client, 'record_h264', pipe_descr)
        pipelines_video_rec.append(record_h264)

        # Create snapshot pipelines
        # -------------------------
        # JPEG snapshot - connects to only one camera at a time via encode_jpeg pipeline and dumps a frame to file
        snap_jpeg = PipelineEntity(client, 'snap_jpeg', 'interpipesrc name=snap_jpeg_src format=time listen-to=encode_jpeg_sink num-buffers=1 ! filesink name=filesink_snap_jpeg')
        pipelines_snap.append(snap_jpeg)
        # Buffered video snapshot - conencts to queue-buffers from each camera, muxes, and file-sinks
        pipe_descr = ''
        for ci, camera in pipelines_cameras:
            cam_name = camera.get_name()
            pipe_descr += ' ' + 'interpipesrc format=time allow-renegotiation=false listen-to={} ! snapmux.video_{}'.format(cam_name, ci)
        pipe_descr += ' ' + 'mp4mux name=snapmux ! filesink name=filesink_snap_video'
        snap_video = PipelineEntity(client, 'snap_video', pipe_descr)
        pipelines_snap.append(snap_video)

        # Play base pipelines
        for pipeline in pipelines_cameras:
            pipeline.play()
        time.sleep(10)

        # Set locations for video recordings
        pipelines_video_rec[0].set_file_location('/home/dev/Videos/ingest_pipeline/test_mp4.mp4')
        # Play video recording pipelines
        for pipeline in pipelines_video_rec:
            pipeline.play()
        time.sleep(10)

        # Set location for snapshot
        for camera in ('camera0', 'camera1', 'camera2', 'camera3'):
            print("Snapping {}.".format(camera))
            encode_jpeg.listen_to(camera)
            pipelines_video_enc[0].play()
            time.sleep(2)
            snap_jpeg.set_file_location('/home/dev/Videos/ingest_pipeline/snap_{}.jpeg'.format(camera))
            snap_jpeg.play()
            time.sleep(3)
            snap_jpeg.stop()

        # Send EOS event to encode pipelines for proper closing
        # EOS to recording pipelines
        for pipeline in pipelines_video_rec:
            pipeline.eos()
        for pipeline in pipelines_video_enc:
            pipeline.eos()

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