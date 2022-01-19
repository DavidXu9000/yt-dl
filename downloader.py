import sys
import time
import os
import yt_dlp
import time
import random
import subprocess

ytid = sys.argv[1]
start = sys.argv[2]
duration = sys.argv[3]
metadata = sys.argv[4] == "True"

base_options = {
    'format': '\"bestvideo[height<=720]+bestaudio/best[height<=720]\"',
    'no-overwrites': True,
    'quiet': True,
    'no-warnings': True,
    'output': '\"~/AudioSet/%(id)s.%(ext)s\"',
    'merge-output-format': 'mkv',
    'external-downloader': 'ffmpeg',
    'write-info-json': metadata,
    'continue': True,
    'throttled-rate': '100K',
    'retries': 5,
    'socket-timeout': 60,
    'no-progress': True,
    'no-cache-dir': True,
    'extractor-args': "\"youtube:skip=hls,dash\""
}
options = dict(base_options)    
options['external-downloader-args'] = "\"ffmpeg_i: -nostats -loglevel panic -hide_banner -ss %s -t %s\"" % (start, duration)

args = ""
for key in options.keys():
    if options[key] == True:
        args += "--%s " % (key)
    elif options[key] == False:
        continue
    else:
        args += "--%s %s " % (key, options[key])

err = subprocess.run("yt-dlp http://www.youtube.com/watch?v=%s %s" % (ytid, args), stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, shell=True).stderr
print(err.decode())
