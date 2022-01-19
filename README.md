# yt-dl
This project implements a massively parallel downloader based on the yt-dlp fork of youtube-dl. We avoid throttling/ip blacklisting by downloading the videos through the UT Austin public lab machines. Using a large number of lab machines ensures that no individual machine downloads too frequently. Currently the tool is geared towards downloading AudioSet data.

It is still possible to get ip blacklisted. Use at your own risk!

### Dependencies
- Python 3.8+
- yt-dlp (https://github.com/yt-dlp/yt-dlp)
- ffmpeg

In addition, you should have passowordless ssh privileges to lab machines.

### Usage
```
python3 yt-dl.py [OPTIONS]
```

### Options
    --data                           AudioSet data csv location
    --ontology                       AudioSet ontology json location
    --num-workers                    Number of downloading processes
    --num-postprocessers             Number of postprocessor processes
    --user                           Username on public lab machines
    --domain                         Domain of public lab machines
    --hostnames                      Hostnames of public lab machines
    --tmp                            Temporary storage location
    --out                            Output directory
    --exp_dir                        Logfile directory
