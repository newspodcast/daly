#!/bin/sh
cd /opt/newsdemon/src
python3 news.py podcast.xml
cd ../upload
git checkout gh-pages
cp ../src/podcast.xml podcast.xml
git commit -m "update" -a
git push
