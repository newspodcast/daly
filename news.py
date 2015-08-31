import feedparser as fd

import re
import urllib.request as rq
import json

import datetime
import PyRSS2Gen
import sys
import os
import sqlite3
import codecs

def quote_identifier(s, errors="strict"):
    encodable = s.encode("utf-8", errors).decode("utf-8")

    nul_index = encodable.find("\x00")

    if nul_index >= 0:
        error = UnicodeEncodeError("NUL-terminated utf-8", encodable,
                                   nul_index, nul_index + 1, "NUL not allowed")
        error_handler = codecs.lookup_error(errors)
        replacement, _ = error_handler(error)
        encodable = encodable.replace("\x00", replacement)

    return "\"" + encodable.replace("\"", "\"\"") + "\""

abendschau = "http://www.ardmediathek.de/tv/Abendschau/Sendung?documentId=3822076&bcastId=3822076&rss=true"
tagesschau = "http://www.tagesschau.de/export/video-podcast/webl/tagesschau"
tagesschauShort = "http://www.tagesschau.de/export/video-podcast/webl/tagesschau-in-100-sekunden/"

db_filename = 'news_podcast.db'
schema_filename = 'news_podcast.sql'

def makeDatabase():
    db_is_new = not os.path.exists(db_filename)
    with sqlite3.connect(db_filename) as conn:
        if db_is_new:
            print('Creating schema')
            doc = str()
            with open(schema_filename, 'rt') as f:
                for line in f:
                    doc = doc + line
            conn.executescript(doc)
        else:
            print('Database exists, assume schema does, too.')

def getHistory():
    entries = []
    makeDatabase()
    with sqlite3.connect(db_filename) as conn:
        
        cursor = conn.cursor()

        cursor.execute("""
            select  title,link,summary, pubDate 
            from podcast 
            where pubDate BETWEEN datetime('now', '-6 days') AND datetime('now', 'localtime');
            """)
        for title,link,summary, pubDate in cursor.fetchall():
            date = datetime.datetime.strptime(pubDate,"%Y-%m-%d %H:%M")
            entries.append((title,link,summary,date))
    return entries

def update():
    entries = []
    
    #first fetch abenschau - only keep recent episodes
    abend = fd.parse(abendschau)
    updates = abend['entries']
    
    episodeMatcher = re.compile(r"(Abendschau - )(Abendschau vom \d+.\d+.\d+)")   
    
    processQueue = [] 
    history = []
    for update in updates:
        result = episodeMatcher.match(update['title'])
        if result:
            X = update['link']

            
            upd = update['published_parsed']
            upd = datetime.datetime(year=upd.tm_year,month=upd.tm_mon,day=upd.tm_mday)
            summary = update['summary']
            #nice little rest interface :D
            processQueue.append((result.group(2),"http://www.ardmediathek.de/play/media/"+X[X.rfind("documentId=")+11:X.rfind('&')]+"?devicetype=pc&features=",summary,upd))

    for update in processQueue:
        with rq.urlopen(update[1]) as resouce:
            doc = str()
            for line in resouce:
                doc = doc + line.decode('utf-8')
            
            video_url = json.loads(doc)['_mediaArray'][0]['_mediaStreamArray'][1]['_stream']
            data = (update[0],video_url,update[2],update[3])
            entries.append(data)
            history.append(data)
    commitToHistory(history,1)
    history = []
    #fetch tagesschauShort keep recent 12:00
    updates = fd.parse(tagesschauShort)['entries']
    for update in updates:
        title = update['title']
        video_url = update['links'][0]['href']
        upd = update['published_parsed']
        if upd.tm_hour != 12:
             continue
        upd = datetime.datetime(year=upd.tm_year,month=upd.tm_mon,day=upd.tm_mday)
        summary = update['summary']
        
        data = (title,video_url,summary,upd)
        entries.append(data)
        history.append(data)

    commitToHistory(history,2)
    history = []
    #fetch tagesschau
    updates = fd.parse(tagesschau)['entries']
    for update in updates:
        title = update['title']
        video_url = update['links'][0]['href']
        upd = update['published_parsed']
        upd = datetime.datetime(year=upd.tm_year,month=upd.tm_mon,day=upd.tm_mday)
        summary = update['summary']
        data = (title,video_url,summary,upd)
        entries.append(data)
        history.append(data)

    commitToHistory(history,3)
    history = []
    return entries

def commitToHistory(entries,t):
        makeDatabase()
        with sqlite3.connect(db_filename) as conn:
            cursor = conn.cursor()

            for e in entries:
                title = quote_identifier(e[0])
                link = quote_identifier(e[1])
                summary = quote_identifier(e[2])
                date = quote_identifier("%d-%02d-%02d %02d:%02d"%(e[3].year,e[3].month,e[3].day,e[3].hour,0))
                insertString = "insert into podcast (title,link,summary, pubDate,f_outlet) values"+ '('+title+','+link+','+summary+','+date+','+str(t)+')'
                cursor.execute(insertString)

def buildFeed(entries,output):
    entries.sort(key = lambda t:t[3],reverse=True)
    items = []
    for e in entries:
        items.append(PyRSS2Gen.RSSItem(
             title = e[0],
             link = e[1],
             pubDate=e[3],

             description = e[2]
             ))
    rss = PyRSS2Gen.RSS2(
        title='',
        link='',
        description = '',
        lastBuildDate = datetime.datetime.now(),
        items = items
    )
    rss.write_xml(open(output, "w"))

if __name__ == "__main__":
    fpath = sys.argv[1]
    update()
    buildFeed(getHistory(),fpath)