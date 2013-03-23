#!/usr/bin/env python

import sys
import time
import datetime
import requests
import unicodecsv
import StringIO
def ilines(source_iterable):
    '''yield lines as in universal-newlines from a stream of data blocks'''
    tail = ''
    for block in source_iterable:
        if not block:
            continue
        if tail.endswith('\015'):
            yield tail[:-1] + '\012'
            if block.startswith('\012'):
                pos = 1
            else:
                tail = ''
        else:
            pos = 0
        try:
            while True: # While we are finding LF.
                npos = block.index('\012', pos) + 1
                try:
                    rend = npos - 2
                    rpos = block.index('\015', pos, rend)
                    if pos:
                        yield block[pos : rpos] + '\n'
                    else:
                        yield tail + block[:rpos] + '\n'
                    pos = rpos + 1
                    while True: # While CRs 'inside' the LF
                        rpos = block.index('\015', pos, rend)
                        yield block[pos : rpos] + '\n'
                        pos = rpos + 1
                except ValueError:
                    pass
                if '\015' == block[rend]:
                    if pos:
                        yield block[pos : rend] + '\n'
                    else:
                        yield tail + block[:rend] + '\n'
                elif pos:
                    yield block[pos : npos]
                else:
                    yield tail + block[:npos]
                pos = npos
        except ValueError:
            pass
        # No LFs left in block.  Do all but final CR (in case LF)
        try:
            while True:
                rpos = block.index('\015', pos, -1)
                if pos:
                    yield block[pos : rpos] + '\n'
                else:
                    yield tail + block[:rpos] + '\n'
                pos = rpos + 1
        except ValueError:
            pass

        if pos:
            tail = block[pos:]
        else:
            tail += block
    if tail:
        yield tail



from flask import Flask, Response, request
app = Flask(__name__)

@app.route("/add_uniq/<colname>/<path:url>")
def add_uniq(colname, url):
    r = requests.get(url, stream=True)
    def gen_lines():
        i = 0
        for line in r.iter_lines():
            if not i:
                suffix = colname
            else:
                suffix = str(i)

            line = line.strip()
            line += "," + suffix + "\n"
            yield line
            i += 1

    return Response(gen_lines(), mimetype="text/csv")

@app.route("/hack_csv/")
def hack_csv():
    date_column = request.args["date_col"]
    date_format = request.args["date_fmt"]
    uniq_column = request.args["uniq_col"]
    url = request.args["url"]

    if url.startswith("file://"):
        r = file(url[7:])
    else:
        r = ilines(requests.get(url, stream=True).iter_content(chunk_size=4096))

    def gen_csv_data():
        i = 0

        f = StringIO.StringIO()

        reader = unicodecsv.DictReader(r)
        writer = unicodecsv.writer(f)

        fields = reader.fieldnames
        fields.append(uniq_column)

        f.seek(0)
        writer.writerow(fields)
        l = f.tell()
        f.seek(0)
        yield f.read(l)

        i = 0
        for row in reader:
            i += 1
            row[uniq_column] = i

            tv = time.strptime(row[date_column], date_format)
            date = datetime.datetime(*tv[:6]).strftime('%Y-%m-%d')
            row[date_column] = date

            data = map(lambda i: row[i], fields)
            f.seek(0)
            writer.writerow(data)
            l = f.tell()
            f.seek(0)
            yield f.read(l)

    return Response(gen_csv_data(), mimetype="text/csv")

def run():
    _, port = sys.argv
    port = int(port)
    app.run(port=port, debug=True)

if __name__ == '__main__':
    run()

