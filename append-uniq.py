#!/usr/bin/python

import sys
import requests

from flask import Flask, Response
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

def run():
    _, port = sys.argv
    port = int(port)
    app.run(port=port)

if __name__ == '__main__':
    run()

