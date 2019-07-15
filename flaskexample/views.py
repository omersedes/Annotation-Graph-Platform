from flaskexample import app
from flask import flash, request, render_template, redirect
from py2neo import Graph
from flaskexample.neo4j_connector import neo4jConnector

@app.route('/', methods=['GET', 'POST'])
@app.route('/index', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        search = request.form['search']
        return search_results(search)

    return render_template("index.html")

@app.route('/results')
def search_results(search):
    #results = HighestOutOccurence(search)
    #if not results:
    #    return redirect('/')
    
    #else:
    #    return render_template("results.html", len=len(results), results=results, search=search)
    return render_template("results.html", search=search)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=80, debug=True)
