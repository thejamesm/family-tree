from flask import Flask, render_template, request
from markupsafe import escape

from family_tree import Family, Person

app = Flask(__name__)

@app.route('/search/')
def search():
    query = escape(request.args.get('query'))
    return render_template('search.html', results=Person.search(query))

@app.route('/<int:id>')
def person_page(id):
    family = Family()
    person = family.add_person(id)
    return render_template('person.html', person=person)