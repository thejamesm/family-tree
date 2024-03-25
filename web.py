import os.path

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
    person = family.person(id)
    if os.path.isfile(os.path.join('static', 'images', f'{id}.jpg')):
        img_filename = f'images/{id}.jpg'
    else:
        img_filename = None
    return render_template('person.html', person=person,
                           img_filename=img_filename)
