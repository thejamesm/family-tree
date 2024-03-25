import os.path

from flask import Flask, render_template, request, redirect, url_for
from markupsafe import escape

from family_tree import Family, Person

app = Flask(__name__)

@app.route('/')
def home():
    return redirect(url_for('person_page', id=1))

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

@app.route('/<int:id_a>/<int:id_b>')
def relatives(id_a, id_b):
    family = Family()
    family.add_all()
    person_a = family.person(id_a)
    person_b = family.person(id_b)
    kinship = person_a.kinship_term(person_b)
    return render_template('relatives.html',
                           person_a=person_a, person_b=person_b,
                           kinship=kinship)