from flask import Flask, render_template
from markupsafe import escape

from family_tree import Family

app = Flask(__name__)

@app.route('/<int:id>')
def person_page(id):
    family = Family()
    person = family.add_person(id)
    return render_template('person.html', person=person)