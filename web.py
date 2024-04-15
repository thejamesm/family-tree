import os.path

from flask import (Flask, render_template, request, redirect, url_for, flash,
                   session, send_from_directory)
from markupsafe import escape
from flask_login import (LoginManager, UserMixin, login_required, login_user,
                         logout_user)

from family_tree import Family, Person, SpuriousConnection
from config import load_config
from filters import parse_notes

class User(UserMixin):
    def __init__(self):
        self.id = 0

class SecuredImagesFlask(Flask):
    def send_static_file(self, filename):
        if filename.startswith('images/') or filename.startswith('trees/'):
            return self.asset_loader(filename)
        return super().send_static_file(filename)

    @login_required
    def asset_loader(self, filename):
        return super().send_static_file(filename)

config = load_config('authentication')

app = SecuredImagesFlask(__name__)

app.secret_key = config['secret_key']
app.config['USE_SESSION_FOR_NEXT'] = True
app.jinja_env.filters['parse_notes'] = parse_notes

login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'password_page'

@login_manager.user_loader
def user_loader(_):
    return User()

@app.route('/')
@login_required
def home():
    return redirect(url_for('person_page', id=1))

@app.route('/<int:id>')
@login_required
def person_page(id):
    try:
        family = Family(True)
        person = family.person(id)
        if os.path.isfile(os.path.join('static', 'images', f'{id}.jpg')):
            img_filename = f'images/{id}.jpg'
        else:
            img_filename = None
        return render_template('person.html', person=person,
                            img_filename=img_filename)
    except IndexError:
        return person_not_found()
    except SpuriousConnection:
        return person_not_found()

@app.route('/tree/<int:id>')
@login_required
def person_tree(id):
    path = os.path.join('static', 'trees', f'{id}.svg')
    print(path)
    if os.path.isfile(path):
        print('isfile')
        return redirect(url_for('static', filename=f'trees/{id}.svg'))
    print('isntfile')
    import draw_tree
    try:
        family = Family(True)
        subject = family.person(id)
        draw_tree.Tree(subject)
        return redirect(url_for('static', filename=f'trees/{id}.svg'))
    except IndexError:
        return person_not_found()
    except SpuriousConnection:
        return person_not_found()

@app.route('/<int:id_a>/<int:id_b>')
@login_required
def relatives(id_a, id_b):
    try:
        family = Family(True)
        person_a = family.person(id_a)
        person_b = family.person(id_b)
        kinship = person_a.kinship_term(person_b)
        return render_template('relatives.html',
                            person_a=person_a, person_b=person_b,
                            kinship=kinship)
    except IndexError:
        return person_not_found()
    except SpuriousConnection:
        return person_not_found()

@app.route('/search/')
@login_required
def search():
    family = Family()
    query = escape(request.args.get('query'))
    return render_template('search.html',
                           query=query,
                           results=Person.search(query, family=family))

@app.route('/password')
def password_page():
    return render_template('password.html')

@app.route('/password', methods=['POST'])
def check_password():
    password = request.form.get('password')
    if password == config['password']:
        user = User()
        login_user(user)
        if 'next' in session:
            return redirect(session.pop('next'))
        return redirect(url_for('home'))
    flash('Incorrect password')
    return redirect(url_for('password_page'))

@app.route('/logout')
def logout():
    logout_user()
    return redirect(url_for('password_page'))

@app.errorhandler(404)
def no_route(e):
    return render_template('no_route.html')
    
def person_not_found():
    return render_template('person_not_found.html')