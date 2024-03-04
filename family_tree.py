import psycopg2
import json
from datetime import date

from config import load_config

class Person:
    family = {}
    def __init__(self, id):
        Person.family[id] = self
        record = get_person(id)
        self.children = []
        self.id = id
        self.name = record['person_name']
        self.date_of_birth = record['date_of_birth']
        self.date_of_death = record['date_of_death']
        self.date_of_birth_precision = record['date_of_birth_precision']
        self.date_of_death_precision = record['date_of_death_precision']
        if record['father_id']:
            if record['father_id'] in Person.family:
                self.father = Person.family[record['father_id']]
            else:
                self.father = Person(record['father_id'])
            self.father.add_child(self)
        else:
            self.father = None
        if record['mother_id']:
            if record['mother_id'] in Person.family:
                self.mother = Person.family[record['mother_id']]
            else:
                self.mother = Person(record['mother_id'])
            self.mother.add_child(self)
        else:
            self.mother = None

    def __str__(self):
        dates = self.dates()
        if dates:
            dates = f' ({dates})'
        else:
            dates = ''
        output = [self.name + dates]
        if self.father:
            output.append('Father: ' + self.father.name)
        if self.mother:
            output.append('Mother: ' + self.mother.name)
        if self.children:
            output.append('Children: ' +
                          ', '.join([c.name for c in self.children]))
        return '\n'.join(output)

    def dates(self):
        if self.date_of_birth and self.date_of_death:
            return f'{self.date_of_birth} - {self.date_of_death}'
        elif self.date_of_birth:
            return f'b. {self.date_of_birth}'
        elif self.date_of_death:
            return f'd. {self.date_of_death}'
        else:
            return None
    
    def add_child(self, child):
        self.children.append(child)
    
    @classmethod
    def add_all(cls):
        ids = get_ids()
        for id in ids:
            if id not in cls.family:
                Person(id)

def sanitize_field(value):
    """Return the field in a format suitable for JSON export."""
    if type(value) is date:
        return str(value)
    return value

def get_all_records(sql, params=()):
    """Return the entire result of the supplied SQL query as a list."""
    config = load_config()
    if type(params) is not tuple:
        params = (params,)
    try:
        with psycopg2.connect(**config) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                col_names = [col.name for col in cur.description]
                rows = [{col_names[i]: sanitize_field(field)
                         for i, field in enumerate(row)}
                        for row in cur.fetchall()]
            return rows
    except (psycopg2.DatabaseError, Exception) as e:
        print(e)
    
def record_generator(sql, params=(), size=100):
    """Create an optionally batched generator from the supplied SQL."""
    config = load_config()
    try:
        with psycopg2.connect(**config) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                col_names = [col.name for col in cur.description]
                rows = cur.fetchmany(size)
                while rows is not None and len(rows):
                    for row in rows:
                        yield {col_names[i]: sanitize_field(field)
                               for i, field in enumerate(row)}
                    rows = cur.fetchmany(size)
    except Exception as e:
        print(e)
    
def get_ids():
    """Return a list of all active IDs in the `people` table."""
    sql = """SELECT person_id
               FROM people
              ORDER BY person_id"""
    return tuple(x['person_id'] for x in get_all_records(sql))

def get_people(match=None):
    """If `match` is supplied, return all people with names containing `match`.
       Otherwise, return the entire contents of the `people` table."""
    if match:
        match = f'%{match}%'
        print('Fuzzy match: ', match)
        sql = """SELECT *
                   FROM people
                  WHERE person_name LIKE %s
                  ORDER BY person_id"""
    else:
        sql = """SELECT *
                   FROM people
                  ORDER BY person_id"""
    return get_all_records(sql, match)

def get_person(id):
    """Return a single person's record."""
    if type(id) is int or (type(id) is str and id.isnumeric()):
        sql = """SELECT *
                   FROM people
                  WHERE person_id = %s"""
    else:
        id = f'%{id}%'
        sql = """SELECT *
                   FROM people
                  WHERE person_name LIKE %s
                  ORDER BY person_id"""
    return get_all_records(sql, id)[0]

def get_children(id):
    """Return a list of the children of a given person."""
    sql = """SELECT *
               FROM people
              WHERE father_id = %s
                 OR mother_id = %s
              ORDER BY date_of_birth"""
    return get_all_records(sql, (id, id))

def get_line(id):
    """Return all ancestors and descendents of a given person."""
    person = get_person(id)
    if person['father_id']:
        person['father'] = get_ancestors(person['father_id'])
    if person['mother_id']:
        person['mother'] = get_ancestors(person['mother_id'])
    person['children'] = []
    children = get_children(id)
    for child in children:
        person['children'].append(get_descendents(child['person_id']))
    return person

def get_descendents(id):
    """Return all descendents of a given person nested within their parents."""
    person = get_person(id)
    person['children'] = []
    children = get_children(id)
    for child in children:
        person['children'].append(get_descendents(child['person_id']))
    return person

def get_descendents_flat(id):
    """Return all descendents of a given person as a flat list."""
    children = get_children(id)
    line = children
    for child in children:
        line.extend(get_descendents_flat(child['person_id']))
    return line

def get_ancestors(id):
    """Return all ancestors of a given person nested within their children."""
    person = get_person(id)
    if person['father_id']:
        person['father'] = get_ancestors(person['father_id'])
    if person['mother_id']:
        person['mother'] = get_ancestors(person['mother_id'])
    return person

def get_ancestors_flat(id):
    """Return all ancestors of a given person as a flat list."""
    person = get_person(id)
    line = [person]
    if person['father_id']:
        line.extend(get_ancestors_flat(person['father_id']))
    if person['mother_id']:
        line.extend(get_ancestors_flat(person['mother_id']))
    return line

if __name__ == '__main__':
    pass