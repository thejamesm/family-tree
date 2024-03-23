import psycopg2
import json
from datetime import date
from functools import cached_property

from config import load_config

class Family:
    def __init__(self):
        self.people = {}
        self.db = Database()

    def add_person(self, id):
        if id not in self.people:
            return Person(id, self)
        return self.people[id]

    def add_all(self):
        for id in self.db.get_ids():
            self.add_person(id)
        self.people = dict(sorted(self.people.items()))

    def search(self, search_string):
        return {p[0]: p[1] for p in self.people.items()
                if search_string in p[1].name}

    def save(self):
        with open(r'family_tree.json', mode='w', encoding='utf-8') as f:
            json.dump(list(self.people.values()), f, cls=PersonEncoder,
                      indent=4, ensure_ascii=False)

    def longest_line(self):
        return max([person.longest_line() for person in self.people.values()],
                   key=len)

class Person:
    def __init__(self, id, family=None):
        self.family = family

        if family:
            record = family.db.get_person(id)
        else:
            record = Database().get_person(id)

        self.id = record['person_id']
        self.name = record['person_name']
        self.gender = record['gender']
        self.dob = record['date_of_birth']
        self.dob_prec = record['date_of_birth_precision']
        if self.dob:
            self.date_of_birth = '-'.join(self.dob.split('-')[:self.dob_prec])
        else:
            self.date_of_birth = None
        self.place_of_birth = record['place_of_birth']
        self.dod = record['date_of_death']
        self.dod_prec = record['date_of_death_precision']
        if self.dod:
            self.date_of_death = '-'.join(self.dod.split('-')[:self.dod_prec])
        else:
            self.date_of_death = None
        self.place_of_death = record['place_of_death']
        self.__father_id = record['father_id']
        self.__mother_id = record['mother_id']
        
        if family:
            family.people[self.id] = self

    def __repr__(self):
        dates = self.dates()
        if dates:
            dates = f' ({dates})'
        else:
            dates = ''
        return self.name + dates

    @cached_property
    def father(self):
        father_id = self.__father_id
        if not father_id:
            return None
        if self.family:
            self.family.add_person(self.__father_id)
            father = self.family.people[father_id]
        else:
            father = Person(father_id)
        return father

    @cached_property
    def mother(self):
        mother_id = self.__mother_id
        if not mother_id:
            return None
        if self.family:
            self.family.add_person(self.__mother_id)
            mother = self.family.people[mother_id]
        else:
            mother = Person(mother_id)
        return mother

    @cached_property
    def children(self):
        child_ids = Database().get_child_ids(self.id)
        if self.family:
            children = [self.family.add_person(child_id)
                        for child_id in child_ids]
        else:
            children = [Person(child_id) for child_id in child_ids]
        return children

    def json(self):
        tree = {
                'id': self.id,
                'name': self.name,
                'gender': self.gender,
                'date_of_birth': self.date_of_birth,
                'date_of_death': self.date_of_death,
                'place_of_birth': self.place_of_birth,
                'place_of_death': self.place_of_death,
                'father': None,
                'mother': None,
                'children': [str(child) for child in self.children]
            }
        if self.father:
            tree['father'] = str(self.father)
        if self.mother:
            tree['mother'] = str(self.mother)
        return tree

    def json_flat(self):
        tree = {
                'id': self.id,
                'name': self.name,
                'gender': self.gender,
                'date_of_birth': self.date_of_birth,
                'date_of_death': self.date_of_death,
                'place_of_birth': self.place_of_birth,
                'place_of_death': self.place_of_death,
                'father_id': None,
                'mother_id': None,
                'child_ids': [child.id for child in self.children]
            }
        if self.father:
            tree['father_id'] = self.father.id
        if self.mother:
            tree['mother_id'] = self.mother.id
        return tree

    def dates(self):
        if self.date_of_birth and self.date_of_death:
            return f'{self.date_of_birth} - {self.date_of_death}'
        elif self.date_of_birth:
            return f'b. {self.date_of_birth}'
        elif self.date_of_death:
            return f'd. {self.date_of_death}'
        else:
            return None

    def ancestors(self):
        tree = {'person': self}
        if self.father:
            tree['father'] = self.father.ancestors()
        if self.mother:
            tree['mother'] = self.mother.ancestors()
        return tree

    def descendants(self):
        tree = {'person': self}
        if self.children:
            tree['children'] = [child.descendants()
                                for child in self.children]
        return tree

    def line(self):
        tree = self.ancestors()
        if self.children:
            tree['children'] = self.descendants()['children']
        return tree

    def longest_ancestor_line(self):
        father_line = (self.father.longest_ancestor_line() if self.father
                       else [])
        mother_line = (self.mother.longest_ancestor_line() if self.mother
                       else [])
        return max(father_line, mother_line, key=len) + [self]

    def longest_descendant_line(self):
        return [self] + max([child.longest_descendant_line()
                             for child in self.children],
                            default=[], key=len)

    def longest_line(self):
        return (self.longest_ancestor_line() +
                self.longest_descendant_line()[1:])

class Database:
    def __init__(self):
        self.config = load_config()

    @staticmethod
    def sanitize_field(value):
        """Return the field in a format suitable for JSON export."""
        if type(value) is date:
            return str(value)
        return value

    def get_all_records(self, sql, params=()):
        """Return the entire result of the supplied SQL query as a list."""
        if type(params) is not tuple:
            params = (params,)
        try:
            with psycopg2.connect(**self.config) as conn:
                with conn.cursor() as cur:
                    cur.execute(sql, params)
                    col_names = [col.name for col in cur.description]
                    rows = [{col_names[i]: Database.sanitize_field(field)
                            for i, field in enumerate(row)}
                            for row in cur.fetchall()]
                return rows
        except (psycopg2.DatabaseError, Exception) as e:
            print(e)

    def record_generator(self, sql, params=(), size=100):
        """Create an optionally batched generator from the supplied SQL."""
        try:
            with psycopg2.connect(**self.config) as conn:
                with conn.cursor() as cur:
                    cur.execute(sql, params)
                    col_names = [col.name for col in cur.description]
                    rows = cur.fetchmany(size)
                    while rows is not None and len(rows):
                        for row in rows:
                            yield {col_names[i]: Database.sanitize_field(field)
                                for i, field in enumerate(row)}
                        rows = cur.fetchmany(size)
        except Exception as e:
            print(e)

    def get_ids(self):
        """Return a list of all active IDs in the `people` table."""
        sql = """SELECT person_id
                FROM people
                ORDER BY person_id"""
        return tuple(x['person_id'] for x in self.get_all_records(sql))

    def get_people(self, match=None):
        """If `match` is supplied, return all people with names
           containing `match`.
           Otherwise, return the entire contents of the `people` table."""
        if match:
            match = f'%{match}%'
            sql = """SELECT *
                    FROM people
                    WHERE person_name LIKE %s
                    ORDER BY person_id"""
        else:
            sql = """SELECT *
                    FROM people
                    ORDER BY person_id"""
        return self.get_all_records(sql, match)

    def get_person(self, id):
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
        result = self.get_all_records(sql, id)
        if not result:
            raise ValueError('Person not found.')
        return result[0]

    def get_children(self, id):
        """Return a list of the children of a given person."""
        sql = """SELECT *
                FROM people
                WHERE father_id = %s
                    OR mother_id = %s
                ORDER BY date_of_birth"""
        return self.get_all_records(sql, (id, id))

    def get_child_ids(self, id):
        """Return a list of the IDs of the children of a specified person."""
        sql = """SELECT person_id
                   FROM people
                  WHERE %s IN (father_id, mother_id)
                  ORDER BY person_id"""
        records = self.get_all_records(sql, id)
        if not records:
            return tuple()
        return tuple(x['person_id'] for x in records)

    def get_line(self, id):
        """Return all ancestors and descendants of a given person."""
        person = self.get_person(id)
        if person['father_id']:
            person['father'] = self.get_ancestors(person['father_id'])
        if person['mother_id']:
            person['mother'] = self.get_ancestors(person['mother_id'])
        person['children'] = []
        children = self.get_children(id)
        for child in children:
            person['children'].append(self.get_descendants(child['person_id']))
        return person

    def get_descendants(self, id):
        """Return all descendants of a given person
           nested within their parents."""
        person = self.get_person(id)
        person['children'] = []
        children = self.get_children(id)
        for child in children:
            person['children'].append(self.get_descendants(child['person_id']))
        return person

    def get_descendants_flat(self, id):
        """Return all descendants of a given person as a flat list."""
        children = self.get_children(id)
        line = children
        for child in children:
            line.extend(self.get_descendants_flat(child['person_id']))
        return line

    def get_ancestors(self, id):
        """Return all ancestors of a given person
           nested within their children."""
        person = self.get_person(id)
        if person['father_id']:
            person['father'] = self.get_ancestors(person['father_id'])
        if person['mother_id']:
            person['mother'] = self.get_ancestors(person['mother_id'])
        return person

    def get_ancestors_flat(self, id):
        """Return all ancestors of a given person as a flat list."""
        person = self.get_person(id)
        line = [person]
        if person['father_id']:
            line.extend(self.get_ancestors_flat(person['father_id']))
        if person['mother_id']:
            line.extend(self.get_ancestors_flat(person['mother_id']))
        return line

class PersonEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Person):
            person = {
                    'id': obj.id,
                    'name': obj.name,
                    'gender': obj.gender,
                    'date_of_birth': obj.date_of_birth,
                    'place_of_birth': obj.place_of_birth,
                    'date_of_death': obj.date_of_death,
                    'place_of_death': obj.place_of_death,
                    'father_id': None,
                    'father': None,
                    'mother_id': None,
                    'mother': None,
                    'children': [{'child_id': child.id, 'child': str(child)}
                                 for child in obj.children]
                }
            if obj.father:
                person['father_id'] = obj.father.id
                person['father'] = str(obj.father)
            if obj.mother:
                person['mother_id'] = obj.mother.id
                person['mother'] = str(obj.mother)
            return person
        return super().default(obj)

if __name__ == '__main__':
    family = Family()
    family.add_all()
    print('\n'.join([f'{k}: {v}' for k, v in sorted(family.people.items())]))