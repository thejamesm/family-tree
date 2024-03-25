import psycopg2
import json
import os
from datetime import date
from functools import cached_property

import inflect

from config import load_config

class Family:
    def __init__(self):
        self.people = {}
        self.db = Database()

    def person(self, person):
        if type(person) is Person:
            id = person.id
            if id not in self.people:
                self.people[id] = person
                person.family = self
        else:
            id = person
            if id not in self.people:
                Person(id, family=self)
        self.people = dict(sorted(self.people.items()))
        return self.people[id]

    def add_all(self):
        for record in Database().get_people():
            Person(record=record, family=self)
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

    def kinship(self, person_a, person_b):
        def search_nested(needle, haystack):
            gen = (x[0] for x in enumerate(haystack) if needle in x[1])
            return next(gen, None)

        def all_parents(layer):
            return tuple(parent for x in layer
                         for parent in (x.father, x.mother)
                         if parent is not None)

        def calculate_kinship(common_ancestor, a_depth, b_depth):
            shorter_leg = min(a_depth, b_depth)
            difference = a_depth - b_depth
            return common_ancestor, shorter_leg, difference

        self.add_all()
        person_a = self.person(person_a)
        person_b = self.person(person_b)

        if person_a is person_b:
            return person_a, 0, 0

        a_ancs = [(person_a,)]
        b_ancs = [(person_b,)]
        a_done = False
        b_done = False

        while not (a_done and b_done):
            if not a_done:
                if a_parents := all_parents(a_ancs[-1]):
                    a_ancs.append(a_parents)
                else:
                    a_done = True

            if not b_done:
                if b_parents := all_parents(b_ancs[-1]):
                    b_ancs.append(b_parents)
                else:
                    b_done = True

            if not a_done:
                for ancestor in a_parents:
                    if (b_depth := search_nested(ancestor, b_ancs)) is not None:
                        a_depth = len(a_ancs) - 1
                        return calculate_kinship(ancestor, a_depth, b_depth)

            if not b_done:
                for ancestor in b_parents:
                    if (a_depth := search_nested(ancestor, a_ancs)) is not None:
                        b_depth = len(b_ancs) - 1
                        return calculate_kinship(ancestor, a_depth, b_depth)

        return None

class Person:
    @classmethod
    def search(cls, search_string):
        return [Person(record=record)
                for record in Database().get_people(search_string)]

    _pattern_parts = (('%#d', '%B', '%Y') if os.name == 'nt'
                      else ('%-d', '%B', '%Y'))

    def __init__(self, id=None, *, record=None, family=None):
        self.family = family

        if record is None:
            if family:
                record = family.db.get_person(id)
            else:
                record = Database().get_person(id)

        self.id = record['person_id']
        self.name = record['person_name']
        self.gender = record['gender']

        _dob = record['date_of_birth']
        self.dob_prec = record['date_of_birth_precision']
        if _dob:
            self.dob = date.fromisoformat(_dob)
            dob_pattern = ' '.join(Person._pattern_parts[3 - self.dob_prec:])
            self.date_of_birth = date.strftime(self.dob, dob_pattern)
            self.year_of_birth = self.dob.year
        else:
            self.dob = None
            self.date_of_birth = None
            self.year_of_birth = None
        self.place_of_birth = record['place_of_birth']

        _dod = record['date_of_death']
        self.dod_prec = record['date_of_death_precision']
        self.dod_unknown = record['date_of_death_unknown']
        if _dod:
            self.dod = date.fromisoformat(_dod)
            dod_pattern = ' '.join(Person._pattern_parts[3 - self.dod_prec:])
            self.date_of_death = date.strftime(self.dod, dod_pattern)
            self.year_of_death = self.dod.year
        elif self.dod_unknown:
            self.dod = None
            self.date_of_death = 'unknown'
            self.year_of_death = None
        else:
            self.dod = None
            self.date_of_death = None
            self.year_of_death = None
        self.place_of_death = record['place_of_death']

        self.occupation = record['occupation']
        self.notes = record['notes']
        self._father_id = record['father_id']
        self._mother_id = record['mother_id']

        if family:
            family.people[self.id] = self

    def __repr__(self):
        years = self.years
        if years:
            years = f' ({years})'
        else:
            years = ''
        return self.name + years

    @cached_property
    def dates(self):
        if self.date_of_birth and self.date_of_death:
            return f'{self.date_of_birth} – {self.date_of_death}'
        elif self.date_of_birth:
            return f'b. {self.date_of_birth}'
        elif self.date_of_death:
            return f'd. {self.date_of_death}'
        else:
            return None

    @cached_property
    def years(self):
        if self.year_of_birth and self.year_of_death:
            return f'{self.year_of_birth} – {self.year_of_death}'
        elif self.year_of_birth:
            return f'b. {self.year_of_birth}'
        elif self.year_of_death:
            return f'd. {self.year_of_death}'
        else:
            return None

    @cached_property
    def born(self):
        return ' in '.join(filter(None,
                                  (self.date_of_birth, self.place_of_birth)))

    @cached_property
    def died(self):
        return ' in '.join(filter(None,
                                  (self.date_of_death, self.place_of_death)))

    @cached_property
    def age(self):
        if (not self.dob) or self.dod_unknown:
            return None
        start = self.dob
        start_prec = self.dob_prec
        if self.dod:
            end = self.dod
            end_prec = self.dod_prec
        else:
            end = date.today()
            end_prec = 3
        age = end.year - start.year
        if min(start_prec, end_prec) < 3:
            return f'approx. {age}'
        if ((start.month > end.month) or
                (start.month == end.month) and (start.day > end.day)):
            return str(age - 1)
        return str(age)

    @cached_property
    def father(self):
        father_id = self._father_id
        if not father_id:
            return None
        if self.family:
            father = self.family.person(self._father_id)
        else:
            father = Person(father_id)
        return father

    @cached_property
    def mother(self):
        mother_id = self._mother_id
        if not mother_id:
            return None
        if self.family:
            mother = self.family.person(self._mother_id)
        else:
            mother = Person(mother_id)
        return mother

    @cached_property
    def parents(self):
        parents = []
        if self.father:
            parents.append(self.father)
        if self.mother:
            parents.append(self.mother)
        return tuple(parents)

    @cached_property
    def children(self):
        child_ids = Database().get_child_ids(self.id)
        if self.family:
            children = [self.family.person(child_id)
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

    def kinship_term(self, person):
        def calc_term(short, diff, gender):
            match short, diff, gender:
                case 0, 0, _:
                    return 'self'
                case 1, 0, 'male':
                    return 'brother'
                case 1, 0, 'female':
                    return 'sister'
                case 1, 0, _:
                    return 'sibling'
                case 0, 1, 'male':
                    return 'father'
                case 0, 1, 'female':
                    return 'mother'
                case 0, 1, _:
                    return 'parent'
                case 0, -1, 'male':
                    return 'son'
                case 0, -1, 'female':
                    return 'daughter'
                case 0, -1, _:
                    return 'child'
                case 1, 1, 'male':
                    return 'uncle'
                case 1, 1, 'female':
                    return 'aunt'
                case 1, 1, _:
                    return 'parent’s sibling'
                case 1, -1, 'male':
                    return 'nephew'
                case 1, -1, 'female':
                    return 'niece'
                case 1, -1, _:
                    return 'sibling’s child'
                case 0, 2, 'male':
                    return 'grandfather'
                case 0, 2, 'female':
                    return 'grandmother'
                case 0, 2, _:
                    return 'grandparent'
                case 0, -2, 'male':
                    return 'grandson'
                case 0, -2, 'female':
                    return 'granddaughter'
                case 0, -2, _:
                    return 'grandchild'
                case 1, 2, 'male':
                    return 'great uncle'
                case 1, 2, 'female':
                    return 'great aunt'
                case 1, 2, _:
                    return 'grandparent’s sibling'
                case 1, -2, 'male':
                    return 'great nephew'
                case 1, -2, 'female':
                    return 'great niece'
                case 1, -2, _:
                    return 'sibling’s grandchild'
                case 2, 0, _:
                    return 'cousin'
            return None

        if not (family := self.family):
            family = Family()
        kinship = family.kinship(self, person)
        if not kinship:
            return 'no blood relation'
        _, short, diff = kinship
        
        if term := calc_term(short, diff, person.gender):
            return term

        if short in (0, 1):
            levels = abs(diff) - 2
            prefix = '-'.join(('great',) * levels) + ' grand'
            sign = 1 if diff > 0 else -1
            return prefix + calc_term(short, sign, person.gender)

        p = inflect.engine()
        term = p.number_to_words(p.ordinal(short-1)) + ' cousin'
        if diff:
            diff = abs(diff)
            match diff:
                case 1:
                    removal = 'once'
                case 2:
                    removal = 'twice'
                case 3:
                    removal = 'thrice'
                case _:
                    removal = p.number_to_words(diff) + ' times'
            term = f'{term} {removal} removed'
        return term

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
                ORDER BY person_id;"""
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
                    ORDER BY person_id;"""
        else:
            sql = """SELECT *
                    FROM people
                    ORDER BY person_id;"""
        return self.get_all_records(sql, match)

    def get_person(self, id):
        """Return a single person's record."""
        if type(id) is int or (type(id) is str and id.isnumeric()):
            sql = """SELECT *
                    FROM people
                    WHERE person_id = %s;"""
        else:
            id = f'%{id}%'
            sql = """SELECT *
                    FROM people
                    WHERE person_name LIKE %s
                    ORDER BY person_id;"""
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
                ORDER BY date_of_birth;"""
        return self.get_all_records(sql, (id, id))

    def get_child_ids(self, id):
        """Return a list of the IDs of the children of a specified person."""
        sql = """SELECT person_id
                   FROM people
                  WHERE %s IN (father_id, mother_id)
                  ORDER BY person_id;"""
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