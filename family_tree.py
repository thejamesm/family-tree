import psycopg2
import json
import os
from datetime import date
from functools import cached_property
from collections import defaultdict

import inflect

from config import load_config

app_config = load_config('family_tree')

class Family:
    def __init__(self):
        self.people = {}
        self.db = Database()
        self.inflect_engine = inflect.engine()

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
        for record in self.db.get_people():
            Person(record=record, family=self)
        self.people = dict(sorted(self.people.items()))
        self.child_ids = self.db.get_parent_child_id_pairs()

    def search(self, search_string):
        return {p[0]: p[1] for p in self.people.items()
                if search_string.lower() in p[1].name.lower()}

    def save(self):
        with open(r'family_tree.json', mode='w', encoding='utf-8') as f:
            json.dump(list(self.people.values()), f, cls=PersonEncoder,
                      indent=4, ensure_ascii=False)

    def get_longest_line(self):
        return max([person.get_longest_line() for person in self.people.values()],
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
    def search(cls, search_string, family=None):
        return [Person(record=record, family=family)
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
        self.dead = _dod or self.dod_unknown
        if _dod:
            self.dod = date.fromisoformat(_dod)
            dod_pattern = ' '.join(Person._pattern_parts[3 - self.dod_prec:])
            self.date_of_death = date.strftime(self.dod, dod_pattern)
            self.year_of_death = self.dod.year
        elif self.dod_unknown:
            self.dod = None
            self.date_of_death = None
            self.year_of_death = '?'
        else:
            self.dod = None
            self.date_of_death = None
            self.year_of_death = None
        self.place_of_death = record['place_of_death']

        self.occupation = record['occupation']
        self.notes = record['notes']
        self._father_id = record['father_id']
        self._mother_id = record['mother_id']

        self.spurious = record['spurious']

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
        elif self.dod_unknown:
            return f'†'
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
        if min(start_prec, end_prec) == 1:
            return f'approx. {age}'
        if (start.month > end.month):
            return str(age - 1)
        if (start.month == end.month) and (start.day > end.day):
            if min(start_prec, end_prec) == 2:
                return f'approx. {age - 1}'
            return str(age - 1)
        return str(age)

    @cached_property
    def father(self):
        father_id = self._father_id
        if not father_id:
            return None
        try:
            if self.family:
                father = self.family.person(self._father_id)
            else:
                father = Person(father_id)
            return father
        except SpuriousConnection:
            return None

    @cached_property
    def mother(self):
        mother_id = self._mother_id
        if not mother_id:
            return None
        try:
            if self.family:
                mother = self.family.person(self._mother_id)
            else:
                mother = Person(mother_id)
            return mother
        except SpuriousConnection:
            return None

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
        if self.family and self.family.child_ids:
            child_ids = self.family.child_ids[self.id]
        else:
            child_ids = Database().get_child_ids(self.id)
        if self.family:
            children = [self.family.person(child_id)
                        for child_id in child_ids]
        else:
            children = [Person(child_id) for child_id in child_ids]
        return sorted(children, key=lambda child: child.dob or date.max)

    @cached_property
    def siblings(self):
        if not (family := self.family):
            family = Family()
        records = family.db.get_siblings(self.id)
        return [Person(record=record, family=family) for record in records]

    @cached_property
    def full_siblings(self):
        if not (family := self.family):
            family = Family()
        records = family.db.get_full_siblings(self.id)
        return [Person(record=record, family=family) for record in records]

    @cached_property
    def half_siblings(self):
        if not (family := self.family):
            family = Family()
        records = family.db.get_half_siblings(self.id)
        return [Person(record=record, family=family) for record in records]

    @cached_property
    def relationships(self):
        if not (family := self.family):
            family = Family()
        records = family.db.get_partners(self.id)
        output = []
        for record in records:
            partner = family.person(record['person_id'])
            output.append(Relationship(self, partner, record))
        return output

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

    def get_ancestors(self):
        tree = {'person': self}
        if self.father:
            tree['father'] = self.father.get_ancestors()
        if self.mother:
            tree['mother'] = self.mother.get_ancestors()
        return tree

    def get_descendants(self):
        tree = {'person': self}
        if self.children:
            tree['children'] = [child.get_descendants()
                                for child in self.children]
        return tree

    def get_line(self):
        tree = self.get_ancestors()
        if self.children:
            tree['children'] = self.get_descendants()['children']
        return tree

    def get_longest_ancestor_line(self):
        father_line = (self.father.get_longest_ancestor_line()
                       if self.father else [])
        mother_line = (self.mother.get_longest_ancestor_line()
                       if self.mother else [])
        return max(father_line, mother_line, key=len) + [self]

    def get_longest_descendant_line(self):
        return [self] + max([child.get_longest_descendant_line()
                             for child in self.children],
                            default=[], key=len)

    def get_longest_line(self):
        return (self.get_longest_ancestor_line() +
                self.get_longest_descendant_line()[1:])

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

        def calc_spousal_term(short, diff, gender):
            match short, diff,  gender:
                case 0, 0, 'male':
                    return 'husband'
                case 0, 0, 'female':
                    return 'wife'
                case 0, 0, _:
                    return 'spouse'
                case 1, 0, 'male':
                    return 'brother-in-law'
                case 1, 0, 'female':
                    return 'sister-in-law'
                case 1, 0, _:
                    return 'sibling-in-law'
                case 0, 1, 'male':
                    return 'father-in-law'
                case 0, 1, 'female':
                    return 'mother-in-law'
                case 0, 1, _:
                    return 'parent-in-law'
                case 0, -1, 'male':
                    return 'stepson'
                case 0, -1, 'female':
                    return 'stepdaughter'
                case 0, -1, _:
                    return 'stepchild'
                case 1, -1, 'male':
                    return 'nephew by marriage'
                case 1, -1, 'female':
                    return 'niece by marriage'
                case 1, -1, _:
                    return 'spouse’s sibling’s child'
                case 0, 2, 'male':
                    return 'grandfather-in-law'
                case 0, 2, 'female':
                    return 'grandmother-in-law'
                case 0, 2, _:
                    return 'grandparent-in-law'
            return None

        def calc_affine_term(short, diff, gender):
            match short, diff, gender:
                case 1, 0, 'male':
                    return 'brother-in-law'
                case 1, 0, 'female':
                    return 'sister-in-law'
                case 1, 0, _:
                    return 'sibling-in-law'
                case 0, 1, 'male':
                    return 'stepfather'
                case 0, 1, 'female':
                    return 'stepmother'
                case 0, 1, _:
                    return 'step-parent'
                case 0, -1, 'male':
                    return 'son-in-law'
                case 0, -1, 'female':
                    return 'daughter-in-law'
                case 1, 1, 'male':
                    return 'uncle by marriage'
                case 1, 1, 'female':
                    return 'aunt by marriage'
                case 1, 2, 'male':
                    return 'great uncle by marriage'
                case 1, 2, 'female':
                    return 'great aunt by marriage'
            return None

        def calc_extended_term(short, diff, gender):
            p = self.family.inflect_engine
            if short in (0, 1):
                levels = abs(diff) - 2
                if levels <= int(app_config['max_great_levels']):
                    prefix = '-'.join(('great',) * levels) + ' grand'
                else:
                    prefix = (p.number_to_words(p.ordinal(levels)) +
                            '-great grand')
                sign = 1 if diff > 0 else -1
                return prefix + calc_term(short, sign, gender)
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

        prefix = ''
        suffix = ''

        if not (family := self.family):
            family = Family()
        kinship = family.kinship(self, person)
        if kinship:
            _, short, diff = kinship
            term = calc_term(short, diff, person.gender)
        else:
            for partner in [r.partner for r in self.relationships
                            if r.type == 'marriage']:
                kinship = family.kinship(partner, person)
                if kinship:
                    _, short, diff = kinship
                    term = calc_spousal_term(short, diff, person.gender)
                    if not term:
                        term = calc_term(short, diff, person.gender)
                        match partner.gender:
                            case 'male':
                                prefix = 'husband’s '
                            case 'female':
                                prefix = 'wife’s '
                            case _:
                                prefix = 'spouse’s '
                    break
            if not kinship:
                for partner in [r.partner for r in person.relationships
                                if r.type == 'marriage']:
                    kinship = family.kinship(self, partner)
                    if kinship:
                        _, short, diff = kinship
                        term = calc_affine_term(short, diff, person.gender)
                        if not term:
                            term = calc_term(short, diff, partner.gender)
                            match person.gender:
                                case 'male':
                                    suffix = '’s husband'
                                case 'female':
                                    suffix = '’s wife'
                                case _:
                                    suffix = '’s spouse'
                        break
            if not kinship:
                return 'no blood relation'

        if not term:
            term = calc_extended_term(short, diff, person.gender)

        return prefix + term + suffix

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

class Relationship:
    def __init__(self, person_a, person_b, record):
        self.id = record['relationship_id']
        self.type = record['relationship_type']
        self.people = (person_a, person_b)
        self.partner = person_b

        start_date = record['start_date']
        start_prec = record['start_date_precision']
        if start_date:
            self._start_date = date.fromisoformat(start_date)
            start_pattern = ' '.join(Person._pattern_parts[3 - start_prec:])
            self.start_date = date.strftime(self._start_date, start_pattern)
            self.start_year = self._start_date.year
        else:
            self._start_date = None
            self.start_date = None
            self.start_year = None
        self.start_place = record['place']

        end_date = record['end_date']
        end_prec = record['end_date_precision']
        if end_date:
            self._end_date = date.fromisoformat(end_date)
            end_pattern = ' '.join(Person._pattern_parts[3 - end_prec:])
            self.end_date = date.strftime(self._end_date, end_pattern)
            self.end_year = self._end_date.year
        else:
            self._end_date = None
            self.end_date = None
            self.end_year = None
        self.end_type = record['end_type']

    @cached_property
    def partner_description(self):
        match self.type, self.partner.gender:
            case 'marriage', 'male':
                return 'husband'
            case 'marriage', 'female':
                return 'wife'
            case 'marriage', _:
                return 'spouse'
            case 'couple', _:
                return 'partner'

    @cached_property
    def dates(self):
        someone_dead = any([person.dead for person in self.people])
        match self.start_date, self.end_date, someone_dead:
            case None, None, _:
                return None
            case _, None, False:
                return f'{self.start_date} –'
            case _, None, True:
                return f'{self.start_date} – ?'
            case None, _, _:
                return f'? – {self.end_date}'
            case _:
                return f'{self.start_date} – {self.end_date}'

    @cached_property
    def years(self):
        someone_dead = any([person.dead for person in self.people])
        match self.start_year, self.end_year, someone_dead:
            case None, None, _:
                return None
            case _, None, False:
                return f'{self.start_year} –'
            case _, None, True:
                return f'{self.start_year} – ?'
            case None, _, _:
                return f'? – {self.end_year}'
            case _:
                return f'{self.start_year} – {self.end_year}'

    @cached_property
    def started(self):
        if place := self.start_place:
            place = ' in ' + place
        return ((self.start_date or '') + (place or '')).strip() or None

    @cached_property
    def ended(self):
        return ', '.join(filter(None, (self.end_date, self.end_type))) or None

    @cached_property
    def description(self):
        return ', '.join(filter(None, (self.started, self.ended))) or None

    def end_type_description(self, noun=True, until=False):
        def death_description():
            per_a, per_b = self.people
            if per_a.dod and per_b.dod:
                per_a_died_first = per_a.dod < per_b.dod
            elif per_a.dod and self._end_date:
                per_a_died_first = per_a.dod <= self._end_date
            elif per_b.dod and self._end_date:
                per_a_died_first = per_b.dod > self._end_date
            else:
                return 'death'

            match per_a_died_first, per_a.gender, per_b.gender:
                case True, 'male', 'female':
                    return 'his death'
                case True, 'female', 'male':
                    return 'her death'
                case False, 'male', 'female':
                    return 'her death'
                case False, 'female', 'male':
                    return 'his death'
                case True, _, _:
                    return f'the death of {per_a.name}'
                case False, _, _:
                    return f'the death of {per_b.name}'

        if self.end_type == 'death':
            description = death_description()
            if until:
                return 'until ' + description
            return description

        if noun:
            return self.end_type
        
        match self.end_type:
            case 'marriage':
                return 'married'
            case 'divorce':
                return 'divorced'
            case 'separation':
                return 'separated'

        return None

class SpuriousConnection(Exception):
    pass

class Database:
    def __init__(self):
        self.db_config = load_config('postgresql')
        self.app_config = load_config('family_tree')
        if self.app_config['exclude_distant_history'].lower() == 'true':
            self.exclude_spurious = True
            self.where_condition = "WHERE spurious = 'FALSE'"
            self.and_condition = "AND spurious = 'FALSE'"
        else:
            self.exclude_spurious = False
            self.where_condition = ''
            self.and_condition = ''
        self.MPHONE_LEN = 15

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
            with psycopg2.connect(**self.db_config) as conn:
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
            with psycopg2.connect(**self.db_config) as conn:
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
        sql = f"""SELECT person_id
                    FROM people
                         {self.where_condition}
                   ORDER BY person_id ASC;"""
        return tuple(x['person_id'] for x in self.get_all_records(sql))

    def get_people(self, match=None):
        """If `match` is supplied, return all people with names
           containing `match`.
           Otherwise, return the entire contents of the `people` table."""
        if match is None:
            sql = f"""SELECT *
                        FROM people
                             {self.where_condition}
                       ORDER BY spurious ASC,
                                person_id ASC;"""
            return self.get_all_records(sql)
        wildcard_match = f'%{match}%'
        sql = f"""SELECT *
                    FROM people
                   WHERE person_name ILIKE %s
                         {self.and_condition}
                   ORDER BY spurious ASC,
                            person_id ASC;"""
        results = self.get_all_records(sql, wildcard_match)
        if not results:
            sql = f"""SELECT *
                        FROM people
                       WHERE METAPHONE(person_name, {self.MPHONE_LEN})
                             {self.and_condition}
                        LIKE '%%' || METAPHONE(%s, {self.MPHONE_LEN}) || '%%'
                       ORDER BY spurious ASC,
                                SIMILARITY(%s, person_name) DESC;"""
            results = self.get_all_records(sql, (match, match))
        return results

    def get_person(self, match):
        """For a given ID or name, return a single matching person."""
        if type(match) is int or (type(match) is str and match.isnumeric()):
            sql = f"""SELECT *
                        FROM people
                       WHERE person_id = %s;"""
            result = self.get_all_records(sql, match)
            if self.exclude_spurious and result[0]['spurious']:
                raise SpuriousConnection
        else:
            wildcard_match = f'%{match}%'
            sql = f"""SELECT *
                        FROM people
                       WHERE person_name ILIKE %s
                             {self.and_condition}
                       ORDER BY spurious ASC,
                                person_id ASC;"""
            result = self.get_all_records(sql, wildcard_match)
        if not result:
            sql = f"""SELECT *
                        FROM people
                       WHERE METAPHONE(person_name, {self.MPHONE_LEN})
                        LIKE '%%' || METAPHONE(%s, {self.MPHONE_LEN}) || '%%'
                             {self.and_condition}
                       ORDER BY spurious ASC,
                                SIMILARITY(%s, person_name) DESC;"""
            result = self.get_all_records(sql, (match, match))
            if not result:
                raise ValueError('Person not found.')
        return result[0]

    def get_children(self, id):
        """Return a list of the children of a given person."""
        sql = """SELECT *
                   FROM people
                  WHERE father_id = %s
                     OR mother_id = %s
                  ORDER BY spurious ASC,
                           date_of_birth ASC;"""
        return self.get_all_records(sql, (id, id))

    def get_child_ids(self, id):
        """Return a list of the IDs of the children of a specified person."""
        sql = """SELECT person_id
                   FROM people
                  WHERE %s IN (father_id, mother_id)
                  ORDER BY spurious ASC,
                           date_of_birth ASC;"""
        if not (records := self.get_all_records(sql, id)):
            return tuple()
        return tuple(x['person_id'] for x in records)

    def get_parent_child_id_pairs(self):
        """Return all pairs of parent and child ID numbers."""
        sql = """(SELECT father_id AS parent_id,
                         person_id AS child_id
                    FROM people
                   WHERE father_id IS NOT NULL
                   ORDER BY father_id,
                            person_id)
                 UNION ALL
                 (SELECT mother_id AS parent_id,
                         person_id AS child_id
                    FROM people
                   WHERE mother_id IS NOT NULL
                   ORDER BY mother_id,
                            person_id);"""
        records = self.get_all_records(sql)
        output = defaultdict(list)
        for record in records:
            output[record['parent_id']].append(record['child_id'])
        return output

    def get_siblings(self, id):
        """Return a list of people who share one or both of the given
           person's parents."""
        sql = """SELECT people.*
                   FROM people
                  CROSS JOIN (SELECT person_id, father_id, mother_id
                                FROM people
                               WHERE person_id = %s) AS person
                  WHERE (people.father_id = person.father_id
                         OR people.mother_id = person.mother_id)
                    AND people.person_id <> person.person_id
                  ORDER BY spurious ASC,
                           date_of_birth ASC;"""
        return self.get_all_records(sql, id)

    def get_full_siblings(self, id):
        """Returns a list of people who share both the given person's
           parents."""
        sql = """SELECT people.*
                   FROM people
                  CROSS JOIN (SELECT person_id, father_id, mother_id
                                FROM people
                               WHERE person_id = %s) AS person
                  WHERE COALESCE(people.father_id, -1) = COALESCE(person.father_id, -1)
                    AND COALESCE(people.mother_id, -1) = COALESCE(person.mother_id, -1)
                    AND people.person_id <> person.person_id
                    AND (person.father_id IS NOT NULL
                         OR person.mother_id IS NOT NULL)
                  ORDER BY spurious ASC,
                           date_of_birth ASC;"""
        return self.get_all_records(sql, id)

    def get_half_siblings(self, id):
        """Return a list of people who share exactly one of the given
           person's parents."""
        sql = """SELECT people.*
                   FROM people
                  CROSS JOIN (SELECT person_id, father_id, mother_id
                                FROM people
                               WHERE person_id = %s) AS person
                  WHERE (people.father_id = person.father_id
                         AND COALESCE(people.mother_id, -1) <> COALESCE(person.mother_id, -1))
                     OR (people.mother_id = person.mother_id
                         AND COALESCE(people.father_id, -1) <> COALESCE(person.father_id, -1))
                    AND people.person_id <> person.person_id
                  ORDER BY spurious ASC,
                           date_of_birth ASC;"""
        return self.get_all_records(sql, id)

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
        try:
            person = self.get_person(id)
        except SpuriousConnection:
            return None
        if person['father_id']:
            father = self.get_ancestors(person['father_id'])
            if father:
                person['father'] = father
        if person['mother_id']:
            mother = self.get_ancestors(person['mother_id'])
            if mother:
                person['mother'] = mother
        return person

    def get_ancestors_flat(self, id):
        """Return all ancestors of a given person as a flat list."""
        try:
            person = self.get_person(id)
        except SpuriousConnection:
            return []
        line = [person]
        if person['father_id']:
            line.extend(self.get_ancestors_flat(person['father_id']))
        if person['mother_id']:
            line.extend(self.get_ancestors_flat(person['mother_id']))
        return line

    def get_partners(self, id):
        """Get partners of any type for the given person."""
        sql = """SELECT *
                   FROM people AS o
                  INNER JOIN relationships AS r
                     ON o.person_id IN (r.person_a_id, r.person_b_id)
                  WHERE %s IN (r.person_a_id, r.person_b_id)
                    AND o.person_id <> %s
                  ORDER BY spurious ASC,
                           r.start_date ASC,
                           r.relationship_id ASC;"""
        return self.get_all_records(sql, (id, id))

if __name__ == '__main__':
    family = Family()
    family.add_all()
    print('\n'.join([f'{k}: {v}' for k, v in sorted(family.people.items())]))