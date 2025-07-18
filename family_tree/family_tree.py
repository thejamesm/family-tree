from __future__ import annotations

import json
import os
from datetime import date
from dateutil.relativedelta import relativedelta
from functools import cached_property, lru_cache
from itertools import chain
from collections import defaultdict
from uuid import uuid4
from typing import Literal, TypedDict


import inflect

from .config import Config, ReadOnlyDict
from .database import Database, RecordField, SpuriousConnection


config: ReadOnlyDict[str, str] = Config['family_tree']

type Gender = Literal['male', 'female'] | None
type PartnerDesc = Literal['husband', 'wife', 'spouse', 'partner',
                           'ex-husband', 'ex-wife', 'ex-spouse', 'ex-partner']
type PersonLine = dict[str, Person | PersonLine | list[PersonLine]]

class Layer(TypedDict):
    people: list[Person]
    groups: defaultdict[int, list[Person]]
    edges: dict[int, tuple[Person, Person]]

class PersonJSONBase(TypedDict):
    id: int
    name: str
    gender: Gender
    date_of_birth: str | None
    date_of_death: str | None
    place_of_birth: str | None
    place_of_death: str | None
    father: str | None
    mother: str | None

class ChildrenJSON(TypedDict):
    child_id: int
    child: str

class PersonJSON(PersonJSONBase):
    children: list[ChildrenJSON]

class PersonJSONChildNames(PersonJSONBase):
    children: list[str]

class PersonJSONChildIds(PersonJSONBase):
    child_ids: list[int]



class Family:

    people: dict[int, Person]
    child_ids: dict[int, list[int]]
    relationships: dict[tuple[int, int], Relationship]
    db: Database
    inflect_engine: inflect.engine


    def __init__(
        self,
        add_all: bool = False
    ) -> None:

        self.people = {}
        self.child_ids = {}
        self.relationships = {}
        self.db = Database()
        self.inflect_engine = inflect.engine()

        if add_all:
            self.add_all()


    def person(
        self,
        person: Person | int
    ) -> Person:

        if isinstance(person, Person):
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


    def add_all(
        self
    ) -> None:

        for record in self.db.get_people():
            Person(record=record, family=self)

        self.people = dict(sorted(self.people.items()))
        self.child_ids = self.db.get_parent_child_id_pairs()
        self.relationships = {(r['person_a_id'], r['person_b_id']):
                              Relationship(family=self, record=r)
                              for r in self.db.get_relationships()}


    def search(
        self,
        search_string: str
    ) -> list[Person]:

        return [p for p in self.people.values()
                if search_string.lower() in p.name.lower()]


    def save(
        self
    ) -> None:

        with open(r'family_tree.json', mode='w', encoding='utf-8') as f:
            json.dump(list(self.people.values()), f, cls=PersonEncoder,
                      indent=4, ensure_ascii=False)


    def get_child_ids(
        self,
        person: Person | int
    ) -> list[int]:

        if isinstance(person, Person):
            person = person.id

        if person in self.child_ids:
            return self.child_ids[person]
        else:
            return []


    def get_relationship(
        self,
        person_a: Person | int,
        person_b: Person | int
    ) -> Relationship:

        if isinstance(person_a, Person):
            person_a = person_a.id

        if isinstance(person_b, Person):
            person_b = person_b.id

        try:
            return self.relationships[(person_a, person_b)]
        except KeyError:
            return None


    def add_relationship(
        self,
        person_a: Person | int,
        person_b: Person | int
    ) -> Relationship:

        rel = Relationship(person_a, person_b, family=self, blank_record=True)
        self.relationships[tuple(p.id for p in rel.people)] = rel

        return rel


    def relationship(
        self,
        person_a: Person | int,
        person_b: Person | int
    ) -> Relationship:

        if not (relationship := self.get_relationship(person_a, person_b)):
            relationship = self.add_relationship(person_a, person_b)

        return relationship


    @lru_cache(maxsize=128)
    def get_parents_id(
        self,
        father: Person | int,
        mother: Person | int
    ) -> str | None:

        if father and mother:
            if ((relationship := self.get_relationship(father, mother)) or
                    (relationship := self.get_relationship(mother, father))):
                return f'r{relationship.id}'
            else:
                return f'{father.id}_{mother.id}'
        elif father:
            return f'{father.id}_x'
        elif mother:
            return f'x_{mother.id}'

        return None


    def get_longest_line(
        self
    ) -> list[Person]:

        return max([p.get_longest_line() for p in self.people.values()],
                   key=len)


    def kinship(
        self,
        person_a: Person | int,
        person_b: Person | int
    ) -> tuple[Person, int, int] | None:


        def search_nested(
            needle: Person,
            haystack: list[tuple[Person, ...]]
        ) -> int | None:

            gen = (x[0] for x in enumerate(haystack) if needle in x[1])
            return next(gen, None)


        def all_parents(
            layer: list[tuple[Person, ...]]
        ) -> tuple[Person, ...]:

            return tuple(parent for x in layer
                         for parent in (x.father, x.mother)
                         if parent is not None)


        def calculate_kinship(
            common_ancestor: Person,
            a_depth: int,
            b_depth: int
        ) -> tuple[Person, int, int]:

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

    family: Family | None
    id: int
    name: str
    gender: Gender
    dob: date | None
    dob_prec: int | None
    date_of_birth: str | None
    year_of_birth: int | None
    place_of_birth: str | None
    dod: date | None
    dod_prec: int | None
    dod_unknown: bool
    date_of_death: str | None
    year_of_death: int | str | None
    place_of_death: str | None
    dead: bool
    occupation: str | None
    notes: str | None
    spurious: bool

    _father_id: int | None
    _mother_id: int | None


    @classmethod
    def search(
        cls,
        search_string: str,
        family: Family = None
    ) -> list[Person]:

        return [Person(record=record, family=family)
                for record in Database().get_people(search_string)]


    @classmethod
    def sorted_ids(
        cls,
        *people: Person
    ) -> tuple[int, ...]:

        ids: list[int] = []

        for index, person in enumerate(people):
            if isinstance(person, Person):
                ids[index] = person.id

        return tuple(sorted(ids))


    @classmethod
    def _add_edge(
        cls,
        layer: Layer,
        id: int,
        father: Person | None,
        mother: Person | None
    ) -> bool:

        edges = layer['edges']
        people = layer['people']
        lefts = [p[0] for p in edges.values()]
        rights = [p[1] for p in edges.values()]

        if ((father, mother) in edges.values() or
                (mother, father) in edges.values()):
            return True

        if ((father in lefts and father in rights) or
                (mother in rights and mother in lefts)):
            return False

        if father in lefts:
            prev_index = list(edges.keys())[lefts.index(father)]
            prev_mother = edges[prev_index][1]
            edges[prev_index] = (prev_mother, father)
            f_index = people.index(father)
            m_index = people.index(prev_mother)
            people[f_index], people[m_index] = people[m_index], people[f_index]

        if mother in rights:
            left = mother
            right = father
        else:
            left = father
            right = mother

        if (right in people and left not in people):
            people.insert(people.index(right), left)

        if (left in people and right not in people):
            people.insert(people.index(left)+1, right)

        edges[id] = (left, right)

        return True


    _pattern_parts: tuple[str, str, str] = (
            ('%#d', '%B', '%Y') if os.name == 'nt'
            else ('%-d', '%B', '%Y')
        )


    def __init__(
        self,
        id: int | None = None,
        *,
        record: dict[str, RecordField] | None = None,
        family: Family | None = None
    ) -> None:

        self.family = family

        if record is None:
            if family:
                record = family.db.get_person(id)
            else:
                record = Database().get_person(id)

        self.id = record['person_id']
        self.name = record['person_name']
        self.gender = record['gender']

        _dob: str | None = record['date_of_birth']
        self.dob_prec = record['date_of_birth_precision']

        if _dob:
            self.dob = date.fromisoformat(_dob)
            dob_pattern = ' '.join(Person._pattern_parts[3 - self.dob_prec:])
            self.date_of_birth = date.strftime(self.dob, dob_pattern)
            if self.dob_prec > 0:
                self.year_of_birth = self.dob.year
            else:
                self.year_of_birth = None
        else:
            self.dob = None
            self.date_of_birth = None
            self.year_of_birth = None

        self.place_of_birth = record['place_of_birth']

        _dod: str | None = record['date_of_death']
        self.dod_prec = record['date_of_death_precision']
        self.dod_unknown = record['date_of_death_unknown']
        self.dead = _dod or self.dod_unknown

        if _dod:
            self.dod = date.fromisoformat(_dod)
            dod_pattern = ' '.join(Person._pattern_parts[3 - self.dod_prec:])
            self.date_of_death = date.strftime(self.dod, dod_pattern)
            if self.dod_prec > 0:
                self.year_of_death = self.dod.year
            else:
                self.dod_unknown = True
                self.year_of_death = '?'
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


    def __repr__(
        self
    ) -> str:

        years = self.years

        if years:
            years = f' ({years})'
        else:
            years = ''

        return self.name + years


    def __lt__(
        self,
        other
    ) -> bool:

        self_dob = self.dob or date.max
        other_dob = other.dob or date.max

        return self_dob < other_dob


    @cached_property
    def dates(
        self
    ) -> str | None:

        if self.date_of_birth and self.date_of_death:
            return f'{self.date_of_birth} – {self.date_of_death}'
        elif self.date_of_birth:
            return f'b. {self.date_of_birth}'
        elif self.date_of_death:
            return f'd. {self.date_of_death}'
        else:
            return None


    @cached_property
    def years(
        self
    ) -> str | None:

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
    def born(
        self
    ) -> str:

        return ' in '.join(filter(None,
                                  (self.date_of_birth, self.place_of_birth)))


    @cached_property
    def died(
        self
    ) -> str:

        return ' in '.join(filter(None,
                                  (self.date_of_death, self.place_of_death)))


    @cached_property
    def age(
        self
    ) -> str | None:

        if (not self.dob) or self.dod_unknown:
            return None

        approx_prefix = ''
        start = self.dob
        start_prec = self.dob_prec

        if self.dod:
            end = self.dod
            end_prec = self.dod_prec
        else:
            end = date.today()
            end_prec = 3

        if min(start_prec, end_prec) < 1:
            return None

        age_delta = relativedelta(end, start)
        min_prec = min(start_prec, end_prec)

        if min_prec == 1:
            return f'approx. {age_delta.years}'

        if (min_prec == 2) and (age_delta.months == 0):
            approx_prefix = 'approx. '

        if (age_delta.years < 1) and (start_prec > 1):
            if age_delta.months > 0:
                age = age_delta.months
                unit = 'month'
            elif (age_delta.weeks > 0) and (start_prec > 2):
                age = age_delta.weeks
                unit = 'week'
            elif start_prec > 2:
                age = age_delta.days
                unit = 'day'
            if age != 1:
                unit += 's'
            return f'{approx_prefix}{age} {unit}'

        return approx_prefix + str(age_delta.years)


    @cached_property
    def father(
        self
    ) -> Person | None:

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
    def mother(
        self
    ) -> Person | None:

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
    def parents(
        self
    ) -> tuple[Person, ...]:

        parents: list[Person] = []

        if self.father:
            parents.append(self.father)

        if self.mother:
            parents.append(self.mother)

        return tuple(parents)


    @cached_property
    def parents_id(
        self
    ) -> str | None:

        if self.family:
            return self.family.get_parents_id(self.father, self.mother)
        else:
            return Family().get_parents_id(self.father, self.mother)


    @cached_property
    def children(
        self
    ) -> list[Person]:

        try:
            if self.family and self.family.child_ids:
                child_ids = self.family.child_ids[self.id]
            else:
                child_ids = Database().get_child_ids(self.id)

            if self.family:
                children = [self.family.person(child_id)
                            for child_id in child_ids]
            else:
                children = [Person(child_id) for child_id in child_ids]

            return sorted(children)

        except KeyError:
            return []


    @cached_property
    def siblings(
        self
    ) -> list[Person]:

        if not (family := self.family):
            family = Family()

        records = family.db.get_siblings(self.id)

        return [Person(record=record, family=family) for record in records]


    @cached_property
    def full_siblings(
        self
    ) -> list[Person]:

        if not (family := self.family):
            family = Family()

        records = family.db.get_full_siblings(self.id)

        return [Person(record=record, family=family) for record in records]


    @cached_property
    def half_siblings(
        self
    ) -> list[Person]:

        if not (family := self.family):
            family = Family()

        records = family.db.get_half_siblings(self.id)

        return [Person(record=record, family=family) for record in records]


    @cached_property
    def siblings_and_self(
        self
    ) -> list[Person]:

        siblings = self.siblings
        siblings.append(self)

        return sorted(siblings)


    @cached_property
    def relationships(
        self
    ) -> list[Relationship]:

        if not (family := self.family):
            family = Family()

        records = family.db.get_partners(self.id)
        output: list[Relationship] = []

        for record in records:
            partner = family.person(record['person_id'])
            relationship = (family.get_relationship(self, partner) or
                            Relationship(self, partner, record=record))
            output.append(relationship)

        return output


    def json(
        self
    ) -> PersonJSONChildNames:

        tree: PersonJSONChildNames = {
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


    def json_flat(
        self
    ) -> PersonJSONChildIds:

        tree: PersonJSONChildIds = {
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


    def get_ancestors(
        self
    ) -> PersonLine:

        tree: PersonLine = {'person': self}

        if self.father:
            tree['father'] = self.father.get_ancestors()

        if self.mother:
            tree['mother'] = self.mother.get_ancestors()

        return tree


    def get_descendants(
        self
    ) -> PersonLine:

        tree = {'person': self}

        if self.children:
            tree['children'] = [child.get_descendants()
                                for child in self.children]

        return tree


    def get_line(
        self
    ) -> PersonLine:

        tree = self.get_ancestors()

        if self.children:
            tree['children'] = self.get_descendants()['children']

        return tree


    def get_longest_ancestor_line(
        self
    ) -> list[Person]:

        father_line = (self.father.get_longest_ancestor_line()
                       if self.father else [])

        mother_line = (self.mother.get_longest_ancestor_line()
                       if self.mother else [])

        return max(father_line, mother_line, key=len) + [self]


    def get_longest_descendant_line(
        self
    ) -> list[Person]:

        return [self] + max([child.get_longest_descendant_line()
                             for child in self.children],
                            default=[], key=len)


    def get_longest_line(
        self
    ) -> list[Person]:

        return (self.get_longest_ancestor_line() +
                self.get_longest_descendant_line()[1:])


    def get_ancestor_layers(
        self,
        level: int = 0,
        layers: list[Layer] | None = None
    ) -> list[Layer]:

        if layers is None:
            layers = []

        if level >= len(layers):
            layers.append({
                    'people': [],
                    'groups': defaultdict(list),
                    'edges': {}
                })

        parents = [None, None]

        if self.father:
            parents[0] = self.father
            self.father.get_ancestor_layers(level=level+1, layers=layers)

        if self.mother:
            parents[1] = self.mother
            self.mother.get_ancestor_layers(level=level+1, layers=layers)

        for parent in [p for p in parents
                       if p and p not in layers[level]['groups'][p.parents_id]]:
                layers[level]['people'].append(parent)
                layers[level]['groups'][parent.parents_id].append(parent)

        if all(parents):
            Person._add_edge(layers[level], self.parents_id,
                             self.father, self.mother)

        if level == 0:
            return layers[-2::-1]   # Exclude empty final layer and reverse


    def get_descendant_layers(
        self,
        level: int = 0,
        layers: list[Layer] | None = None,
        include_partners: bool = False,
        include_siblings: bool = False,
        ancestor_layers: list[Layer] | None = None
    ) -> list[Layer]:

        people: list[Person]

        if layers is None:
            layers = [{
                    'people': [],
                    'groups': defaultdict(list),
                    'edges': {}
                }]

            if include_siblings:
                people = self.siblings_and_self
            else:
                people = [self]

            for person in people:

                layers[0]['people'].append(person)
                layers[0]['groups'][person.parents_id].append(person)

                if len(ancestor_layers):

                    if person.father not in ancestor_layers[-1]['people']:
                        ancestor_layers[-1]['people'].append(person.father)
                        ancestor_layers[-1]['groups'] \
                            [person.father.parents_id].append(person.father)

                    if person.mother not in ancestor_layers[-1]['people']:
                        ancestor_layers[-1]['people'].append(person.mother)
                        ancestor_layers[-1]['groups'] \
                            [person.mother.parents_id].append(person.mother)

                    if person.father and person.mother:
                        Person._add_edge(ancestor_layers[-1], person.parents_id,
                                        person.father, person.mother)

                if include_partners:

                    for relationship in person.relationships:
                        person_a = person
                        person_b = relationship.partner

                        if (person_a.gender == 'female' and
                                person_b.gender == 'male'):
                            person_a, person_b = person_b, person_a

                        Person._add_edge(layers[0], f'r{relationship.id}',
                                         person_a, person_b)

            level = 1
            outer_layer = True

        else:
            people = [self]
            outer_layer = False

        for person in people:

            if person.children:

                if level >= len(layers):
                    layers.append({
                            'people': [],
                            'groups': defaultdict(list),
                            'edges': {}
                        })

                prev_layer = layers[level-1]

                for child in [
                            c for c in person.children
                            if c not in layers[level]['groups'][c.parents_id]
                        ]:

                    layers[level]['people'].append(child)
                    layers[level]['groups'][child.parents_id].append(child)

                    for parent in child.parents:

                        if parent not in prev_layer['people']:
                            prev_layer['people'].append(parent)

                        if parent not in chain(*prev_layer['groups'].values()):
                            prev_layer['groups'][parent.parents_id] \
                                .append(parent)
                            Person._add_edge(prev_layer, child.parents_id,
                                             child.father, child.mother)

                    child.get_descendant_layers(level=level+1, layers=layers)

        if outer_layer:
            return layers


    def get_layers(
        self,
        include_partners: bool = False,
        include_siblings: bool = False
    ) -> list[Layer]:

        ancestor_layers = self.get_ancestor_layers()
        descendant_layers = self.get_descendant_layers(
                                    include_partners=include_partners,
                                    include_siblings=include_siblings,
                                    ancestor_layers=ancestor_layers
                                )

        return ancestor_layers + descendant_layers


    def kinship_term(
        self,
        person: Person
    ) -> str:


        def calc_term(
            short: int,
            diff: int,
            gender: Gender
        ) -> str | None:

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


        def calc_spousal_term(
            short: int,
            diff: int,
            gender: Gender
        ) -> str | None:

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


        def calc_affine_term(
            short: int,
            diff: int,
            gender: Gender
        ) -> str | None:

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


        def calc_extended_term(
            short: int,
            diff: int,
            gender: Gender
        ) -> str:

            p = self.family.inflect_engine

            if short in (0, 1):
                levels = abs(diff) - 2
                if levels <= int(config['max_great_levels']):
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
        term = None
        suffix = ''

        if not (family := self.family):
            family = Family()

        kinship = family.kinship(self, person)

        if kinship:

            _, short, diff = kinship
            term = calc_term(short, diff, person.gender)

        else:

            for rel in self.relationships:

                partner = rel.partner
                kinship = family.kinship(partner, person)

                if kinship:

                    _, short, diff = kinship
                    term = None

                    if rel.type == 'marriage':
                        term = calc_spousal_term(short, diff, person.gender)

                    if term:
                        term = rel.ex_prefix + term
                    else:
                        term = calc_term(short, diff, person.gender)
                        if rel.type == 'marriage':
                            match partner.gender:
                                case 'male':
                                    prefix = f'{rel.ex_prefix}husband’s '
                                case 'female':
                                    prefix = f'{rel.ex_prefix}wife’s '
                                case _:
                                    prefix = f'{rel.ex_prefix}spouse’s '
                        else:
                            prefix = f'{rel.ex_prefix}partner’s '

                    break

            if not kinship:

                for rel in person.relationships:

                    partner = rel.partner
                    kinship = family.kinship(self, partner)

                    if kinship:

                        _, short, diff = kinship
                        term = None

                        if rel.type == 'marriage':
                            term = calc_affine_term(short, diff, person.gender)

                        if term:
                            term = rel.ex_prefix + term
                        else:
                            term = calc_term(short, diff, partner.gender)
                            if rel.type == 'marriage':
                                match person.gender:
                                    case 'male':
                                        suffix = f'’s {rel.ex_prefix}husband'
                                    case 'female':
                                        suffix = f'’s {rel.ex_prefix}wife'
                                    case _:
                                        suffix = f'’s {rel.ex_prefix}spouse'
                            else:
                                suffix = f'’s {rel.ex_prefix}partner'
                            break

            if not kinship:
                return 'no blood relation'

        if not term:
            term = calc_extended_term(short, diff, person.gender)

        output = prefix + term + suffix
        output = output.replace('self’s ', '')
        output = output.replace('’s self', '')

        return output



class PersonEncoder(json.JSONEncoder):


    def default(
        self,
        obj
    ) -> PersonJSON | any:

        if isinstance(obj, Person):

            person: PersonJSON = {
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

    id: int
    type: Literal['marriage', 'couple']
    people: tuple[Person, Person]
    partner: Person
    _start_date: date | None
    start_date: str | None
    start_year: int | None
    start_place: str | None
    _end_date: date | None
    end_date: str | None
    end_year: int | None
    end_type: Literal['marriage', 'divorce', 'separation', 'death'] | None


    def __init__(
        self,
        subject: Person | int | None = None,
        partner: Person | int | None = None,
        family: Family | None = None,
        record: dict[str, RecordField] | None = None,
        blank_record: bool = False
    ) -> None:

        if not ((subject and partner) or record):
            raise ValueError('Insufficient data to describe relationship.')

        if not (subject and partner):

            if family:
                subject = family.people[record['person_a_id']]
                partner = family.people[record['person_b_id']]
            else:
                subject = Person(record['person_a_id'])
                partner = Person(record['person_b_id'])

        else:

            if isinstance(subject, int):
                if family:
                    subject = family.people[subject]
                else:
                    subject = Person(subject)

            if isinstance(partner, int):
                if family:
                    partner = family.people[partner]
                else:
                    partner = Person(partner)

        if blank_record:
            record = {
                    'relationship_id': str(uuid4()),
                    'relationship_type': 'marriage',
                    'start_date': None,
                    'start_date_precision': 3,
                    'place': None,
                    'end_date': None,
                    'end_date_precision': 3,
                    'end_type': None
                }
        elif not record:
            if family:
                record = family.db.get_relationship(subject, partner)
            else:
                record = Database().get_relationship(subject, partner)

        self.id = record['relationship_id']
        self.type = record['relationship_type']
        self.people = (subject, partner)
        self.partner = partner

        start_date: str | None = record['start_date']
        start_prec: int = record['start_date_precision']

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
    def partner_description(
        self
    ) -> PartnerDesc:

        match self.type, self.partner.gender:
            case 'marriage', 'male':
                rel_type = 'husband'
            case 'marriage', 'female':
                rel_type = 'wife'
            case 'marriage', _:
                rel_type = 'spouse'
            case 'couple', _:
                rel_type = 'partner'

        return self.ex_prefix + rel_type


    @cached_property
    def dates(
        self
    ) -> str | None:

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
    def years(
        self
    ) -> str | None:

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
    def started(
        self
    ) -> str | None:

        if place := self.start_place:
            place = ' in ' + place

        return ((self.start_date or '') + (place or '')).strip() or None


    @cached_property
    def ended(
        self
    ) -> str | None:

        return ', '.join(filter(None, (self.end_date, self.end_type))) or None


    @cached_property
    def is_ex(
        self
    ) -> bool:

        return self.end_type in ('divorce', 'separation')


    @cached_property
    def ex_prefix(
        self
    ) -> Literal['ex-', '']:

        return 'ex-' if self.is_ex else ''


    @cached_property
    def description(
        self
    ) -> str | None:

        return ', '.join(filter(None, (self.started, self.ended))) or None


    @cached_property
    def children(
        self
    ) -> list[Person]:

        person_a, person_b = self.people
        family = person_a.family or person_b.family

        if family and family.child_ids:
            child_ids_a = family.get_child_ids(person_a.id)
            child_ids_b = family.get_child_ids(person_b.id)
        else:
            child_ids_a = Database().get_child_ids(person_a.id)
            child_ids_b = Database().get_child_ids(person_b.id)

        child_ids = list(set(child_ids_a) & set(child_ids_b))

        if family:
            children = [family.person(child_id)
                        for child_id in child_ids]
        else:
            children = [Person(child_id) for child_id in child_ids]

        return sorted(children)


    def end_type_description(
        self,
        noun: bool = True,
        until: bool = False
    ) -> str | None:


        def death_description() -> str:

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



if __name__ == '__main__':
    family = Family(True)
    print('\n'.join([f'{k}: {v}' for k, v in sorted(family.people.items())]))