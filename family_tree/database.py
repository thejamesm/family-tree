import psycopg2
from psycopg2.extensions import connection


from collections import defaultdict
from typing import Generator, Iterable, cast


from .config import load_config


field_types = str | int | float | bool | list | None
type RecordField = field_types
type RecordLine = dict[str, RecordField | RecordLine | list[RecordLine]]



class SpuriousConnection(Exception):

    pass



class Database:

    MPHONE_LEN: int = 15

    db_config: dict[str, str]
    app_config: dict[str, str]

    exclude_spurious: bool
    where_condition: str
    and_condition: str


    def __init__(
        self
    ) -> None:

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


    @staticmethod
    def sanitize_field(
        value: any
    ) -> RecordField:
        """Return the field in a format suitable for JSON export."""

        if not isinstance(value, field_types):
            return str(value)

        return value


    @staticmethod
    def tuple_from(
        iter: Iterable[str]
    ) -> tuple[str, ...]:
        """Convert an iterable of strings to a tuple of strings"""

        if isinstance(iter, str) or not isinstance(iter, Iterable):
            output = (iter,)
        else:
            output = tuple(iter)

        return output


    def get_all_records(
        self,
        sql: str,
        params: Iterable[str] = ()
    ) -> list[dict[str, RecordField]]:
        """Return the entire result of the supplied SQL query as a list."""

        params_tuple = Database.tuple_from(params)

        try:
            with psycopg2.connect(**self.db_config) as conn:
                conn = cast(connection, conn)
                with conn.cursor() as cur:
                    cur.execute(sql, params_tuple)
                    col_names: list[str] = [col.name for col in cur.description]
                    rows = [{col_names[i]: Database.sanitize_field(field)
                            for i, field in enumerate(row)}
                            for row in cur.fetchall()]

            return rows

        except (psycopg2.DatabaseError, Exception) as e:
            print(e)


    def record_generator(
        self,
        sql: str,
        params: Iterable[str] = (),
        size: int = 100
    ) -> Generator[dict[str, RecordField], None, None]:
        """Create an optionally batched generator from the supplied SQL."""

        params_tuple = Database.tuple_from(params)

        try:
            with psycopg2.connect(**self.db_config) as conn:
                conn = cast(connection, conn)
                with conn.cursor() as cur:
                    cur.execute(sql, params_tuple)
                    col_names: list[str] = [col.name for col in cur.description]
                    rows = cur.fetchmany(size)
                    while rows is not None and len(rows):
                        for row in rows:
                            yield {col_names[i]: Database.sanitize_field(field)
                                for i, field in enumerate(row)}
                        rows = cur.fetchmany(size)

        except Exception as e:
            print(e)


    def get_ids(
        self
    ) -> tuple[int, ...]:
        """Return a list of all active IDs in the `people` table."""

        sql = f"""SELECT person_id
                    FROM people
                         {self.where_condition}
                   ORDER BY person_id ASC;"""

        return tuple(x['person_id'] for x in self.get_all_records(sql))


    def get_people(
        self,
        match: str | None = None
    ) -> list[dict[str, RecordField]]:
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
                        LIKE '%%' || METAPHONE(%s, {self.MPHONE_LEN}) || '%%'
                             {self.and_condition}
                       ORDER BY spurious ASC,
                                SIMILARITY(%s, person_name) DESC;"""

            results = self.get_all_records(sql, (match, match))

        if not results:
            return []

        return results


    def get_person(
        self,
        match: int | str
    ) -> dict[str, RecordField]:
        """For a given ID or name, return a single matching person."""

        if (isinstance(match, int) or
                (isinstance(match, str) and match.isnumeric())):

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


    def get_children(
        self,
        id: int
    ) -> list[dict[str, RecordField]]:
        """Return a list of the children of a given person."""

        sql = """SELECT *
                   FROM people
                  WHERE father_id = %s
                     OR mother_id = %s
                  ORDER BY spurious ASC,
                           date_of_birth ASC,
                           person_id ASC;"""

        return self.get_all_records(sql, (id, id))


    def get_child_ids(
        self,
        id: int
    ) -> tuple[int, ...]:
        """Return a list of the IDs of the children of a specified person."""

        sql = """SELECT person_id
                   FROM people
                  WHERE %s IN (father_id, mother_id)
                  ORDER BY spurious ASC,
                           date_of_birth ASC,
                           person_id ASC;"""

        if not (records := self.get_all_records(sql, id)):
            return tuple()

        return tuple(x['person_id'] for x in records)


    def get_parent_child_id_pairs(
        self
    ) -> dict[int, list[int]]:
        """Return all pairs of parent and child ID numbers."""

        sql = """(SELECT father_id AS parent_id,
                         person_id AS child_id
                    FROM people
                   WHERE father_id IS NOT NULL
                   ORDER BY father_id ASC,
                            date_of_birth ASC,
                            person_id ASC)
                 UNION ALL
                 (SELECT mother_id AS parent_id,
                         person_id AS child_id
                    FROM people
                   WHERE mother_id IS NOT NULL
                   ORDER BY mother_id ASC,
                            date_of_birth ASC,
                            person_id ASC);"""

        records = self.get_all_records(sql)
        output: defaultdict[int, list[int]] = defaultdict(list)

        for record in records:
            output[record['parent_id']].append(record['child_id'])

        return dict(output)


    def get_siblings(
        self,
        id: int
    ) -> list[dict[str, RecordField]]:
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
                           date_of_birth ASC,
                           person_id ASC;"""

        return self.get_all_records(sql, id)


    def get_full_siblings(
        self,
        id: int
    ) -> list[dict[str, RecordField]]:
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
                           date_of_birth ASC,
                           person_id ASC;"""

        return self.get_all_records(sql, id)


    def get_half_siblings(
        self,
        id: int
    ) -> list[dict[str, RecordField]]:
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
                           date_of_birth ASC,
                           person_id ASC;"""

        return self.get_all_records(sql, id)


    def get_line(
        self,
        id: int
    ) -> RecordLine:
        """Return all ancestors and descendants of a given person."""

        person: RecordLine = self.get_person(id)

        if person['father_id']:
            person['father'] = self.get_ancestors(person['father_id'])

        if person['mother_id']:
            person['mother'] = self.get_ancestors(person['mother_id'])

        person['children'] = []
        children = self.get_children(id)

        for child in children:
            person['children'].append(self.get_descendants(child['person_id']))

        return person


    def get_descendants(
        self,
        id: int
    ) -> RecordLine:
        """Return all descendants of a given person
           nested within their parents."""

        person: RecordLine = self.get_person(id)
        person['children'] = []
        children = self.get_children(id)

        for child in children:
            person['children'].append(self.get_descendants(child['person_id']))

        return person


    def get_descendants_flat(
        self,
        id: int
    ) -> list[dict[str, RecordField]]:
        """Return all descendants of a given person as a flat list."""

        children = self.get_children(id)
        line = children

        for child in children:
            line.extend(self.get_descendants_flat(child['person_id']))

        return line


    def get_ancestors(
        self,
        id: int
    ) -> RecordLine:
        """Return all ancestors of a given person
           nested within their children."""

        try:
            person: RecordLine = self.get_person(id)

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


    def get_ancestors_flat(
        self,
        id: int
    ) -> list[dict[str, RecordField]]:
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


    def get_partners(
        self,
        id: int
    ) -> list[dict[str, RecordField]]:
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


    def get_relationship(
        self,
        person_a: int,
        person_b: int
    ) -> dict[str, RecordField]:
        """Get the relationship record for two given people."""

        sql = """SELECT *
                   FROM relationships
                  WHERE person_a_id = %s
                    AND person_b_id = %s
                  ORDER BY relationship_id;"""

        result = self.get_all_records(sql, sorted(person_a, person_b))[0]

        if not result:
            raise ValueError('Relationship not found.')

        return result


    def get_relationships(
        self
    ) -> list[dict[str, RecordField]]:
        """Get all relationships."""

        if self.exclude_spurious:

            sql = """SELECT *
                       FROM relationships AS r
                       JOIN people AS a
                         ON a.person_id = r.person_a_id
                       JOIN people AS b
                         ON b.person_id = r.person_b_id
                      WHERE a.spurious = 'FALSE'
                        AND b.spurious = 'FALSE'
                      ORDER BY r.relationship_id;"""

        else:

            sql = """SELECT *
                    FROM relationships
                    ORDER BY relationship_id;"""

        return self.get_all_records(sql)