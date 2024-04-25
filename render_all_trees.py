from datetime import datetime

from family_tree import Family
from draw_tree import Tree

family = Family(True)

count = len(family.people)
progress = 0

for id in family.people:
    person = family.person(id)
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    percentage = f'{progress*100/count:.2f}%'
    print(f'{timestamp} \t{percentage} \t{person.id} \t{person}')
    Tree(person, calculate_kinship=True)
    progress += 1