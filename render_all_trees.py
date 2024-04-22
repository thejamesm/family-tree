from family_tree import Family
from draw_tree import Tree

family = Family(True)

count = len(family.people)
progress = 0

for id in family.people:
    person = family.person(id)
    print(f'{progress*100/count:.2f}%', person)
    Tree(person, calculate_kinship=True)
    progress += 1