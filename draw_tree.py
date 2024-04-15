from graphviz import Digraph

from family_tree import Family, Person

def url_for(page, id):
    return str(id)

class TreeGraph(Digraph):
    MARRIED_EDGE = {}
    UNMARRIED_EDGE = {'style': 'dashed'}

    def node(self, id, label='', attributes={}, invis=False, **kwargs):
        if type(id) is Person:
            person = id
            id = person.id
            label = label or person.name
            attributes = attributes | {
                    'fillcolor': self.node_fill(person.gender),
                    'URL': url_for('person_page', id=person.id)
                }
        if invis:
            attributes = attributes | {'shape': 'point',
                                       'peripheries': '0',
                                       'height': '0',
                                       'width': '0'}
        id = str(id)
        attributes = {k: str(v) for k, v in attributes.items()}
        Digraph.node(self, id, label=label, _attributes=attributes, **kwargs)

    def edge(self, node_a, node_b, attributes={}, weight=1, invis=False,
             **kwargs):
        if invis:
            attributes = attributes | {'style': 'invis'}
        node_a = str(node_a)
        node_b = str(node_b)
        attributes = attributes | {'weight': weight}
        attributes = {k: str(v) for k, v in attributes.items()}
        Digraph.edge(self, node_a, node_b, _attributes=attributes, **kwargs)

    def node_fill(self, gender):
        match gender:
            case 'male':
                return 'lightblue'
            case 'female':
                return 'lightpink'
            case _:
                return 'gray'

class Tree:
    def __init__(self, subject):
        self.graph = TreeGraph('dot',
                               format='svg',
                               filename=f'static/trees/{subject.id}',
                               graph_attr={'splines': 'ortho'},
                               node_attr={'shape': 'box',
                                          'style': 'filled'},
                               edge_attr={'dir': 'none'})

        self.family = subject.family

        layers = subject.get_layers()
        self.draw_layer(layers[0], first_layer=True)
        for layer in layers[1:]:
            self.draw_layer(layer)
        self.graph.render()

    def draw_layer(self, layer, first_layer=False):
        if not first_layer:
            with self.graph.subgraph() as line_subgraph:
                for parents_id, group in [g for g in layer['groups'].items()
                                          if g[0]]:
                    people = list(group)
                    if len(people) == 1:
                        line_subgraph.edge(parents_id, people[0].id)
                    else:
                        head_nodes = []
                        for person in people[1:-1]:
                            node_id = f'n{person.id}'
                            line_subgraph.node(node_id, invis=True)
                            line_subgraph.edge(node_id, person.id)
                            head_nodes.append(node_id)
                        n_nodes = len(head_nodes)
                        if n_nodes % 2 == 0:
                            node_id = f'b{parents_id}'
                            head_nodes.insert(n_nodes // 2, node_id)
                            line_subgraph.node(node_id, invis=True)
                        line_subgraph.edge(parents_id, head_nodes[n_nodes // 2])
                        for prev_id, cur in zip(head_nodes, head_nodes[1:]):
                            line_subgraph.edge(prev_id, cur)
                        line_subgraph.edge(head_nodes[0], people[0].id)
                        line_subgraph.edge(head_nodes[-1], people[-1].id)
        with self.graph.subgraph() as person_subgraph:
            person_subgraph.attr(rank='same')
            for people in layer['groups'].values():
                for person in people:
                    person_subgraph.node(person)
            prev_id = None
            for couple_id, (left, right) in layer['edges'].items():
                if ((relationship := self.family.get_relationship(left, right))
                        and relationship.type == 'marriage'):
                    join_style = TreeGraph.MARRIED_EDGE
                else:
                    join_style = TreeGraph.UNMARRIED_EDGE
                person_subgraph.node(couple_id, invis=True)
                person_subgraph.edge(left.id, couple_id, join_style)
                person_subgraph.edge(couple_id, right.id, join_style)
                if prev_id:
                    person_subgraph.edge(prev_id, left.id, invis=True)
                prev_id = right.id


if __name__ == '__main__':
    family = Family()
    family.add_all()
    tree = Tree(family.people[5])
    tree.graph.view()