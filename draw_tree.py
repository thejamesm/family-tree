from graphviz import Digraph

from family_tree import Family

def url_for(page, id):
    return str(id)

class Tree:
    def __init__(self, subject):
        self.invis_node = {'label': '',
                           '_attributes': {'shape': 'point',
                                           'peripheries': '0',
                                           'height': '0',
                                           'width': '0'}}
        self.invis_edge = {'_attributes': {'style': 'invis'}}
        self.married_edge = {}
        self.unmarried_edge = {'_attributes': {'style': 'dashed'}}

        self.graph = Digraph('neato',
                             format='svg',
                             filename=f'static/trees/{subject.id}',
                             graph_attr={'splines': 'ortho'},
                             node_attr={'shape': 'box',
                                        'style': 'filled'},
                             edge_attr={'dir': 'none'})

        layers = subject.get_layers()
        self.draw_layer(layers[0], first_layer=True)
        for layer in layers[1:]:
            self.draw_layer(layer)

    def node_fill(self, gender):
        match gender:
            case 'male':
                return 'lightblue'
            case 'female':
                return 'lightpink'
            case _:
                return 'gray'

    def draw_person_node(self, graph, person):
        graph.node(str(person.id), label=person.name,
                _attributes={'fillcolor': self.node_fill(person.gender),
                             'URL': url_for('person_page', id=person.id)})

    def draw_layer(self, layer, first_layer=False):
        if not first_layer:
            with self.graph.subgraph() as line_subgraph:
                for parents_id, group in [g for g in layer['groups'].items()
                                          if g[0]]:
                    people = list(group)
                    head_nodes = []
                    for person in people[1:-1]:
                        node_id = f'n{person.id}'
                        line_subgraph.node(node_id, **self.invis_node)
                        line_subgraph.edge(node_id, str(person.id))
                        head_nodes.append(node_id)
                    n_nodes = len(head_nodes)
                    if n_nodes % 2 == 0:
                        node_id = f'b{parents_id}'
                        head_nodes.insert(n_nodes // 2, node_id)
                        line_subgraph.node(node_id, **self.invis_node)
                    line_subgraph.edge(parents_id, head_nodes[n_nodes // 2])
                    for prev_id, cur in zip(head_nodes, head_nodes[1:]):
                        line_subgraph.edge(str(prev_id.id), str(cur.id))
                    line_subgraph.edge(head_nodes[0], str(people[0].id))
                    if len(people) > 1:
                        line_subgraph.edge(head_nodes[-1], str(people[-1].id))
        with self.graph.subgraph() as person_subgraph:
            person_subgraph.attr(rank='same')
            for people in layer['groups'].values():
                for person in people:
                    self.draw_person_node(person_subgraph, person)
            prev_id = None
            for couple_id, (left, right) in layer['edges'].items():
                if ((relationship := family.get_relationship(left, right)) and
                        relationship.type == 'marriage'):
                    join_style = self.married_edge
                else:
                    join_style = self.unmarried_edge
                person_subgraph.node(couple_id, **self.invis_node)
                person_subgraph.edge(str(left.id), couple_id, **join_style)
                person_subgraph.edge(couple_id, str(right.id), **join_style)
                if prev_id:
                    person_subgraph.edge(prev_id, str(left.id),
                                         **self.invis_edge)
                prev_id = str(right.id)


if __name__ == '__main__':
    family = Family()
    family.add_all()
    tree = Tree(family.people[1])
    tree.graph.view()