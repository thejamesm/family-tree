from graphviz import Digraph

from family_tree import Family, Person

web_app = None

def person_url(id):
    if web_app:
        return web_app.url_for('person_page', id=id)
    else:
        return f'/{id}'

class TreeGraph(Digraph):
    MARRIED_EDGE = {}
    UNMARRIED_EDGE = {'style': 'dashed'}

    def node(self, id, label='', attributes={}, invis=False,
             kinship_subject=None, is_subject=False, **kwargs):
        if type(id) is Person:
            person = id
            id = person.id
            label = label or person.name
            attributes = attributes | {
                    'class': person.gender,
                    'URL': person_url(person.id)
                }
            if kinship_subject:
                attributes = attributes | {
                        'tooltip': kinship_subject.kinship_term(person)
                    }
            if is_subject:
                attributes = attributes | {
                        'id': 'subject'
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
    def __init__(self, subject, calculate_kinship=False, app=None):
        if app:
            global web_app
            web_app = app
            css = web_app.url_for('static', filename='diagram.css')
        else:
            css = '/static/diagram.css'

        self.graph = TreeGraph('dot',
                               format='svg',
                               filename=f'static/trees/{subject.id}',
                               graph_attr={'splines': 'ortho',
                                           'concentrate': 'true',
                                           'stylesheet': css,
                                           'bgcolor': 'transparent',
                                           'tooltip': ' '},
                               node_attr={'shape': 'box',
                                          'style': 'filled',
                                          'fontname': 'Merriweather',
                                          'fontsize': '13.0',
                                          'target': '_top',
                                          'tooltip': ' '},
                               edge_attr={'dir': 'none',
                                          'tooltip': ' '})

        self.subject = subject
        self.family = subject.family

        if calculate_kinship:
            self.kinship_subject = subject
        else:
            self.kinship_subject = None

        self.layers = subject.get_layers()
        for layer_number in range(len(self.layers)):
            self.draw_layer(layer_number)

        self.graph.render()

    def draw_layer(self, layer_number):
        layer = self.layers[layer_number]

        if layer_number > 0:
            with self.graph.subgraph(name=f'h{layer_number}') as line_subgraph:
                line_subgraph.attr(rank='same')
                for parents_id, group in [g for g in layer['groups'].items()
                                          if g[0]]:
                    people = list(group)
                    if len(people) == 1:
                        self.graph.edge(parents_id, people[0].id, weight=10000)
                    else:
                        head_nodes = []
                        for person in people[1:-1]:
                            node_id = f'n{person.id}'
                            line_subgraph.node(node_id, invis=True)
                            self.graph.edge(node_id, person.id, weight=10000)
                            head_nodes.append(node_id)
                        n_nodes = len(head_nodes)
                        if n_nodes % 2 == 0:
                            node_id = f'b{parents_id}'
                            head_nodes.insert(n_nodes // 2, node_id)
                            line_subgraph.node(node_id, invis=True)
                        self.graph.edge(parents_id, head_nodes[n_nodes // 2],
                                           weight=10000)
                        for prev_id, cur in zip(head_nodes, head_nodes[1:]):
                            self.graph.edge(prev_id, cur, weight=10)
                        self.graph.edge(self.people_layer[0], head_nodes[0],
                                        invis=True)
                        self.graph.edge(head_nodes[0], people[0].id, weight=5)
                        self.graph.edge(head_nodes[-1], people[-1].id, weight=5)

        with self.graph.subgraph(name=f'p{layer_number}') as person_subgraph:
            self.people_layer = []
            person_subgraph.attr(rank='same')
            for person in layer['people']:
                person_subgraph.node(person, is_subject=(person==self.subject),
                                     kinship_subject=self.kinship_subject)
                self.people_layer.append(person.id)
            for couple_id, (left, right) in layer['edges'].items():
                if ((relationship := self.family.get_relationship(left, right))
                        and relationship.type == 'marriage'):
                    join_style = TreeGraph.MARRIED_EDGE
                else:
                    join_style = TreeGraph.UNMARRIED_EDGE
                person_subgraph.node(couple_id, invis=True)
                self.graph.edge(left.id, couple_id, join_style, weight=1000)
                self.graph.edge(couple_id, right.id, join_style, weight=1000)
                self.people_layer.insert(self.people_layer.index(right.id),
                                         couple_id)
            for prev, cur in zip(self.people_layer, self.people_layer[1:]):
                self.graph.edge(prev, cur, invis=True)

if __name__ == '__main__':
    family = Family()
    family.add_all()
    tree = Tree(family.people[5])
    tree.graph.view()