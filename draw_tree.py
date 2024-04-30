from math import ceil

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
    UNMARRIED_EDGE = {}

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

                prev_p_anc_id = f'ap{layer_number-1}'
                h_anc_id = f'ah{layer_number}'
                cur_p_anc_id = f'ap{layer_number}'
                self.graph.edge(prev_p_anc_id, h_anc_id, invis=True)
                line_subgraph.node(h_anc_id, invis=True)
                self.graph.edge(h_anc_id, cur_p_anc_id, invis=True)

                for parents_id, group in [g for g in layer['groups'].items()
                                          if g[0]]:
                    people = list(group)
                    if len(people) == 1:
                        self.graph.edge(parents_id, people[0].id, weight=10000)
                    else:
                        head_nodes = []
                        head_edges = []
                        for person in people[1:-1]:
                            node_id = f'n{person.id}'
                            self.graph.edge(node_id, person.id, weight=10000)
                            head_nodes.append(node_id)
                            head_edges.append((node_id, person.id))
                        n_nodes = len(head_nodes)
                        if n_nodes % 2 == 0:
                            node_id = f'b{parents_id}'
                            midpoint = n_nodes // 2
                            person_id = people[midpoint].id
                            person = [p for p in layer['people']
                                      if p.id == person_id][0]
                            rel_ids = [id for (id, r) in layer['edges'].items()
                                       if r[0] == person]
                            if rel_ids:
                                invis_id = rel_ids[0]
                            else:
                                invis_id = f'i{person.id}'
                                person.invis_neighbour = True
                            head_nodes.insert(midpoint, node_id)
                            head_edges.insert(midpoint,
                                              (node_id, invis_id,
                                               {'style': 'invis'}))
                            head_edges.insert(midpoint, (parents_id, node_id))
                            n_nodes += 1
                        else:
                            middle_node = head_nodes[n_nodes // 2]
                            head_edges.insert(n_nodes // 2,
                                              (parents_id, middle_node))
                        if n_nodes > 1:
                            mid = n_nodes / 2
                            parents = (self.layers[layer_number-1]['edges']
                                                  [parents_id])
                            left = parents[0].id
                            right = parents[1].id
                            head_nodes.insert(int(mid), f'm{left}')
                            head_nodes.insert(ceil(mid+1), f'm{right}')
                            for n in range(n_nodes):
                                if n < mid:
                                    self.graph.edge(left, head_nodes[n],
                                                    invis=True)
                                if n > mid+1:
                                    self.graph.edge(right, head_nodes[n],
                                                    invis=True)
                        self.graph.edge(head_nodes[0], people[0].id, weight=5)
                        self.graph.edge(head_nodes[-1], people[-1].id, weight=5)
                        for node_id in head_nodes:
                            line_subgraph.node(node_id, invis=True)
                        for edge in head_edges:
                            self.graph.edge(*edge, weight=10000)
                        for prev_id, cur in zip(head_nodes, head_nodes[1:]):
                            self.graph.edge(prev_id, cur, weight=100)

        with self.graph.subgraph(name=f'p{layer_number}') as person_subgraph:
            self.people_layer = []
            person_subgraph.attr(rank='same')

            person_subgraph.node(f'ap{layer_number}', invis=True)

            for person in layer['people']:
                person_subgraph.node(person, is_subject=(person==self.subject),
                                     kinship_subject=self.kinship_subject)
                self.people_layer.append(person.id)
                if hasattr(person, 'invis_neighbour'):
                    invis_id = f'i{person.id}'
                    person_subgraph.node(invis_id, invis=True)
                    self.people_layer.append(invis_id)
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