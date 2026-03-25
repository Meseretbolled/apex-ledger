from __future__ import annotations

END = "__END__"

class _CompiledGraph:
    def __init__(self, entry_point, nodes, edges, conditional_edges):
        self.entry_point = entry_point
        self.nodes = nodes
        self.edges = edges
        self.conditional_edges = conditional_edges

    async def ainvoke(self, state):
        current = self.entry_point
        while current and current != END:
            node = self.nodes[current]
            result = await node(state)
            if result is not None:
                state = result
            if current in self.conditional_edges:
                current = self.conditional_edges[current](state)
            else:
                next_nodes = self.edges.get(current, [])
                current = next_nodes[0] if next_nodes else END
        return state

class StateGraph:
    def __init__(self, state_type=None):
        self.state_type = state_type
        self.nodes = {}
        self.edges = {}
        self.conditional_edges = {}
        self.entry_point = None

    def add_node(self, name, fn):
        self.nodes[name] = fn

    def set_entry_point(self, name):
        self.entry_point = name

    def add_edge(self, src, dst):
        self.edges.setdefault(src, []).append(dst)

    def add_conditional_edges(self, src, selector):
        self.conditional_edges[src] = selector

    def compile(self):
        return _CompiledGraph(self.entry_point, self.nodes, self.edges, self.conditional_edges)
