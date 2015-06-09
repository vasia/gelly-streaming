import networkx as nx
import random

for v in range(1, 11):

    g = nx.barabasi_albert_graph(v * 1000, 30)
    edge_list = g.edges()
    random.shuffle(edge_list)

    fp = open('graph_%d.txt' % (v * 1000), 'wb')
    for (src, trg) in edge_list:
        fp.write("%d %d\n" % (src, trg))
    fp.close()
