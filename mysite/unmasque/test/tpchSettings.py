relations = ["orders", "lineitem", "customer", "supplier", "part", "partsupp", "nation", "region"]

key_lists = [[('part', 'p_partkey'), ('partsupp', 'ps_partkey'), ('lineitem', 'l_partkey')],
             [('supplier', 's_suppkey'), ('partsupp', 'ps_suppkey'), ('lineitem', 'l_suppkey')],
             [('supplier', 's_nationkey'), ('nation', 'n_nationkey'), ('customer', 'c_nationkey')],
             [('customer', 'c_custkey'), ('orders', 'o_custkey')],
             [('orders', 'o_orderkey'), ('lineitem', 'l_orderkey')],
             [('region', 'r_regionkey'), ('nation', 'n_regionkey')]]

from_rels = {'tpch_query1': ["lineitem"],
             'tpch_query3': ["customer", "orders", "lineitem"],
             'Q1': ["lineitem"],
             'Q2': ["part", "supplier", "partsupp", "nation", "region"],
             'Q3': ["customer", "orders", "lineitem"],
             'Q3_1': ["customer", "orders", "lineitem"],
             'Q4': ["orders"],
             'Q5': ["customer", "orders", "lineitem", "supplier", "nation", "region"],
             'Q6': ["lineitem"],
             'Q7': ["customer", "orders", "lineitem", "nation"],
             'Q11': ["partsupp", "supplier", "nation"],
             'Q16': ["partsupp", "part"],
             'Q17': ["lineitem", "part"],
             'Q18': ["partsupp", "part"],
             'Q21': ["supplier", "lineitem", "orders", "nation"],
             'Q23_1': ["partsupp", "supplier", "nation", "region"],
             'Q18_test': ["partsupp", "part"],
             'Q18_test1': ["partsupp", "part"],
             'Q9_simple': ['part', 'supplier', 'lineitem', 'partsupp', 'orders', 'nation'],
             'Q10_simple': ['customer','orders','lineitem','nation']}
