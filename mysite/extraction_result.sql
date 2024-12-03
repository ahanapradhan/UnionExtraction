
 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 
        (select n_name, c_acctbal from nation, customer where c_nationkey = n_nationkey and n_regionkey > 3)
        UNION ALL
        (select r_name, s_acctbal from region, nation, supplier where r_regionkey = n_regionkey 
        and n_nationkey = s_nationkey and s_name = 'Supplier#000000008');
        
 --- extracted query:
  
 (Select n_name, c_acctbal 
 From customer, nation 
 Where customer.c_nationkey = nation.n_nationkey
 and nation.n_regionkey >= 4)
 UNION ALL  
 (Select r_name as n_name, s_acctbal as c_acctbal 
 From nation, region, supplier 
 Where nation.n_nationkey = supplier.s_nationkey
 and nation.n_regionkey = region.r_regionkey
 and supplier.s_name = 'Supplier#000000008');
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 
        (select n_name, c_acctbal from nation, customer where c_nationkey = n_nationkey and n_regionkey > 3)
        UNION ALL
        (select r_name, s_acctbal from region, nation, supplier where r_regionkey = n_regionkey 
        and n_nationkey = s_nationkey and s_name = 'Supplier#000000008');
        
 --- extracted query:
  
 (Select r_name as n_name, s_acctbal as c_acctbal 
 From nation, region, supplier 
 Where nation.n_nationkey = supplier.s_nationkey
 and nation.n_regionkey = region.r_regionkey
 and supplier.s_name = 'Supplier#000000008')
 UNION ALL  
 (Select n_name, c_acctbal 
 From customer, nation 
 Where customer.c_nationkey = nation.n_nationkey
 and nation.n_regionkey >= 4);
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 
        (select n_name, c_acctbal from nation, customer where c_nationkey = n_nationkey and n_regionkey > 3)
        UNION ALL
        (select r_name, s_acctbal from region, nation, supplier where r_regionkey = n_regionkey 
        and n_nationkey = s_nationkey and s_name = 'Supplier#000000008');
        
 --- extracted query:
  
 (Select r_name as n_name, s_acctbal as c_acctbal 
 From nation, region, supplier 
 Where nation.n_nationkey = supplier.s_nationkey
 and nation.n_regionkey = region.r_regionkey
 and supplier.s_name = 'Supplier#000000008')
 UNION ALL  
 (Select n_name, c_acctbal 
 From customer, nation 
 Where customer.c_nationkey = nation.n_nationkey
 and nation.n_regionkey >= 4);
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 
        (select n_name, c_acctbal from nation, customer where c_nationkey = n_nationkey and n_regionkey > 3)
        UNION ALL
        (select r_name, s_acctbal from region, nation, supplier where r_regionkey = n_regionkey 
        and n_nationkey = s_nationkey and s_name = 'Supplier#000000008');
        
 --- extracted query:
  
 (Select n_name, c_acctbal 
 From customer, nation 
 Where customer.c_nationkey = nation.n_nationkey
 and nation.n_regionkey >= 4)
 UNION ALL  
 (Select r_name as n_name, s_acctbal as c_acctbal 
 From nation, region, supplier 
 Where nation.n_nationkey = supplier.s_nationkey
 and nation.n_regionkey = region.r_regionkey
 and supplier.s_name = 'Supplier#000000008');
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 
        (select n_name, c_acctbal from nation, customer where c_nationkey = n_nationkey and n_regionkey > 3)
        UNION ALL
        (select r_name, s_acctbal from region, nation, supplier where r_regionkey = n_regionkey 
        and n_nationkey = s_nationkey and s_name = 'Supplier#000000008');
        
 --- extracted query:
  
 (Select r_name as n_name, s_acctbal as c_acctbal 
 From nation, region, supplier 
 Where nation.n_nationkey = supplier.s_nationkey
 and nation.n_regionkey = region.r_regionkey
 and supplier.s_name = 'Supplier#000000008')
 UNION ALL  
 (Select n_name, c_acctbal 
 From customer, nation 
 Where customer.c_nationkey = nation.n_nationkey
 and nation.n_regionkey >= 4);
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 
        (select n_name, c_acctbal from nation, customer where c_nationkey = n_nationkey and n_regionkey > 3)
        UNION ALL
        (select r_name, s_acctbal from region, nation, supplier where r_regionkey = n_regionkey 
        and n_nationkey = s_nationkey and s_name = 'Supplier#000000008');
        
 --- extracted query:
  
 (Select r_name as n_name, s_acctbal as c_acctbal 
 From nation, region, supplier 
 Where nation.n_nationkey = supplier.s_nationkey
 and nation.n_regionkey = region.r_regionkey
 and supplier.s_name = 'Supplier#000000008')
 UNION ALL  
 (Select n_name, c_acctbal 
 From customer, nation 
 Where customer.c_nationkey = nation.n_nationkey
 and nation.n_regionkey >= 4);
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 
        (select n_name, c_acctbal from nation, customer where c_nationkey = n_nationkey and n_regionkey > 3)
        UNION ALL
        (select r_name, s_acctbal from region, nation, supplier where r_regionkey = n_regionkey 
        and n_nationkey = s_nationkey and s_name = 'Supplier#000000008');
        
 --- extracted query:
  
 (Select n_name, c_acctbal 
 From customer, nation 
 Where customer.c_nationkey = nation.n_nationkey
 and nation.n_regionkey >= 4)
 UNION ALL  
 (Select r_name as n_name, s_acctbal as c_acctbal 
 From nation, region, supplier 
 Where nation.n_nationkey = supplier.s_nationkey
 and nation.n_regionkey = region.r_regionkey
 and supplier.s_name = 'Supplier#000000008');
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 
        (select n_name, c_acctbal from nation, customer where c_nationkey = n_nationkey and n_regionkey > 3)
        UNION ALL
        (select r_name, s_acctbal from region, nation, supplier where r_regionkey = n_regionkey 
        and n_nationkey = s_nationkey and s_name = 'Supplier#000000008');
        
 --- extracted query:
  
 (Select n_name, c_acctbal 
 From customer, nation 
 Where customer.c_nationkey = nation.n_nationkey
 and nation.n_regionkey >= 4)
 UNION ALL  
 (Select r_name as n_name, s_acctbal as c_acctbal 
 From nation, region, supplier 
 Where nation.n_nationkey = supplier.s_nationkey
 and nation.n_regionkey = region.r_regionkey
 and supplier.s_name = 'Supplier#000000008');
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 
        (select n_name, c_acctbal from nation, customer where c_nationkey = n_nationkey and n_regionkey > 3)
        UNION ALL
        (select r_name, s_acctbal from region, nation, supplier where r_regionkey = n_regionkey 
        and n_nationkey = s_nationkey and s_name = 'Supplier#000000008');
        
 --- extracted query:
  
 (Select n_name, c_acctbal 
 From customer, nation 
 Where customer.c_nationkey = nation.n_nationkey
 and nation.n_regionkey >= 4)
 UNION ALL  
 (Select r_name as n_name, s_acctbal as c_acctbal 
 From nation, region, supplier 
 Where nation.n_nationkey = supplier.s_nationkey
 and nation.n_regionkey = region.r_regionkey
 and supplier.s_name = 'Supplier#000000008');
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 
        (select n_name, c_acctbal from nation, customer where c_nationkey = n_nationkey and n_regionkey > 3)
        UNION ALL
        (select r_name, s_acctbal from region, nation, supplier where r_regionkey = n_regionkey 
        and n_nationkey = s_nationkey and s_name = 'Supplier#000000008');
        
 --- extracted query:
  
 (Select r_name as n_name, s_acctbal as c_acctbal 
 From nation, region, supplier 
 Where nation.n_nationkey = supplier.s_nationkey
 and nation.n_regionkey = region.r_regionkey
 and supplier.s_name = 'Supplier#000000008')
 UNION ALL  
 (Select n_name, c_acctbal 
 From customer, nation 
 Where customer.c_nationkey = nation.n_nationkey
 and nation.n_regionkey >= 4);
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 
        (select n_name, c_acctbal from nation, customer where c_nationkey = n_nationkey and n_regionkey > 3)
        UNION ALL
        (select r_name, s_acctbal from region, nation, supplier where r_regionkey = n_regionkey 
        and n_nationkey = s_nationkey and s_name = 'Supplier#000000008');
        
 --- extracted query:
  
 (Select r_name as n_name, s_acctbal as c_acctbal 
 From nation, region, supplier 
 Where nation.n_nationkey = supplier.s_nationkey
 and nation.n_regionkey = region.r_regionkey
 and supplier.s_name = 'Supplier#000000008')
 UNION ALL  
 (Select n_name, c_acctbal 
 From customer, nation 
 Where customer.c_nationkey = nation.n_nationkey
 and nation.n_regionkey >= 4);
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 
        (select n_name, c_acctbal from nation, customer where c_nationkey = n_nationkey and n_regionkey > 3)
        UNION ALL
        (select r_name, s_acctbal from region, nation, supplier where r_regionkey = n_regionkey 
        and n_nationkey = s_nationkey and s_name = 'Supplier#000000008');
        
 --- extracted query:
  
 (Select n_name, c_acctbal 
 From customer, nation 
 Where customer.c_nationkey = nation.n_nationkey
 and nation.n_regionkey >= 4)
 UNION ALL  
 (Select r_name as n_name, s_acctbal as c_acctbal 
 From nation, region, supplier 
 Where nation.n_nationkey = supplier.s_nationkey
 and nation.n_regionkey = region.r_regionkey
 and supplier.s_name = 'Supplier#000000008');
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 
        (select n_name, c_acctbal from nation, customer where c_nationkey = n_nationkey and n_regionkey > 3)
        UNION ALL
        (select r_name, s_acctbal from region, nation, supplier where r_regionkey = n_regionkey 
        and n_nationkey = s_nationkey and s_name = 'Supplier#000000008');
        
 --- extracted query:
  
 (Select r_name as n_name, s_acctbal as c_acctbal 
 From nation, region, supplier 
 Where nation.n_nationkey = supplier.s_nationkey
 and nation.n_regionkey = region.r_regionkey
 and supplier.s_name = 'Supplier#000000008')
 UNION ALL  
 (Select n_name, c_acctbal 
 From customer, nation 
 Where customer.c_nationkey = nation.n_nationkey
 and nation.n_regionkey >= 4);
 --- END OF ONE EXTRACTION EXPERIMENT
