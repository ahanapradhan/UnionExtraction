
 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 Select ps_COMMENT, sum(ps_supplycost * ps_availqty) as value From partsupp, supplier, nation         Where ps_suppkey = s_suppkey and s_nationkey = n_nationkey and n_name = 'ARGENTINA' Group By ps_COMMENT         Order by value desc Limit 100;
 --- extracted query:
  
 Select ps_comment, Sum(ps_availqty*ps_supplycost) as value 
 From nation, partsupp, supplier 
 Where nation.n_nationkey = supplier.s_nationkey
 and partsupp.ps_suppkey = supplier.s_suppkey
 and nation.n_name = 'ARGENTINA' 
 Group By ps_comment 
 Order By value desc, ps_comment asc 
 Limit 100;
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 Select ps_COMMENT, sum(ps_supplycost * ps_availqty) as value From partsupp, supplier, nation         Where ps_suppkey = s_suppkey and s_nationkey = n_nationkey and n_name = 'ARGENTINA' Group By ps_COMMENT         Order by value desc Limit 100;
 --- extracted query:
  
 Select ps_comment, Sum(ps_availqty*ps_supplycost) as value 
 From nation, partsupp, supplier 
 Where nation.n_nationkey = supplier.s_nationkey
 and partsupp.ps_suppkey = supplier.s_suppkey
 and nation.n_name = 'ARGENTINA' 
 Group By ps_comment 
 Order By value desc, ps_comment asc 
 Limit 100;
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 Select ps_COMMENT, sum(ps_supplycost * ps_availqty) as value From partsupp, supplier, nation         Where ps_suppkey = s_suppkey and s_nationkey = n_nationkey and n_name = 'ARGENTINA' Group By ps_COMMENT         Order by value desc Limit 100;
 --- extracted query:
  
 Select ps_comment, Sum(ps_availqty*ps_supplycost) as value 
 From nation, partsupp, supplier 
 Where nation.n_nationkey = supplier.s_nationkey
 and partsupp.ps_suppkey = supplier.s_suppkey
 and nation.n_name = 'ARGENTINA' 
 Group By ps_comment 
 Order By value desc, ps_comment asc 
 Limit 100;
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 Select ps_COMMENT, sum(ps_supplycost * ps_availqty) as value From partsupp, supplier, nation         Where ps_suppkey = s_suppkey and s_nationkey = n_nationkey and n_name = 'ARGENTINA' Group By ps_COMMENT         Order by value desc Limit 100;
 --- extracted query:
  
 Select ps_comment, Sum(ps_availqty*ps_supplycost) as value 
 From nation, partsupp, supplier 
 Where nation.n_nationkey = supplier.s_nationkey
 and partsupp.ps_suppkey = supplier.s_suppkey
 and nation.n_name = 'ARGENTINA' 
 Group By ps_comment 
 Order By value desc, ps_comment asc 
 Limit 100;
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 Select ps_COMMENT, sum(ps_supplycost * ps_availqty) as value From partsupp, supplier, nation         Where ps_suppkey = s_suppkey and s_nationkey = n_nationkey and n_name = 'ARGENTINA' Group By ps_COMMENT         Order by value desc Limit 100;
 --- extracted query:
 Some problem in Regular mutation pipeline. Aborting extraction!
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 Select ps_COMMENT, sum(ps_supplycost * ps_availqty) as value From partsupp, supplier, nation         Where ps_suppkey = s_suppkey and s_nationkey = n_nationkey and n_name = 'ARGENTINA' Group By ps_COMMENT         Order by value desc Limit 100;
 --- extracted query:
 --- Extraction Failed! Nothing to show! 
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 Select ps_COMMENT, sum(ps_supplycost * ps_availqty) as value From partsupp, supplier, nation         Where ps_suppkey = s_suppkey and s_nationkey = n_nationkey and n_name = 'ARGENTINA' Group By ps_COMMENT         Order by value desc Limit 100;
 --- extracted query:
 Some problem in Regular mutation pipeline. Aborting extraction!
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 Select ps_COMMENT, sum(ps_supplycost * ps_availqty) as value From partsupp, supplier, nation         Where ps_suppkey = s_suppkey and s_nationkey = n_nationkey and n_name = 'ARGENTINA' Group By ps_COMMENT         Order by value desc Limit 100;
 --- extracted query:
  
 Select ps_comment, Sum(ps_availqty*ps_supplycost) as value 
 From nation, partsupp, supplier 
 Where nation.n_nationkey = supplier.s_nationkey
 and partsupp.ps_suppkey = supplier.s_suppkey
 and nation.n_name = 'ARGENTINA' 
 Group By ps_comment 
 Order By value desc, ps_comment asc 
 Limit 100;
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 Select ps_COMMENT, sum(ps_supplycost * ps_availqty) as value From partsupp, supplier, nation         Where ps_suppkey = s_suppkey and s_nationkey = n_nationkey and n_name = 'ARGENTINA' Group By ps_COMMENT         Order by value desc Limit 100;
 --- extracted query:
  
 Select ps_comment, Sum(ps_availqty*ps_supplycost) as value 
 From nation, partsupp, supplier 
 Where nation.n_nationkey = supplier.s_nationkey
 and partsupp.ps_suppkey = supplier.s_suppkey
 and nation.n_name = 'ARGENTINA' 
 Group By ps_comment 
 Order By value desc, ps_comment asc 
 Limit 100;
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 Select ps_COMMENT, sum(ps_supplycost * ps_availqty) as value From partsupp, supplier, nation         Where ps_suppkey = s_suppkey and s_nationkey = n_nationkey and n_name = 'ARGENTINA' Group By ps_COMMENT         Order by value desc Limit 100;
 --- extracted query:
 Cannot do database minimization. Some problem in Regular mutation pipeline. Aborting extraction!
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 Select ps_COMMENT, sum(ps_supplycost * ps_availqty) as value From partsupp, supplier, nation         Where ps_suppkey = s_suppkey and s_nationkey = n_nationkey and n_name = 'ARGENTINA' Group By ps_COMMENT         Order by value desc Limit 100;
 --- extracted query:
 Cannot do database minimization. Some problem in Regular mutation pipeline. Aborting extraction!
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 Select ps_COMMENT, sum(ps_supplycost * ps_availqty) as value From partsupp, supplier, nation         Where ps_suppkey = s_suppkey and s_nationkey = n_nationkey and n_name = 'ARGENTINA' Group By ps_COMMENT         Order by value desc Limit 100;
 --- extracted query:
  
 Select ps_comment, Sum(ps_availqty*ps_supplycost) as value 
 From nation, partsupp, supplier 
 Where nation.n_nationkey = supplier.s_nationkey
 and partsupp.ps_suppkey = supplier.s_suppkey
 and nation.n_name = 'ARGENTINA' 
 Group By ps_comment 
 Order By value desc, ps_comment asc 
 Limit 100;
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 Select ps_COMMENT, sum(ps_supplycost * ps_availqty) as value From partsupp, supplier, nation         Where ps_suppkey = s_suppkey and s_nationkey = n_nationkey and n_name = 'ARGENTINA' Group By ps_COMMENT         Order by value desc Limit 100;
 --- extracted query:
  
 Select ps_comment, Sum(ps_availqty*ps_supplycost) as value 
 From nation, partsupp, supplier 
 Where nation.n_nationkey = supplier.s_nationkey
 and partsupp.ps_suppkey = supplier.s_suppkey
 and nation.n_name = 'ARGENTINA' 
 Group By ps_comment 
 Order By value desc, ps_comment asc 
 Limit 100;
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 Select ps_COMMENT, sum(ps_supplycost * ps_availqty) as value From partsupp, supplier, nation         Where ps_suppkey = s_suppkey and s_nationkey = n_nationkey and n_name = 'ARGENTINA' Group By ps_COMMENT         Order by value desc Limit 100;
 --- extracted query:
  
 Select ps_comment, Sum(ps_availqty*ps_supplycost) as value 
 From nation, partsupp, supplier 
 Where nation.n_nationkey = supplier.s_nationkey
 and partsupp.ps_suppkey = supplier.s_suppkey
 and nation.n_name = 'ARGENTINA' 
 Group By ps_comment 
 Order By value desc, ps_comment asc 
 Limit 100;
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 Select ps_COMMENT, sum(ps_supplycost * ps_availqty) as value From partsupp, supplier, nation         Where ps_suppkey = s_suppkey and s_nationkey = n_nationkey and n_name = 'ARGENTINA' Group By ps_COMMENT         Order by value desc Limit 100;
 --- extracted query:
  
 Select ps_comment, Sum(ps_availqty*ps_supplycost) as value 
 From nation, partsupp, supplier 
 Where nation.n_nationkey = supplier.s_nationkey
 and partsupp.ps_suppkey = supplier.s_suppkey
 and nation.n_name = 'ARGENTINA' 
 Group By ps_comment 
 Order By value desc, ps_comment asc 
 Limit 100;
 --- END OF ONE EXTRACTION EXPERIMENT
