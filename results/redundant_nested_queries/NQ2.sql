select c_name from customer where c_acctbal <= (select MIN(s_acctbal) from supplier);