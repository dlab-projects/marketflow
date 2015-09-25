libname ctm_20150102 '/work/dlab-finance/basic-taq/';

proc export Data = ctm_20150102.MySASDataset
	   file = "/work/dlab-finance/basic-taq/ctm_20150102.csv"
	   DBMS = csv
	   replace;
run;

