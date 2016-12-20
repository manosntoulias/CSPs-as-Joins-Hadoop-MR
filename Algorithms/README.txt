make file is inside the join_source folder. Read it to find out how to compile and run the porgrams.

ABBREVATIONS FOR VARIABLES IN MAKEFILE:
input path -> IN
number of reducers -> RDS

example:
make exec_cons IN=../../input/n-queens/n-queens-5/ RDS=16

-Results of cascade(repartition) and semi join are stored in the input path
-Results  of constructive join are stored whre the makefile is (in the source folder)

-Always remember to delete previous output in case it overlaps with next program's input.

-including modified prolog file for n-queens in order to compare the the algorithms.

-the jar with the executable file is included just in case.
