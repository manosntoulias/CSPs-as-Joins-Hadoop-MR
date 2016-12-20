% ECLiPSe SAMPLE CODE
%
% AUTHOR:	Joachim Schimpf, IC-Parc
%
% The famous N-queens problem, using finite domains
% and a selection fo different search strategies
%
% Modified by Manos Ntoulias
% added create_header predicate
% solutions are now stored into files

:- set_flag(gc_interval, 100000000).
:- lib(lists).
:- lib(ic).
:- lib(ic_search).


%--------------------
% The model
%--------------------

queens(N, Board) :-
	length(Board, N),
	Board :: 1..N,
	( fromto(Board, [Q1|Cols], Cols, []) do
	    ( foreach(Q2, Cols), param(Q1), count(Dist,1,_) do
	    	noattack(Q1, Q2, Dist)
	    )
	).

noattack(Q1,Q2,Dist) :-
	Q2 #\= Q1,
	Q2 - Q1 #\= Dist,
	Q1 - Q2 #\= Dist.


%-----------------------
% The search strategies
%-----------------------

labeling(a, AllVars, BT) :-
        search(AllVars, 0, input_order, indomain, complete, [backtrack(BT)]).

labeling(b, AllVars, BT) :-
        search(AllVars, 0, first_fail, indomain, complete, [backtrack(BT)]).

labeling(c, AllVars, BT) :-
        middle_first(AllVars, AllVarsPreOrdered), % static var-select
        search(AllVarsPreOrdered, 0, input_order, indomain, complete, [backtrack(BT)]).

labeling(d, AllVars, BT) :-
        middle_first(AllVars, AllVarsPreOrdered), % static var-select
        search(AllVarsPreOrdered, 0, first_fail, indomain, complete, [backtrack(BT)]).

labeling(e, AllVars, BT) :-
        middle_first(AllVars, AllVarsPreOrdered), % static var-select
        search(AllVarsPreOrdered, 0, first_fail, indomain_middle,
               complete, [backtrack(BT)]). 

% reorder a list so that the middle elements are first

middle_first(List, Ordered) :-
	halve(List, Front, Back),
	reverse(Front, RevFront),
	splice(Back, RevFront, Ordered).


%-----------------------------------
% Toplevel code
%
% all_queens/2 finds all solutions
% first_queens/2 finds one solution
% Strategy is a,b,c,d or e
% N is the size of the board
%-----------------------------------

:- local variable(solutions), variable(backtracks).

create_header(N, N, Header, []) :- N\=0, number_string(N, N_str), append_strings("X_", N_str, Var_name), X is N-1, create_header(N, X, Header, [Var_name]).
create_header(N, Cnt, Header, Cur_Header):- N\=Cnt, Cnt\=0, number_string(Cnt, Cnt_str), append_strings("X_", Cnt_str, Var_name), X is Cnt-1,
											create_header(N, X, Header, [Var_name|Cur_Header]).
create_header(_, 0, Header, Header).


all_queens(Strategy, N) :-		% Find all solutions
	setval(solutions, 0),
	setval(backtracks, 0),
	number_string(N, N_str), append_strings(N_str, "-queens-sol.txt", Output_name),
	open(Output_name, write, Handle),
	create_header(N, N, Header, []),
	writeln(Handle, Header),
	statistics(times, [T0|_]),
	(
	    queens(N, Board),
	    labeling(Strategy, Board, BT),
	    incval(solutions),
	    setval(backtracks, BT),
	    writeln(Handle, Board),
	    fail
	;
	    true
	),
	statistics(times, [T1|_]),
	close(Handle),
	T is T1-T0,
	getval(solutions, S),
	getval(backtracks, B),
	printf("\nFound %d solutions for %d queens in %w s with %d backtracks%n",
		[S,N,T,B]).


first_queens(Strategy, N) :-		% Find one solution
	statistics(times, [T0|_]),
	queens(N, Board),
	statistics(times, [T1|_]),
	D1 is T1-T0,
	printf("Setup for %d queens done in %w seconds", [N,D1]), nl,
        labeling(Strategy, Board, B),
	statistics(times, [T2|_]),
	D2 is T2-T1,
	printf("Found first solution for %d queens in %w s with %d backtracks%n",
		[N,D2,B]).