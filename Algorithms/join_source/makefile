join: CascadeJoin.java string/StringManipulation.java HeaderInputFormat.java calculation/InputOptimization.java TaggedKey.java JoiningPartitioner.java JoiningGroupingComparator.java ConstructiveJoin.java NotSplitHeaderFormat.java hash_semi_join.java semi_join_phase2.java broadcast_join.java Semi_Join.java
	mkdir -p ../join
	hadoop com.sun.tools.javac.Main -d ../join CascadeJoin.java string/StringManipulation.java HeaderInputFormat.java calculation/InputOptimization.java TaggedKey.java JoiningPartitioner.java JoiningGroupingComparator.java ConstructiveJoin.java NotSplitHeaderFormat.java hash_semi_join.java semi_join_phase2.java broadcast_join.java Semi_Join.java
	jar -cf ../join.jar -C ../join/ .

exec_rep:
	hadoop jar ../join.jar CascadeJoin $(IN) $(RDS)

exec_cons:
	hadoop jar ../join.jar ConstructiveJoin $(IN) $(RDS)

exec_semi:
	hadoop jar ../join.jar Semi_Join $(IN) $(RDS)

clean:
	rm -r ../join
	

