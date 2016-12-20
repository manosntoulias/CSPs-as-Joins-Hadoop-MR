import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;


public class produce_data
{
	static Integer value;
	static boolean stored = true;	
	public static ArrayList<ArrayList<ArrayList<Integer>>> permutations;

	//PRODUCE DATA FOR Spatially Balanced Latin Squares PROBLEM
	public static void main(String[] args) throws IOException
	{
		if (args.length != 1)
		{
			System.out.println("usage: java produce_data <number_of_squares_per_line>");
			return;
		}
		
		int num_of_squares = Integer.parseInt(args[0]);
		ArrayList<Integer> color_order = new ArrayList<Integer>(num_of_squares);
		int file_number = 1;

		//init_lin_system(num_of_squares);
		int[] fixed = {}; // 3 ????
		int[] sol = init_lin_system(num_of_squares, fixed);

		//path to store data files
		String input_path = "./SBL-Squares-" + num_of_squares;

		//create input directory
		boolean success = (new File(input_path)).mkdirs();
		if (!success) {
		    System.out.println("Error while creating directory. Delete it if it already exists");
		    return;
		}

		//create filenames for 1st contraint (having every color in every line and every column)
		FileWriter[][] files_vert = new FileWriter[num_of_squares-1][];
		for (int i=0; i<num_of_squares-1; i++)
		{	files_vert[i] = new FileWriter[num_of_squares-i-1];
			for (int j=num_of_squares-i-2; j >= 0; j--, file_number++)
				files_vert[i][j] = new FileWriter(input_path + "/" + file_number);
		}

		//headers
		String pair_variables;
		int ii, jj;
		for (ii=1; ii<=num_of_squares; ii++)
		{	pair_variables = "";
			for (jj=1; jj<num_of_squares; jj++)
			{	pair_variables = pair_variables + "X_" + jj + "_" + ii + "\t";
			}
			pair_variables = pair_variables + "X_" + jj + "_" + ii;
			for (int j,i=ii+1; i<=num_of_squares; i++)
			{	String header_vert = "";
				for (j=1; j<num_of_squares; j++)
					header_vert = header_vert + "X_" + j + "_" + i + "\t";
				header_vert = header_vert + "X_" + j + "_" + i;
				files_vert[ii-1][num_of_squares-i].write(header_vert + "\t" + pair_variables);
			}	
		}

		// Initialize first order of colors
		for (int i=0; i<num_of_squares; i++)
			color_order.add(i);

		// construct lists and save permutations
		permutations = new ArrayList<ArrayList<ArrayList<Integer>>>();
		for (int i=0; i<num_of_squares; i++)
			permutations.add(new ArrayList<ArrayList<Integer>>());
		for (int column = num_of_squares-1; column >= 1; column--)
		{	java.util.Collections.swap(color_order, 0, column);
		    	value = color_order.remove(column);
		    	color_order.add(1, value);
			permute_store(color_order, 2, permutations);
		    	value = color_order.remove(1);
		    	color_order.add(column, value);
			java.util.Collections.swap(color_order, column, 0);
		}
		for (int i=0; i<num_of_squares; i++)
			System.out.println(permutations.get(i));
		System.out.println(color_order);
		// end of permutations
		
			
		double sum = (num_of_squares+1)*num_of_squares/3-num_of_squares;
		for (int i=0; i<fixed.length; i++)
			sum -= fixed[i]-1;

		// A) iterative solutions
		//find_all_solutions(color_order, sol, 0, (num_of_squares+1)*num_of_squares/3-num_of_squares, files_vert); // old
		find_all_solutions(color_order, sol, fixed.length, sum, files_vert);

		// B) recursice solutions
		for (int i = fixed.length; i< num_of_squares; i++)
			sol[i] = 1;
		//solutions_rec(color_order, sol, -1, num_of_squares, (num_of_squares+1)*num_of_squares/3, files_vert); //old
		//solutions_rec(color_order, sol, -1+fixed.length, 0, sum, files_vert);
		System.out.println("Counted: " + cnt + " solutions");
		System.out.println("Counted: " + number_of_records + " records");

		//1ST constraint
		System.out.println("<<" + java.util.Arrays.toString(sol) + ">>");

		for (int i=0; i<num_of_squares-1; i++)
			for (int j=0; j<num_of_squares-i-1; j++)
				files_vert[i][j].close();
					
	}

	static int[] init_lin_system(int n, int[] fixed_digits)
	{
		int sum = n*(n+1)/3;
		double m = (n+1)/3.0;
		int[] sol = new int[n];
		int f_len;
		if (fixed_digits == null)	
			f_len = 0;
		else
			f_len = fixed_digits.length;

		if (n % 3 == 1)
		{	System.out.println("Integer must not be divided by (3k+1)");
			System.exit(0);
		}
		else if (n % 3 == 2)
			for (int i=0; i<n; i++)
				sol[i] = (int)m;		
		/*else if (n % 3 == 0)
			for (int i=0; i<n; i+=3)
			{	sol[i] = sol[i+1] = (int)m;
				sol[i+2] = (int)Math.ceil(m);
			}
		for (int i=fixed_digits, j=n-1, temp; ;)
		{	while (sol[j] == n-1) j--;
			while (sol[i] == 1)   i++;
			if (i>=j)	break;
			sol[i]--; sol[j]++;
		}*/
		for (int i=0; i<f_len; i++)
		{	sol[i] = fixed_digits[i];
			sum -= fixed_digits[i];
		}
		for (int i=f_len; i<n; i++)
			sol[i] = 1;
		sum -= n-f_len;
		
		int div = sum/(n-2);
		int mod = sum%(n-2);
		int i;	
		for (i=n-1; i>= n-div; i--)
			sol[i] = n-1;
		sol[i] += mod;

		System.out.println("<<" + java.util.Arrays.toString(sol) + ">>");
		return sol;
	}


	static void find_all_solutions(ArrayList<Integer> color_order, int[] sol, int fixed_dgts, double sum2, FileWriter[][] files_vert) throws IOException
	{	int k, n = sol.length;
		int digit, sum, mod2, div;
		long count=0;
		int div2 = (int)sum2 / (n-2);
		mod2 = (int)sum2 % (n-2);
		while (true)
		{	count++;
			//System.out.println(java.util.Arrays.toString(sol).replaceAll(",|\\[|\\]", ""));
			//permute(color_order, 1, sol, files_vert, 0);
			for (int column = n-1; column >= sol[0]; column--)
			{	java.util.Collections.swap(color_order, 0, column);
			    	value = color_order.remove(column);
			    	color_order.add(1, value);
				permute(color_order, 2, sol, files_vert, column);
			    	value = color_order.remove(1);
			    	color_order.add(column, value);
				java.util.Collections.swap(color_order, column, 0);
			}
			/*******************************************************************************/

			for (k=fixed_dgts; sol[k]==n-1; k++);
			if (k - fixed_dgts == div2 && sol[k]-1 == mod2)
			{	System.out.println("iteratively Counted: " + count + " solutions");
				break;
			}
			digit=n-1; sum=-1;
			if (sol[digit] != 1 && sol[digit-1] != n-1)
			{	sol[digit]--;
				sol[digit-1]++;
				continue;
			}
			if (sol[digit] == 1)
			{	while (sol[digit] == 1)
				{	
					digit--;
				}
				/*
				sum += sol[digit] -1;
				sol[digit] = 1;
				digit--;
				
				while (sol[digit] == n-1)
				{	sum += sol[digit] -1;
					sol[digit] = 1;
					digit--;
				}
				sol[digit]++;*/
			}
			//else if (sol[digit-1] == n-1) {
				sum += sol[digit] -1;
				sol[digit] = 1;
				digit--;
				while (sol[digit] == n-1)
				{	sum += sol[digit] -1;
					sol[digit] = 1;
					digit--;
				}
				sol[digit]++;	
			//}

			div = sum/(n-2);
			int j;
			for (j=n-1; n-1-j<div; j--)
				sol[j] = n-1;
			sol[j] += sum%(n-2);
		}
	}

	static long cnt=0;
	static void solutions_rec(ArrayList<Integer> color_order, int[] sol, int next, int counter, double sum, FileWriter[][] files_vert) throws IOException
	{	int n = sol.length;
		next++;
		for (int i = 0; i < n-1; i++)
		{
			if (counter == sum)
			{	cnt++;
				if (stored == false)
				{	for (int column = n-1; column >= sol[0]; column--)
					{	java.util.Collections.swap(color_order, 0, column);
					    	value = color_order.remove(column);
					    	color_order.add(1, value);
						permute(color_order, 2, sol, files_vert, column);
					    	value = color_order.remove(1);
					    	color_order.add(column, value);
						java.util.Collections.swap(color_order, column, 0);
					}
				}
				else
				{	
					for (int ii=permutations.size()-1; ii>= 1; ii--)
					{	ArrayList<ArrayList<Integer>> temp = permutations.get(ii);
						if (temp.get(0).get(0) - sol[0] < 0)
							break;
						for (int j=0; j< temp.size(); j++)
						{	ArrayList<Integer> arr = temp.get(j);
							int index0;
							int[] used = new int[arr.size()];
							int[] pair = new int[arr.size()];
							index0 = pair[0] = arr.get(0)-sol[0];
							used[index0] = 1;
							if ( Math.abs(pair[0] - arr.get(0)) == sol[0])
							{	
								pair[index0] = 0; used[0] = 1;
								if ( Math.abs(pair[index0] - arr.get(index0)) == sol[index0])
									valid_dist(arr, 1, sol, files_vert, pair, used, -1);
							}
						}
					}
				}
				return;
			}
			else if(counter + (n -1 - sol[next]) + (n-2)*(n-1-next) < sum)
				return;
			if (next != n-1)	
				solutions_rec(color_order, java.util.Arrays.copyOf(sol, n), next, counter, sum, files_vert);
			sol[next]++; counter++;
		}

	}

	static void permute(ArrayList<Integer> arr, int k, int[] sol, FileWriter[][] files_vert, int column) throws IOException {
		for(int i=k; i < arr.size(); i++){
		    value = arr.remove(i);
		    arr.add(k, value);

		    permute(arr, k+1, sol, files_vert, column);

		    value = arr.remove(k);
		    arr.add(i, value);
		}
		if (k == arr.size() -1)
		{	if (arr.get(0) != 0)
			{	value = arr.remove(1);
		    		arr.add((int)arr.get(0), value);
			}

			int index0;
			int[] used = new int[arr.size()];
			int[] pair = new int[arr.size()];
			index0 = pair[0] = arr.get(0)-sol[0]; used[index0] = 1;
			if ( Math.abs(pair[0] - arr.get(0)) == sol[0])
			{	
				pair[index0] = 0; used[0] = 1;
				if ( Math.abs(pair[index0] - arr.get(index0)) == sol[index0])
					valid_dist(arr, 1, sol, files_vert, pair, used, column);
			}

			if (arr.get(0) != 0)
			{	value = arr.remove((int)arr.get(0));
		    		arr.add(1, value);
			}
		}
        }

	static void permute_store(ArrayList<Integer> arr, int k, ArrayList<ArrayList<ArrayList<Integer>>> permutations) throws IOException {
		int i;
		for(i=k; i < arr.size(); i++){
		    value = arr.remove(i);
		    arr.add(k, value);

		    permute_store(arr, k+1, permutations);

		    value = arr.remove(k);
		    arr.add(i, value);
		}
		if (k == arr.size() -1)
		{	if (arr.get(0) != 0)
			{	value = arr.remove(1);
		    		arr.add((int)arr.get(0), value);
			}

			permutations.get(arr.get(0)).add(new ArrayList<Integer>(arr));

			if (arr.get(0) != 0)
				{	value = arr.remove((int)arr.get(0));
			    		arr.add(1, value);
				}
		}
        }

	public static long number_of_records = 0;
	static void valid_dist(ArrayList<Integer> arr, int k, int[] sol, FileWriter[][] files_vert, int[] pair, int[] used, int column) throws IOException
	{	if (k == arr.size())
		{	String record = java.util.Arrays.toString(arr.toArray()).replaceAll(",|\\[|\\]", "")
				+ "\t"+ java.util.Arrays.toString(pair).replaceAll(",|\\[|\\]", "");
			record = "\n" + record.replaceAll(" ", "\t");

			files_vert[pair[0]][pair.length-arr.get(0)-1].write(record);
			number_of_records++;
			return;
		}
		else if (k == pair[0])
		{	valid_dist(arr, k+1, sol, files_vert, pair, used, column);
			return;
		}
		if ((pair[k] = arr.get(k) - sol[k]) >= 0 && used[pair[k]] == 0)
		{	used[pair[k]] = 1;
			valid_dist(arr, k+1, sol, files_vert, pair, used, column);
			used[pair[k]] = 0;
		}
		if ((pair[k] = arr.get(k) + sol[k]) < arr.size() && used[pair[k]] == 0)
		{	used[pair[k]] = 1;
			valid_dist(arr, k+1, sol, files_vert, pair, used, column);
			used[pair[k]] = 0;
		}
	}

}

