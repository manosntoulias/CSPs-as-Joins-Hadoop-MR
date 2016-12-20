import java.io.File;
import java.io.FileWriter;

public class produce_data
{
	//PRODUCE DATA FOR N-QUEENS PROBLEM
	public static void main(String[] args)
	{
		if (args.length != 1)
		{
			System.out.println("usage: java produce_data <number_of_queens>");
			return;
		}
		
		char[] buf = new char[100];
		int num_of_queens = Integer.parseInt(args[0]);
		String var_name = "x";

		//each queen represents a line
		int queen1;
		int queen2;
		
		
		int line_differ;

		//path to store data files
		String input_path = "./n-queens-" + num_of_queens;
		
		//create input directory
		boolean success = (new File(input_path)).mkdirs();
		if (!success) {
		    System.out.println("Error while creating directory. Delete it if it already exists");
		}
		input_path = input_path + "/";
		
		int file_counter = 1;
		try
		{	for (queen1=1; queen1<num_of_queens; queen1++)
			{
				for (queen2 = queen1 + 1; queen2 <= num_of_queens; queen2++)
				{
					//create a constraint file
					String filename = input_path + file_counter;
					//new File(filename).createNewFile();
					FileWriter fw = new FileWriter(filename);
					boolean reverse = false;

					//write header
					if ((var_name+String.format("%05d", queen1)).compareTo(var_name+String.format("%05d", queen2)) <= 0)
						fw.write(var_name+String.format("%05d", queen1) + "	" + var_name+String.format("%05d", queen2));
					else
					{	fw.write(var_name+String.format("%05d", queen2) + "	" + var_name+String.format("%05d", queen1));
						reverse=true;
					}

					//columns that the two queens are placed
					int column1, column2;
					for (column1=1; column1<=num_of_queens; column1++)
						for (column2=1; column2<=num_of_queens; column2++)
						{
							if (column1 == column2 || queen1 + column1 == queen2 + column2 || 
										  queen1 - column1 == queen2 - column2)
							{
								continue;
							}
							if (reverse == false)
								fw.write("\n" + column1 + "	" + column2);
							else
								fw.write("\n" + column2 + "	" + column1);
						}

					fw.close();
					file_counter++;

				}

			}
		}
		catch (Exception e) { System.out.println(e.getMessage());}
	}
}
