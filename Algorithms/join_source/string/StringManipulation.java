package string;

import java.util.Arrays;
import java.util.NoSuchElementException;
import java.util.ArrayList;
import java.util.HashMap;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.util.Iterator;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.ContentSummary;



/**
 * Created by hduser on 16/12/2015.
 */
public class StringManipulation {

    public static String get_header(String file1, FileSystem fs) throws IOException
    {
	//INPUT: PATH OF A FILE
	//OUTPUT: FIRST LINE OF FILE

	Path pt=new Path(file1);
	if (fs.isDirectory(pt))
	{	file1 = adjust_input(file1);
		pt = new Path(file1 + "part-r-00000");
		if (!fs.exists(pt))
			pt = new Path(file1 + "part-m-00000");
	}
	BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
	String header = br.readLine();
	br.close();
	return header; 

    }

    public static String new_header(String line, String keys, String separator)
    {
	String header;
	if (keys != separator)
		header = line.trim() + separator + keys.trim();
	else
		header = line;
	
	return header;
    }


    public static String adjust_input(String input_path)
    {
	if (!input_path.substring(input_path.length()-1).equals("/"))
		input_path = input_path + "/";
	return input_path; 
    }

    public static String[] Intersection(String s1, String s2, String separator)
    {
        // Get two strings and return their common words.
        // strings s1 and s2 must contain spaces between words.

	if (s1 == null || s2 == null)
	{	System.out.println("Error: input strings are null");
		return null;
	}

        String[] itr1 = s1.split(separator);
        String[] itr2 = s2.split(separator);
        String[] header = s2.split(separator);
        int size1 = itr1.length, size2 = itr2.length;
        int counter, counter2;
        int count_keycolumns=0;
        int[] column_index = new int[size2];
        String[] column_index_header = new String[size2];
        String keys = "";
        String keys2 = "";
        String unique_columns = separator;
	String unique_columns2 = separator;

        Arrays.fill(column_index, -1);

        try {
            for (counter=0; counter < size1; counter++)
            {	for (counter2=0; counter2 < size2; counter2++)
            	{
		        if (itr1[counter].compareTo(itr2[counter2]) == 0)
		        {
		            keys = keys + 0 + separator;
		            column_index[counter2] = count_keycolumns++;
		            header[counter2] = null;
		            break;
		        }
            	}
                if (counter2 >= size2)
                {	keys = keys + -1 + separator;
                    unique_columns = unique_columns + itr1[counter] + separator;
                }
            }


            for (counter=0; counter < column_index.length - 1; counter++)
                keys2 = keys2 + column_index[counter] + separator;
            keys2 = keys2 + column_index[counter];
	    for (String str: header)
			if (str != null)
				unique_columns2 = unique_columns2 + str + separator;

        } catch (NoSuchElementException e) {}

	String[] ret = new String[4];
	ret[0] = keys; ret[1] = keys2;
	ret[2] = unique_columns; ret[3] = unique_columns2;
	//System.out.println(s1 + " has keys:\n" + keys + "\n\n" + s2 + " has keys:\n" + keys2);
	return ret;

    }

    public static HashMap<String, ArrayList<String>> var_in_files(String input_path, FileSystem fs, String separator) throws IOException
    {
	if (input_path == null)
	{
		System.out.println("input_path is null");
		return null;
	}

	HashMap<String, ArrayList<String>> var_in_files = new HashMap<String, ArrayList<String>>();

	Path dir_pt = new Path(input_path);
	ContentSummary cs = fs.getContentSummary(dir_pt);
	long fileCount = cs.getFileCount();

	for (int i=1; i<=fileCount; i++)
	{
		String file = input_path + i;
		String header = get_header(file, fs);
		String[] vars = header.split(separator);
		for (int j=0; j < vars.length; j++)
		{	ArrayList<String> pair;
			if ((pair = var_in_files.get(vars[j])) == null)
			{	pair = new ArrayList<String>();
				var_in_files.put(vars[j], pair);
			}
			pair.add(Integer.toString(i)); //file number/name. even
			//pair.add(Integer.toString(0)); //index of var. how many vars we've so far checked in this file. odd
			
			
		}
	}
	return var_in_files;
    }

    //called after var_in_files
    public static HashMap<String, String> var_positions(int num_of_files)
    {
	HashMap<String, String> var_positions = new HashMap<String, String>();
	for (int i=1; i <= num_of_files; i++)
	{
		var_positions.put(Integer.toString(i), "0");
	}
	return var_positions;
    }


    public static String get_sol_header_index (String solution_header, String file_header, String separator)
    {
	int index = 0;
	String sol_header_index = "";
	StringTokenizer solution_itr = new StringTokenizer(solution_header, separator);
	StringTokenizer file_itr = new StringTokenizer(file_header, separator);

	while (file_itr.hasMoreTokens())
	{	String file_token = file_itr.nextToken();
		while (solution_itr.hasMoreTokens())
		{	String sol_token = solution_itr.nextToken();
			if (sol_token.equals(file_token))
			{	sol_header_index += index + separator;
				index++;
				break;
			}
			index++;
		}
		
	}
	return sol_header_index;
    }
}
