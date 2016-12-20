package calculation;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;

import string.StringManipulation;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;


/**
 * Created by hduser on 16/12/2015.
 */
public class InputOptimization {
    //private int[][] multi;
    //private int num_of_files;

    //public InputOptimization(int num_of_files) {
    //    multi = new int[num_of_files][num_of_files];
    //    this.num_of_files = num_of_files;
    //}

    public static String[] Outer(String[] filenames, FileSystem fs, String separator) throws IOException {

        Path pt1, pt2;
        BufferedReader br1, br2;
        String header1, header2;
        int max = -1;
        int counter;
        int[] max_pair = new int[2];
	String[] paths = new String[2];
        long multiply_size = -1;
        long multiply_size2;
        for (int i = 0; i < filenames.length; i++) {

            pt1 = new Path(filenames[i]);
            br1 = new BufferedReader(new InputStreamReader(fs.open(pt1)));   //read 1st line of file1
            header1 = br1.readLine();                                       //read header1
	    br1.close();
            for (int j = i + 1; j < filenames.length; j++) {
                pt2 = new Path(filenames[j]);
                br2 = new BufferedReader(new InputStreamReader(fs.open(pt2))); //read 1st line of file2
                header2 = br2.readLine();                                   //read header2
		br2.close();

                //split the two keys
                String[] keys = StringManipulation.Intersection(header1, header2, separator);
                String[] keys_0 = keys[0].split(separator);
                counter = 0;
                for (String str : keys_0) {
                    if (Integer.parseInt(str) != -1) {
                        counter++;
                    }
                }
                if (counter > max) {

                    max = counter;
                    max_pair[0] = i;
                    max_pair[1] = j;
                    multiply_size = (long) (fs.getContentSummary(new Path(filenames[i])).getLength() *
                            fs.getContentSummary(new Path(filenames[j])).getLength());
                } else if (counter == max) {

                    multiply_size2 = (long) (fs.getContentSummary(new Path(filenames[i])).getLength() *
                            fs.getContentSummary(new Path(filenames[j])).getLength());

                    if (multiply_size2 < multiply_size) {
                        max_pair[0] = i;
                        max_pair[1] = j;
                        multiply_size = multiply_size2;
                    }
                }
            }

        }

	paths[0] = filenames[max_pair[0]];
	paths[1] = filenames[max_pair[1]];
	filenames[max_pair[0]] = null;
	filenames[max_pair[1]] = null;

        return paths;
    }

    public static String Inner(String[] filenames, String filename, FileSystem fs, String separator) throws IOException {

        Path pt1, pt2;
        BufferedReader br1, br2;
        String header1, header2;
        int max = -1;
        int counter;
        int max_single = 0;
        long multiply_size = -1;
        long multiply_size2;

        pt1 = new Path(filename);
	if (fs.isDirectory(pt1))
	{
		pt1 = new Path(filename + "part-r-00000");
		if (!fs.exists(pt1))
			pt1 = new Path(filename + "part-m-00000");
	}
        br1 = new BufferedReader(new InputStreamReader(fs.open(pt1)));   //read 1st line of file1
        header1 = br1.readLine();                                       //read header1
	br1.close();
        for (int i = 0; i < filenames.length; i++) {
            if (filenames[i] == null)
                continue;

            pt2 = new Path(filenames[i]);
            br2 = new BufferedReader(new InputStreamReader(fs.open(pt2))); //read 1st line of file2
            header2 = br2.readLine();                                   //read header2
	    br2.close();

            //split the two keys
            String[] keys = StringManipulation.Intersection(header1, header2, separator);
            String[] keys_0 = keys[0].split(separator);
            counter = 0;
            for (String str : keys_0) {
                if (Integer.parseInt(str) != -1) {
                    counter++;
                }
            }
            if (counter > max) {

                max = counter;
                max_single =i;
                multiply_size = (long) fs.getContentSummary(new Path(filenames[i])).getLength();
            } else if (counter == max) {

                multiply_size2 = (long) fs.getContentSummary(new Path(filenames[i])).getLength();

                if (multiply_size2 < multiply_size) {
                    max_single = i ;
                    multiply_size = multiply_size2;
                }
            }
        }
	String next_file = filenames[max_single];
	filenames[max_single] = null;

        return next_file;
    }


    // number of files is num_of_iterations + 1
    public static long number_of_iterations(String input_path, FileSystem fs) throws IOException
    {
	Path dir_pt = new Path(input_path);
	ContentSummary cs = fs.getContentSummary(dir_pt);
	long fileCount = cs.getFileCount();
	System.out.println(fileCount + " <---- NUM OF FILES");
	return fileCount - 1;
    }

    public static String[] create_filenames(String input_path, long fileCount)
    {	input_path = StringManipulation.adjust_input(input_path);
	String[] filenames=new String[(int) fileCount];
	for (int i=0 ;i<fileCount ; i++)
		filenames[i] = input_path + (i+1);
	return filenames;
		
    }

    public static boolean compare_files(String file1, String file2, FileSystem fs) throws IOException
    {
	long size1, size2;
	Path pt1 = new Path(file1);
	Path pt2 = new Path(file2);
	size1  = (long) fs.getContentSummary(pt1).getLength();
	size2  = (long) fs.getContentSummary(pt2).getLength();
	if (size1 >= size2)
		return true;
	else
		return false;

    }

    public static void main(String[] args) throws IOException
    {
	if (args.length != 2)
	{	System.out.println("Give two paths");
		return;
	}
	Configuration conf = new Configuration();
	FileSystem fs = FileSystem.get(conf);

	System.out.println("First path is bigger than second path: " + compare_files(args[0], args[1], fs));

    }
}
