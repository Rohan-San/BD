package employee;
import java.io.*;
import java.util.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.io.*;

class reducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
  public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
    double sum = 0.0;
    int count = 0;
    List<String> allValues = new ArrayList<>(); // Temporary list to hold all values
    while (values.hasNext()) {
        String value = values.next().toString();
        allValues.add(value); // Add value to the temporary list
        if (value.startsWith("S:")) {
            double salary = Double.parseDouble(value.substring(2));
            sum += salary;
            count++;
        }
    }
    double averageSalary = sum / count;
    output.collect(new Text(key.toString() + " Average Salary"), new Text(Double.toString(averageSalary)));
    List<String> employeeNames = new ArrayList<>();
    // Iterate over the temporary list to collect employee names with salary > average
    for (String value : allValues) {
      if (value.startsWith("N:")) {
          // Extract employee name
          String[] parts = value.split(", ");
          String employeeName = parts[0].substring(2); // Extracting the employee name from "N:EmployeeName"
          // Extract and parse salary from the second part
          double salary = Double.parseDouble(parts[1].substring(2)); // Extracting the salary from "S:Salary"
          if (salary > averageSalary) {
              employeeNames.add(employeeName);
          }
       }
    }
    // Output employee names with salary greater than the average
    for (String employee : employeeNames) {
        output.collect(new Text(key.toString() + " Employee"), new Text(employee));
    }
  }
}
