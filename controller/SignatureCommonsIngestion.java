import java.io.File;
import java.io.FileInputStream;
import java.util.HashMap;
import java.util.Scanner;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SignatureCommonsDataIngestion {
  public static void main(String[] args) throws Exception {
    System.out.println(parse_file(new FileInputStream(new File("test.csv"))));
  }

  private static HashMap<String, Object> slice(Object left, Object right) {
    HashMap<String, Object> ret = new HashMap<String, Object>();
    ret.put("left", left);
    ret.put("right", right);
    return ret;
  }
  
  private static HashMap<String, Object> resolve_slice(Object s, Object l) throws Exception {
    if (s == null) {
      return slice(0, l);
    } else if (s instanceof Integer) {
      if ((int)s < 0) {
        return slice((int)l + (int)s, (int)l + (int)s + 1);
      } else {
        return slice((int)s, (int)s + 1);
      }
    } else if (s instanceof HashMap<?, ?>) {
      return slice(
        ((HashMap<?,?>)s).get("left") == null ? 0 : resolve_slice(((HashMap<?,?>)s).get("left"), l).get("left"),
        ((HashMap<?,?>)s).get("right") == null ? l : resolve_slice(((HashMap<?,?>)s).get("right"), l).get("left")
      );
    }
    throw new Exception("Cannot determine slice type");
  }
  
  private static Object[][] matrix_slice(Object[][] M, Object X_slice, Object Y_slice) throws Exception {
    HashMap<String, Object> X = resolve_slice(X_slice, M[0].length);
    HashMap<String, Object> Y = resolve_slice(Y_slice, M.length);
  
    int j = 0;
    Object[][] M_ = new Object[(int)Y.get("right") - (int)Y.get("left")][(int)X.get("right") - (int)X.get("left")];
    for (int y = (int)Y.get("left"); y < (int)Y.get("right"); y++) {
      int i = 0;
      for (int x = (int)X.get("left"); x < (int)X.get("right"); x++) {
        M_[j][i] = M[y][x];
        i++;
      }
      j++;
    }
    return M_;
  }
  
  private static Object[] matrix_flatten(Object[][] M) {
    int X = M[0].length;
    int Y = M.length;
  
    int i = 0;
    Object[] M_ = new Object[X * Y];
    for (int y = 0; y < Y; y++) {
      for (int x = 0; x < X; x++) {
        M_[i++] = M[y][x];
      }
    }
  
    return M_;
  }
  
  private static Object[][] matrix_transpose(Object[][] M) {
    int X = M[0].length;
    int Y = M.length;
  
    int j = 0;
    Object[][] M_ = new Object[X][Y];
    for (int y = 0; y < Y; y++) {
      int i = 0;
      for (int x = 0; x < X; x++) {
        M_[j][i] = M[x][y];
        i++;
      }
      j++;
    }
  
    return M_;
  }
  
  public static int count_first_na(Object[] L) throws Exception {
    for (int i = 0; i < L.length; i++) {
      if (L[i] != null)
        return i;
    }
    throw new Exception("NaNs not identified");
  }
  
  public static HashMap<Object, Object> dictzip(Object[] header, Object[] data) {
    HashMap<Object, Object> D = new HashMap<Object, Object>();
    for (int i = 0; i < Math.min(header.length, data.length); i++) {
      D.put(header[i], data[i]);
    }
    return D;
  }
  
  public static Vector<HashMap<String, Object>> parse(Object[][] matrix) throws Exception {
    Vector<HashMap<String, Object>> yield = new Vector<HashMap<String, Object>>();

    int border_x = count_first_na(matrix[0]);
    int border_y = count_first_na(matrix_flatten(matrix_slice(matrix, 0, null)));
  
    if (border_y <= 0 || border_x <= 0) {
      throw new Exception("Invalid formatting");
    }
  
    Object[] header_x = matrix_flatten(matrix_slice(matrix, border_x, slice(null, border_y + 1)));
    Object[] header_y = matrix_flatten(matrix_slice(matrix, slice(null, border_x + 1), border_y));
  
    for (int y = border_y + 1; y < matrix.length; y++) {
      for (int x = border_x + 1; x < matrix[0].length; x++) {
        HashMap<String, Object> root = new HashMap<String, Object>();

        HashMap<Object, Object> meta = new HashMap<Object, Object>();
        meta.putAll(
          dictzip(
            header_x,
            matrix_flatten(matrix_slice(matrix, x, slice(null, border_y + 1)))
          )
        );
        meta.putAll(
          dictzip(
            header_y,
            matrix_flatten(matrix_slice(matrix, slice(null, border_x + 1), y))
          )
        );

        root.put("meta", meta);
        root.put("data", matrix[y][x]);

        yield.add(root);
      }
    }

    return yield;
  }
  
  public static Object[] parse_line(String line) {
    Vector<Object> yield = new Vector<Object>();

    Pattern line_re = Pattern.compile("(^((\"([^\"]*)\")|([^,]*)),)|(((\"([^\"]*)\")|([^,]*)),)|(((\"([^\"]+)\")|([^,]+))$)");
    Matcher line_matcher = line_re.matcher(line);

    while (line_matcher.find()) {
      Object r = line_matcher.group(2);
      if (r == null)
        r = line_matcher.group(7);
      if (r == null)
        r = line_matcher.group(12);
      if (((String)r).equals(""))
        r = null;

      // Parse as float if possible
      try {
        r = Float.parseFloat((String)r);
      } catch(Exception e) {
      }

      yield.add(r);
    }
    // Hack to fix not-quite-perfect regex pattern
    if (line.charAt(line.length() - 1) == ',') {
      yield.add(null);
    }

    return yield.toArray();
  }

  public static Object[][] parse_csv(FileInputStream data) {
    Vector<Object[]> matrix = new Vector<Object[]>();
    
    Scanner scanner = new Scanner(data);
    scanner.useDelimiter(Pattern.compile("[\\n\\r]"));

    String line = scanner.nextLine();
    int width = line.length();
    while (scanner.hasNextLine()) {
      matrix.add(parse_line(line));
      line = scanner.nextLine();
    }
    int height = matrix.size();

    scanner.close();

    return matrix.toArray(new Object[height][width]);
  }
  
  public static Vector<HashMap<String, Object>> parse_file(FileInputStream data) throws Exception {
    return parse(parse_csv(data));
  }
}
