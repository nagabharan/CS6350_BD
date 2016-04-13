import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;

public class FORMAT_GENRE extends EvalFunc<String> {
	
	@Override
	public String exec(Tuple input) {
		try {
			if (input == null || input.size() == 0) {
				return null;
			}

			String genre = input.get(0).toString().trim();
			String[] genresArr = genre.split("\\|");
			
			if(genresArr.length==0) {
				return null;
			}
			StringBuilder output = new StringBuilder();

			int i = 0;
			for (; i < genresArr.length - 2; i++)
				output.append(i + 1 + ") " + genresArr[i] + ", ");

			if (i == genresArr.length - 2)
				output.append(i + 1 + ") " + genresArr[i++] + " & ");

			output.append(i + 1 + ") " + genresArr[i] + " ");

			return output.toString();
		} catch (ExecException ex) {
			System.out.println("Error: " + ex.toString());
		}

		return null;
	}
}