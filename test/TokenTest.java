import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
 
public class TokenTest {
 
	public static void main(String[] args) {
 		final int N = 4;
 		String ngram[] = new String[N];
 		boolean flag = false;
		BufferedReader br = null;
 
		try {
 
			String sCurrentLine;
 			int cnt = 0;
			br = new BufferedReader(new FileReader("query1.txt"));
 
			while ((sCurrentLine = br.readLine()) != null) {
				Tokenizer parser = new Tokenizer(sCurrentLine);
				while (parser.hasNext()) {
					String word = parser.next();
					System.out.println(word);
					ngram[cnt++] = new String(word);
					if (cnt==N) {
						cnt = 0;
						flag = true;
					}
					if (flag) {
						String ngramString = ngram[cnt];
						for (int i=1;i<N;i++) ngramString += " "+ngram[(cnt+i)%N];
						System.out.println(ngramString);
					}
				}
				//System.out.println(sCurrentLine);
			}
 
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (br != null)br.close();
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		}
 
	}
}