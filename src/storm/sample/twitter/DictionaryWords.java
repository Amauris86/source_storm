package storm.sample.twitter;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.Logger;

/**
 * @author Permite cargar y gestionar los diferentes diccionarios de
 *         palabras (positivas, negativas).
 */
public class DictionaryWords {

	private Set<String> negativeWords;
	private Set<String> positiveWords;
	private static DictionaryWords _singleton;

	private DictionaryWords() {
		this.negativeWords = loadDictionary("/resources/neg-words.txt");
		this.positiveWords = loadDictionary("/resources/pos-words.txt");
	}

	/**
	 * Este metodo permite cargar la lista de palabras contenidas en el archivo
	 * definido en el argumento <code>type</code>. Retorna un conjunto de
	 * palabras.
	 * 
	 * @param type
	 *            Es la ruta del archivo que contiene la lista de palabras a ser
	 *            cargadas.
	 * @return El conjunto de palabras contenidas en el archivo definido.
	 */
	private Set<String> loadDictionary(String type) {
		Set<String> dict = new HashSet<String>();
		BufferedReader bf = null;
		try {
			bf = new BufferedReader(new InputStreamReader(this.getClass()
					.getResourceAsStream(type)));
			String line;
			while ((line = bf.readLine()) != null)
				dict.add(line);
			return dict;
		} catch (IOException ex) {
			Logger.getLogger(this.getClass()).error(
					"IO error while initializing " + type, ex);
			return null;
		} finally {
			try {
				if (bf != null)
					bf.close();
			} catch (IOException ex) {
			}
		}
	}

	/**
	 * @return La instancia de la clase <code>DictionaryWords</code>.
	 */
	private static DictionaryWords getDictionaryWords() {
		if (_singleton == null)
			_singleton = new DictionaryWords();
		return _singleton;
	}

	/**
	 * @return La instancia del conjunto de palabras negativas contenidas en el
	 *         archivo neg-words.txt
	 */
	public static Set<String> getNegativeWords() {
		return getDictionaryWords().negativeWords;
	}

	/**
	 * @return La instancia del conjunto de palabras positivas contenidas en el
	 *         archivo pos-words.txt
	 */
	public static Set<String> getPositiveWords() {
		return getDictionaryWords().positiveWords;
	}
}