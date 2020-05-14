// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.parse;

import org.lemurproject.galago.core.parse.tagtok.IntSpan;
import org.lemurproject.galago.core.parse.tagtok.TagTokenizerParser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * <p>
 * This class processes document text into tokens tha can be indexed.
 * </p>
 * <p>
 * The text is assumed to contain some HTML/XML tags. The tokenizer tries to
 * extract as much data as possible from each document, even if it is not well
 * formed (e.g. there are start tags with no ending tags). The resulting
 * document object contains an array of terms and an array of tags.
 * </p>
 *
 * This class is <strong>NOT</strong> threadsafe.
 * 
 * @author trevor
 */
public class TagTokenizer {
	public static final Logger log = Logger.getLogger(TagTokenizer.class.getName());
	public static HashSet<String> ignoredTags = new HashSet<>(Arrays.asList("script", "style"));

	protected List<Pattern> whitelist;
	public TagTokenizerParser state;

	public TagTokenizer() {
		this.init(Collections.emptyList());
	}

	public TagTokenizer(List<String> fields) {
		this.init(fields);
	}

	private void init(List<String> fields) {
		state = new TagTokenizerParser();

		whitelist = new ArrayList<>();
		// This has to come after we initialize whitelist.
		for (String value : fields) {
			assert (whitelist != null);
			addField(value);
		}
	}

	/** Register the fields that should be parsed and collected */
	public void addField(String f) {
		whitelist.add(Pattern.compile(f));
	}

	/**
	 * Resets parsing in preparation for the next document.
	 */
	public void reset() {
		state.reset();
	}

	/**
	 * Parses the state.text in the document.state.text attribute and fills in the
	 * document.terms and document.tags arrays.
	 *
	 */
	public void tokenize(Document document) {
		reset();
		assert (document != null);
		state.text = document.text;
		assert (state.text != null);

		try {
			state.parse();
			// Pull tag information into this document object.
			state.finishDocument(document, whitelist);
		} catch (Exception e) {
			log.log(Level.WARNING, "Parse failure: " + document.name, e);
		}

		assert (document.terms != null);
	}

	public ArrayList<IntSpan> getTokenPositions() {
		return state.tokenPositions;
	}

	public static void main(String[] args) throws IOException {
		ObjectMapper mapper = new ObjectMapper();
		TagTokenizer tok = new TagTokenizer();
		Document doc = new Document();

		LinkedList<String> params = new LinkedList<>(Arrays.asList(args));

		while (!params.isEmpty()) {
			String cmd = params.poll();
			if (cmd.equals("--file")) {
				String path = Objects.requireNonNull(params.poll(), "--file needs an argument!");
				doc.text = new String(Files.readAllBytes(Paths.get(path)), "UTF-8");
			} else if (cmd.equals("--tag")) {
				String tag = Objects.requireNonNull(params.poll(), "--file needs an argument!");
				tok.addField(tag);
			} else if (cmd.equals("--text")) {
				String inline = Objects.requireNonNull(params.poll(), "--text needs an argument!");
				doc.text = inline;
			} else {
				throw new RuntimeException("Unknown arg: "+cmd+" try: --file, --tag, --text");
			}
		}

		if (args[0].equals("--file")) {
			doc.text = new String(Files.readAllBytes(Paths.get(args[1])), "UTF-8");
		} else {
			doc.text = args[0];
		}
		tok.tokenize(doc);
		mapper.writeValue(System.out, doc);
	}
}
