// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.parse;

import java.util.*;

/**
 * This is Galago's document class. It represents a sequence of tokens that have begin and end offsets back into
 * original text. It also supports SGML/HTML/XML-like tags which surround tokens and can therefore be mapped back into
 * the original text.
 * <p>
 * Traditionally the document has an internal numeric identifier, and an external human-readable name. The identifier is
 * assigned automatically during Galago's build process.
 * <p>
 * The document also has a Map&lt;String,String&gt; of metadata.
 */
public class Document  {
    /**
     * document data - these values are serialized
     */
    public String name;
    public Map<String, String> metadata;
    public String text;
    public List<String> terms;
    public List<Integer> termCharBegin = new ArrayList<>();
    public List<Integer> termCharEnd = new ArrayList<>();
    public List<Tag> tags;

    public Document() {
        metadata = new HashMap<>();
    }

    public Document(String externalIdentifier, String text) {
        this();
        this.name = externalIdentifier;
        this.text = text;
    }

    public Document(Document d) {
        this.name = d.name;
        this.metadata = new HashMap<>(d.metadata);
        this.text = d.text;
        this.terms = new ArrayList<>(d.terms);
        this.termCharBegin = new ArrayList<>(d.termCharBegin);
        this.termCharEnd = new ArrayList<>(d.termCharEnd);
        this.tags = new ArrayList<>(d.tags);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Identifier: ").append(name).append("\n");
        if (metadata != null) {
            sb.append("Metadata: \n");
            for (Map.Entry<String, String> entry : metadata.entrySet()) {
                sb.append("<");
                sb.append(entry.getKey()).append(",").append(entry.getValue());
                sb.append("> ");
            }
        }

        if (tags != null) {
            int count = 0;
            sb.append("\nTags: \n");
            for (Tag t : tags) {
                sb.append(count).append(" : ");
                sb.append(t.toString()).append("\n");
                count += 1;
            }
        }

        if (terms != null) {
            int count = 0;
            sb.append("\nTerm vector: \n");
            for (String s : terms) {
                sb.append(count).append(" : ");
                sb.append(s).append("\n");
                count += 1;
            }
        }

        if (text != null) {
            sb.append("\nText :").append(text);
        }
        sb.append("\n");

        return sb.toString();
    }
}

