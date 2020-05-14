use crate::HashMap;
use crate::HashSet;
use once_cell::sync::Lazy;
use std::collections::hash_map::Entry;

#[derive(PartialEq, Eq, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Document {
    text: String,
    terms: Vec<String>,
    term_char_begin: Vec<u32>,
    term_char_end: Vec<u32>,
    tags: Vec<Tag>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Tag {
    name: String,
    attributes: HashMap<String, String>,
    begin: u32,
    end: u32,
    char_begin: u32,
    char_end: u32,
}

impl Tag {
    fn position_order(&self, other: &Self) -> std::cmp::Ordering {
        match self.begin.cmp(&other.begin) {
            std::cmp::Ordering::Equal => other.end.cmp(&self.end),
            ord => ord
        }
    }
}

impl From<BeginTag> for Tag {
    fn from(tag: BeginTag) -> Self {
        Self { name: tag.name, attributes: tag.attributes, begin: tag.term_start, end: tag.term_start, char_begin: tag.byte_start, char_end: tag.byte_start }
    }
}
impl From<ClosedTag> for Tag {
    fn from(tag: ClosedTag) -> Self {
        let IntSpan(begin, end) = tag.terms;
        let IntSpan(char_begin, char_end) = tag.bytes;
        Self { name: tag.name, attributes: tag.attributes, begin, end, char_begin, char_end }
    }
}

pub enum Error {

}


fn ignored_tag(tag: &str) -> bool {
    match tag {
        "style" | "script" => true,
        _ => false
    } 
}

static SPLIT_CHARS: Lazy<Vec<bool>> = Lazy::new(|| {
    fn is_punct_char(ch: char) -> bool {
        match ch {
            ';' | '\"' | '&' | '/' | ':' | '!' | '#' |
            '?' | '$' | '%' | '(' | ')' | '@' | '^' |
            '*' | '+' | '-' | ',' | '=' | '>' | '<' | '[' |
            ']' | '{' | '}' | '|' | '`' | '~' | '_' => true,
            _ => false
        }
    }
    (0u8..=255).map(|n| n <= 32 || is_punct_char(n as char)).collect()
});


const MAX_TOKEN_LENGTH: usize = 100;

#[derive(Debug, PartialEq)]
struct BeginTag {
    name: String,
    attributes: HashMap<String, String>,
    byte_start: u32,
    term_start: u32,
}

impl BeginTag {
    fn new(name: String, attributes: HashMap<String, String>, byte_start: u32, term_start: u32) -> Self {
        Self {
            name, attributes, term_start, byte_start
        }
    }
}
#[derive(Debug, PartialEq)]
struct ClosedTag {
    name: String,
    attributes: HashMap<String, String>,
    bytes: IntSpan,
    terms: IntSpan,
}

impl ClosedTag {
    fn new(begin: BeginTag, end_bytes: u32, end_terms: u32) -> ClosedTag {
        ClosedTag {
            name: begin.name,
            attributes: begin.attributes,
            bytes: IntSpan(begin.byte_start, end_bytes),
            terms: IntSpan(begin.term_start, end_terms),
        }
    }
}

#[derive(Debug, PartialEq, Eq, Copy, Clone)]
struct IntSpan(u32, u32);

pub struct State {
    text: Vec<char>,
    tokens: Vec<String>,
    ignore_until: Option<String>,
    position: usize,
    start: usize,
    tokenize_tag_content: bool,
    open_tags: HashMap<String, Vec<BeginTag>>,
    closed_tags: Vec<ClosedTag>,
    token_positions: Vec<IntSpan>,
}

impl State {
    pub fn new(text: &str) -> Self {
        Self {
            text: text.chars().collect(),
            tokens: Vec::new(),
            ignore_until: None,
            position: 0,
            start: 0,
            tokenize_tag_content: true,
            open_tags: HashMap::default(),
            closed_tags: Vec::new(),
            token_positions: Vec::new(),
        }
    }

    pub fn parse(&mut self) {
        while self.position < self.text.len() {
            let c = self.text[self.position];
            let ord = c as usize;

            if c == '<' {
                if self.ignore_until.is_none() {
                    self.on_split();
                }
                self.on_start_bracket();
            } else if self.ignore_until.is_some() {
                continue;
            } else if c == '&' {
                self.on_ampersand();
            } else if self.tokenize_tag_content && ord < 256 && SPLIT_CHARS[ord]  {
                self.on_split();
            }

            self.position += 1;
        }

        if self.ignore_until.is_none() {
            self.on_split();
        }
    }

    pub fn into_document(self, tag_whitelist: HashSet<String>) -> Document {
        let terms = self.tokens;
        let mut term_char_begin = Vec::with_capacity(self.token_positions.len());
        let mut term_char_end = Vec::with_capacity(self.token_positions.len());
        for IntSpan(begin, end) in self.token_positions {
            term_char_begin.push(begin);
            term_char_end.push(end);
        }

        let mut tags = Vec::new();
        for (tag, still_open) in self.open_tags {
            if tag_whitelist.contains(&tag) {
                for only_opened in still_open {
                    tags.push(only_opened.into());
                }
            }
        }
        for tag in self.closed_tags {
            if tag_whitelist.contains(&tag.name) {
                tags.push(tag.into());
            }
        }

        // Sort by start.
        tags.sort_unstable_by(|lhs: &Tag, rhs: &Tag| lhs.position_order(rhs));

        Document {
            text: self.text.iter().collect(),
            terms,
            term_char_begin,
            term_char_end,
            tags,
        }
        
    }

    fn add_token(&mut self, token: String, start: usize, end: usize) {
        if token.len() <= 0 {
            return;
        }
        // Hacks to see if token is short enough: (at least rust skips the allocation here)
        if token.len() > MAX_TOKEN_LENGTH / 6 && token.as_bytes().len() >= MAX_TOKEN_LENGTH {
            return;
        }
        self.tokens.push(token);
        self.token_positions.push(IntSpan(start as u32, end as u32));
    }

    //
    // TODO: mine this for actual, like, unit-tests.
	// This method does three kinds of processing:
	// <ul>
	//  <li>If the token contains periods at the beginning or the end,
	//      they are removed.</li>
	//  <li>If the token contains single letters followed by periods, such
	//      as I.B.M., C.I.A., or U.S.A., the periods are removed.</li>
	//  <li>If, instead, the token contains longer strings of state.text with
	//      periods in the middle, the token is split into
	//      smaller tokens ("umass.edu" becomes {"umass", "edu"}).  Notice
	//      that this means ("ph.d." becomes {"ph", "d"}).</li>
	// </ul>
	// @param normalized The term containing dots.
	// @param start Start offset in outer document.
	// @param end End offset in outer document.
	///
    fn extract_terms_from_acronym(&mut self, normalized: String, orig_start: usize, orig_end: usize) {
        let mut input: Vec<char> = normalized.chars().collect();

        let mut start = 0;
        while input[start] == '.' {
            start += 1;
        }
        let mut end = orig_end;
        while input.last() == Some(&'.') {
            input.pop();
            end -= 1;
        }
        let relevant = &input[start..];
        let mut token: String = relevant.iter().collect();
        let start = start + orig_start;
        if token.contains('.') {
            let mut is_acronym = token.len() > 0;
            for (i, c) in token.chars().enumerate() {
                if i % 2 == 1 && c != '.' {
                    is_acronym = false;
                    break;
                }
            }
            if is_acronym {
                token = token.replace(".", "");
                self.add_token(token, start, end);
                return;
            } else {
                let mut s = 0;
                for (e, c) in relevant.iter().cloned().enumerate() {
                    if c == '.' {
                        if e - s > 1 {
                            let sub_token: String = relevant[s..e].iter().collect();
                            self.add_token(sub_token, start + s, start + e);
                        }
                        s = e + 1;
                    }
                }
                if relevant.len() - s > 0 {
                    let sub_token: String = relevant[s..].iter().collect();
                    self.add_token(sub_token, start + s, end); 
                }
            }
        }
        self.add_token(token,  start, end)
    }

    fn process_and_add_token(&mut self, start: usize, end: usize) {
        let token: Vec<char> = self.text[self.start..self.position].iter().cloned().collect();

        match check_token_status(&token) {
            StringStatus::Clean => self.add_token(token.iter().collect(), start, end),
            StringStatus::NeedsSimpleFix => self.add_token(normalize_simple(&token), start, end),
            StringStatus::NeedsComplexFix => self.add_token(normalize_complex(&token), start, end),
            StringStatus::NeedsAcronymProcessing => self.extract_terms_from_acronym(normalize_complex(&token), start, end),
        };
    }

    fn on_split(&mut self) { 
        // Consume word if non-zero:
        if self.position > self.start {
            self.process_and_add_token(self.start, self.position);
        }
        // Move past characters we've consumed.
        self.start = self.position+1;
    }
    fn on_start_bracket(&mut self) {
        match self.text.get(self.position + 1) {
            None => self.end_parsing(),
            Some('/') => self.parse_end_tag(),
            Some('!') => self.skip_comment(),
            Some('?') => self.skip_processing_instruction(),
            Some(_) => self.parse_begin_tag(),
        }
        self.start = self.position + 1;
    }

    fn on_ampersand(&mut self) {
        self.on_split();
        // Lookahead, unbounded :(
        // Our goal here is fidelity over efficiency.
        for i in self.position+1..self.text.len() {
            let c = self.text[i];
            if c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '#' {
				continue;
			}
			if c == ';' {
				self.position = i;
				self.start = i+1;
				return;
			}

			// not a valid escape sequence
			break;
        }
    }
    fn end_parsing(&mut self) { 
        self.position = self.text.len();
    }
    
    fn parse_begin_tag(&mut self) { 
        // 1. read the name, skipping the '<'
        let mut i = self.position + 1;

        while i < self.text.len() {
            let ch = self.text[i];
            if ch.is_whitespace() || ch == '>' {
                break;
            }
            i+=1;
        }
        
        // Must allocate here for to_lowercase.
        let tag_name: String = self.text[self.position+2..i].iter().flat_map(|c| c.to_lowercase()).collect();
        
        // 2. read attr pairs
        let non_space = index_of_non_space(&self.text, Some(i));
        if let Some(pos) = non_space {
            i = pos
        }
        let mut close_it = false;
        let mut attributes: HashMap<String, String> = HashMap::default();
        
        if let Some(tag_end) = self.index_of(">", i + 1) {
            while non_space.is_some() && i < tag_end {
                let start = index_of_non_space(&self.text, Some(i));
                // Detect end of tag:
                if let Some(start) = start {
                    if self.text[start] == '>' {
                        i = start;
                        break;
                    } else if self.text[start] == '/' && self.text.get(start+1) == Some(&'>') {
                        i = start + 1;
                        close_it = true;
                        break;
                    }
                }

                let end = index_of_end_attribute(&self.text, start, Some(tag_end));
                let equals = index_of_equals(&self.text, start, end);
                // try to find an equals sign
                if equals.is_none() || equals == start || equals == end {
                    // if there's no equals, try to move to the next thing
                    if end.is_none() {
                        i = tag_end;
                        break;
                    } else {
                        i = end.unwrap();
                        continue;
                    }
                }

                let equals = equals.unwrap();

                let start_key = start;
                let end_key = equals;

                let mut start_value = equals + 1;
                let end_value = end;

                if self.text[start_value] == '"' || self.text[start_value] == '\'' {
                    start_value += 1;
                }
                if start_key.is_none() || end_value.is_none() || start_value >= end_value.unwrap() || start_key.unwrap_or(0) >= end_key {
                    i = end.unwrap();
                    continue;
                }
                let start_key = start_key.unwrap();
                let mut end_value = end_value.unwrap();

                let key: String = self.text[start_key..end_key].iter().flat_map(|c| c.to_lowercase()).collect();
                let value: String = self.text[start_value..end_value].iter().collect();

                attributes.insert(key, value);

                if end_value >= self.text.len() {
                    self.end_parsing();
                    break;
                }
                if self.text[end_value] == '"' || self.text[end_value] == '\'' {
                    end_value += 1;
                }
                i = end_value;
            }
        }

        self.position = i;

        if !ignored_tag(&tag_name) {
            let tokenize_tag_content = attributes.get("tokenizetagcontent").map(|val| val.to_lowercase() == "true");
            let tag = BeginTag::new(tag_name.clone(), attributes, (self.position + 1) as u32, self.tokens.len() as u32);

            if ! close_it {
                self.open_tags.entry(tag_name).or_default().push(tag);

                if let Some(opt) = tokenize_tag_content {
                    self.tokenize_tag_content = opt;
                }
            } else {
                let closed_tag = ClosedTag::new(tag, self.position as u32, self.tokens.len() as u32);
                self.closed_tags.push(closed_tag);
            }
        } else if !close_it {
            self.ignore_until = Some(tag_name);
        }
    }
    fn parse_end_tag(&mut self) { 
		// 1. read name (skipping the </ part)
        let mut i = self.position + 2;

        while i < self.text.len() {
            let ch = self.text[i];
            if ch.is_whitespace() || ch == '>' {
                break;
            }
            i+=1;
        }

        // Must allocate here for to_lowercase.
        let tag_name: String = self.text[self.position+2..i].iter().flat_map(|c| c.to_lowercase()).collect();

        if self.ignore_until.is_some() && self.ignore_until.as_ref().unwrap() == &tag_name {
            self.ignore_until = None;
        }
        if self.ignore_until.is_none() {
            self.close_tag(tag_name);
        }
        while i < self.text.len() && self.text[i] == '>' {
            i += 1;
        }
        self.position = i;
    }
    fn close_tag(&mut self, tag_name: String) {
        if !self.open_tags.contains_key(&tag_name) {
            return;
        }
        match self.open_tags.entry(tag_name) {
            Entry::Occupied(mut entry) => {
                if let Some(begin_tag) = entry.get_mut().pop() {
                    let closed_tag = ClosedTag::new(begin_tag, self.position as u32, self.tokens.len() as u32);
                    self.closed_tags.push(closed_tag);

                    // switch out of Do not tokenize mode.
                    if !self.tokenize_tag_content {
                        self.tokenize_tag_content = true;
                    }
                }
            }
            Entry::Vacant(_) => {}
        }
    }

    fn index_of(&self, pattern: &str, start: usize) -> Option<usize> {
        let needle: Vec<char> = pattern.chars().collect();
        let here = start;
        let end = self.text.len() - needle.len();
        for i in here..end {
            let ch = self.text[i];
            if ch == needle[0] {
                if self.text[i..].starts_with(&needle) {
                    return Some(i);
                }
            }
        }
        None
    }
    /// Skip a HTML comment: <!-- ... -->
    fn skip_comment(&mut self) { 
        let here = &self.text[self.position..];
        if here.starts_with(&['<', '!', '-', '-']) {
            if let Some(found) = self.index_of("-->", self.position + 1) {
                self.position = found + 2;
            } else {
                self.end_parsing();
            }
        } else {
            if let Some(found) = self.index_of(">", self.position + 1) {
                self.position = found;
            } else {
                self.end_parsing();
            }
        }
    }
    // Skip XML processing instructions.
    fn skip_processing_instruction(&mut self) { 
        if let Some(found) = self.index_of("?>", self.position + 1) {
            self.position = found;
        } else {
            self.end_parsing();
        }
    }

}

#[derive(Copy, Clone,Ord, PartialOrd, Eq, PartialEq, Hash)]
enum StringStatus {
    Clean,
    NeedsSimpleFix,
    NeedsComplexFix,
    NeedsAcronymProcessing,
}
fn check_token_status(token: &[char]) -> StringStatus {
    let mut status = StringStatus::Clean;

    for c in token.iter().cloned() {
        let is_ascii_lowercase = c >= 'a' && c <= 'z';
        let is_ascii_digit = c >= '0' && c <= '9';
        if is_ascii_lowercase || is_ascii_digit {
            continue;
        }
        let is_ascii_upper = c >= 'A' && c <= 'Z';
        let is_period = c == '.';
        let is_apostrophe = c == '\'';
        if (is_ascii_upper || is_apostrophe) && status == StringStatus::Clean {
            status = StringStatus::NeedsSimpleFix;
        } else if !is_period {
            status = StringStatus::NeedsComplexFix;
        } else {
            status = StringStatus::NeedsAcronymProcessing;
            break;
        }
    }

    status
}

fn normalize_simple(token: &[char]) -> String {
    let mut keep = String::with_capacity(token.len());

    for c in token.iter().cloned() {
        let is_ascii_upper = c >= 'A' && c <= 'Z';
        let is_apostrophe = c == '\'';

        if is_ascii_upper {
            keep.push(c.to_ascii_lowercase());
        } else if is_apostrophe {
            continue;
        } else {
            keep.push(c);
        }
    }

    keep
}
fn normalize_complex(token: &[char]) -> String {
    // ...lol, need to pick up Java locale?
    // Best-case-scenario: this does some form of unicode lowercasing.
    normalize_simple(token).to_lowercase()
}

fn index_of_non_space(text: &[char], start: Option<usize>) -> Option<usize> {
    if start.is_none() {
        return None;
    }
    for i in start.unwrap()..text.len() {
        if !text[i].is_whitespace() {
            return Some(i);
        }
    }
    None
}
fn index_of_equals(text: &[char], start: Option<usize>, end: Option<usize>) -> Option<usize> {
    if start.is_none() || end.is_none() {
        return None;
    }
    let start = start.unwrap();
    let end = end.unwrap();
    assert!(start > 0 && start < text.len());
    assert!(end > 0 && end <= text.len());
    for i in start..end {
        if text[i] == '=' {
            return Some(i);
        }
    }
    return None;
}
fn index_of_end_attribute(text: &[char], start: Option<usize>, end: Option<usize>) -> Option<usize> {
    if start.is_none() || end.is_none() {
        return None;
    }
    let start = start.unwrap();
    let end = end.unwrap();

    let mut in_quote = false;
    let mut last_escape = false;
    assert!(start > 0 && start < text.len());
    assert!(end > 0 && end < text.len());
    for i in start..=end {
        let c = text[i];
        if (c == '"' || c == '\'') && !last_escape {
            in_quote = !in_quote;
            if !in_quote {
                return Some(i);
            }
        } else if !in_quote && (c.is_whitespace() || c == '>') {
            return Some(i);
        } else if c == '\\' && !last_escape {
            last_escape = true;
        } else {
            last_escape = false;
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tokenize(text: &str) -> Document {
        let mut tokenizer = State::new(text);
        tokenizer.parse();
        tokenizer.into_document(HashSet::default())
    }

    #[test]
    fn simple_splitting() {
        let expected: Document = serde_json::from_str(r#"
        {"name":null,"metadata":{},"text":"This is a bit of regular, tag-free English.","terms":["this","is","a","bit","of","regular","tag","free","english"],"termCharBegin":[0,5,8,10,14,17,26,30,35],"termCharEnd":[4,7,9,13,16,24,29,34,42],"tags":[]}
        "#).unwrap();
        let doc = tokenize("This is a bit of regular, tag-free English.");
        assert_eq!(expected, doc);

        let data = "This is a bit of regular, tag-free English.<!-- comments are skipped -->";
        let doc = tokenize(data);
        assert_eq!(expected.terms, doc.terms);
    }
    
    #[test]
    fn simple_escapes() {
        let expected: Document = serde_json::from_str(r#"
        {"name":null,"metadata":{},"text":"&gt;home&lt;","terms":["home"],"termCharBegin":[4],"termCharEnd":[8],"tags":[]}
        "#).unwrap();
        let data = "&gt;home&lt;";
        let mut tokenizer = State::new(data);
        tokenizer.parse();
        let doc = tokenizer.into_document(HashSet::default());
        assert_eq!(expected, doc);
    }
    
    #[test]
    fn simple_splitting_tags() {
        let expected: Document = serde_json::from_str(r#"
        {"name":null,"metadata":{},"text":"This is a bit of <i>regular</i>, <a href=\"foo\">tagged</a> English.","terms":["this","is","a","bit","of","regular","tagged","english"],"termCharBegin":[0,5,8,10,14,20,47,58],"termCharEnd":[4,7,9,13,16,27,53,65],"tags":[]}
        "#).unwrap();
        let data = "This is a bit of <i>regular</i>, <a href=\"foo\">tagged</a> English.";
        let mut tokenizer = State::new(data);
        tokenizer.parse();
        let doc = tokenizer.into_document(HashSet::default());
        assert_eq!(expected, doc);
    }
}