//! This module is a direct port of Galago's Krovetz Stemmer.
//! Which was a direct port of Bob Krovetz' kstem stemmer.
//! ..by Sergio Guzman-Lara.
//! All I did was adapt the data ingestion and code to Rust.
// BSD License (http://lemurproject.org/galago-license)
/*
Copyright 2003,
Center for Intelligent Information Retrieval,
University of Massachusetts, Amherst.
All rights reserved.

Redistribution and use in source and binary forms, with or without modification,
are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice, this
list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice,
this list of conditions and the following disclaimer in the documentation
and/or other materials provided with the distribution.

3. The names "Center for Intelligent Information Retrieval" and
"University of Massachusetts" must not be used to endorse or promote products
derived from this software without prior written permission. To obtain
permission, contact info@ciir.cs.umass.edu.

THIS SOFTWARE IS PROVIDED BY UNIVERSITY OF MASSACHUSETTS AND OTHER CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDERS OR CONTRIBUTORS BE
LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE
GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
SUCH DAMAGE.
 */

use crate::HashMap;
use once_cell::sync::Lazy;

// Familiar from our TagTokenizer port.
const MAX_WORD_LEN: usize = 100;

use super::kstem_data;

struct KStemState<'t> {
    word: Vec<char>,
    lookup_buffer: String,
    original: &'t str,
    /// Index of final letter in stem (within word)
    j: usize,
}

pub fn stem(token: &str) -> String {
    let mut state = KStemState {
        // utf-32 vec: for ease of translation.
        word: Vec::new(),
        // utf-8 vec: for hashmap lookups.
        lookup_buffer: String::new(),
        original: token,
        j: 0,
    };
    state.stem()
}

impl<'t> KStemState<'t> {
    fn stem(&mut self) -> String {
        let k = self.original.chars().count();
        if k <= 1 || k >= (MAX_WORD_LEN - 1) {
            return self.original.to_lowercase();
        } else {
            self.word.reserve(self.original.len());
            for ch in self.original.chars().flat_map(|ch| ch.to_lowercase()) {
                if !ch.is_ascii_alphabetic() {
                    return self.original.to_lowercase();
                }
            }
        }
        // See if this is in our table:
        if let Some(found) = self.check_done() {
            return found;
        }

        // Try all endings sequentially and break when found:
        self.plural();
        if let Some(found) = self.check_done() {
            return found;
        }

        self.past_tense();
        if let Some(found) = self.check_done() {
            return found;
        }

        self.word.iter().collect()
    }
    fn final_char(&self) -> Option<char> {
        self.word.last().cloned()
    }
    fn k(&self) -> usize {
        return self.word.len() - 1;
    }
    fn ends_in(&mut self, xs: &str) -> bool {
        let suffix: Vec<char> = xs.chars().collect();
        let r = self.word.len() - suffix.len();
        if suffix.len() > self.k() {
            return false;
        }

        let mut matches = true;
        for i in 0..suffix.len() {
            let lhs = suffix[i];
            let rhs = self.word[i];
            if lhs != rhs {
                matches = false;
                break;
            }
        }

        if matches {
            // index of character before suffix!
            self.j = r - 1;
        } else {
            // index of character before end!
            self.j = self.k();
        }
        return matches;
    }

    fn check_done(&mut self) -> Option<String> {
        self.lookup_buffer.clear();
        self.lookup_buffer.extend(&self.word);
        if let Some(entry) = DICTIONARY.get(self.lookup_buffer.as_str()) {
            return match entry {
                DictEntry::Regular => Some(self.lookup_buffer.clone()),
                DictEntry::Special { root, .. } => Some(root.to_string()),
            };
        }
        None
    }
    fn lookup(&mut self) -> bool {
        self.lookup_buffer.clear();
        self.lookup_buffer.extend(&self.word);
        DICTIONARY.get(self.lookup_buffer.as_str()).is_some()
    }
    fn entry(&mut self) -> Option<&DictEntry> {
        self.lookup_buffer.clear();
        self.lookup_buffer.extend(&self.word);
        DICTIONARY.get(self.lookup_buffer.as_str())
    }

    fn set_suffix(&mut self, s: &str) {
        self.word.truncate(self.j + 1);
        self.word.extend(s.chars());
    }
    fn plural(&mut self) {
        if self.final_char() != Some('s') {
            return;
        }
        if self.ends_in("ies") {
            self.word.truncate(self.j + 1);
            if self.lookup() {
                return;
            }
            self.word.push('s');
            self.set_suffix("y");
            return;
        }
        if self.ends_in("es") {
            /* try just removing the "s" */
            self.word.truncate(self.j + 2);

            /*
             * note: don't check for exceptions here. So, `aides' -> `aide', but `aided' ->
             * `aid'. The exception for double s is used to prevent crosses -> crosse. This
             * is actually correct if crosses is a plural noun (a type of racket used in
             * lacrosse), but the verb is much more common
             */
            if (self.j > 0)
                && (self.lookup())
                && !((self.word[self.j] == 's') && (self.word[self.j - 1] == 's'))
            {
                return;
            }

            /* try removing the "es" */
            self.word.truncate(self.j + 1);
            if self.lookup() {
                return;
            }

            /* the default is to retain the "e" */
            self.word.push('e');
            return;
        }
    } // plural

    fn past_tense(&mut self) {
        // Handle words less than 5 letters with a direct mapping This prevents (fled -> fl).
        if self.word.len() <= 4 {
            return;
        }

        if self.ends_in("ied") {
            self.word.truncate(self.j + 3);
            if self.lookup()
            /* we almost always want to convert -ied to -y, but */
            {
                return; /* this isn't true for short words (died->die) */
            }
            /* I don't know any long words that this applies to, */
            self.word.push('d'); /* but just in case... */
            self.set_suffix("y");
            return;
        }

        /* the vowelInStem() is necessary so we don't stem acronyms */
        if self.ends_in("ed") && self.vowel_in_stem() {
            /* see if the root ends in `e' */
            self.word.truncate(self.j + 2);

            if let Some(entry) = self.entry() {
                if !entry.exception() {
                    return;
                }
            }

            /* try removing the "ed" */
            self.word.truncate(self.j + 1);
            if self.lookup() {
                return;
            }

            /*
             * try removing a doubled consonant. if the root isn't found in the dictionary,
             * the default is to leave it doubled. This will correctly capture `backfilled'
             * -> `backfill' instead of `backfill' -> `backfille', and seems correct most of
             * the time
             */

            if self.double_consonant(self.k()) {
                self.word.truncate(self.k());
                if self.lookup() {
                    return;
                }
                self.word.push(*self.word.last().unwrap());
                return;
            }

            /* if we have a `un-' prefix, then leave the word alone */
            /* (this will sometimes screw up with `under-', but we */
            /* will take care of that later) */

            if self.word[..2] == ['u', 'n'] {
                self.word.push('e');
                self.word.push('d');
                return;
            }

            /*
             * it wasn't found by just removing the `d' or the `ed', so prefer to end with
             * an `e' (e.g., `microcoded' -> `microcode').
             */

            self.word.truncate(self.j + 1);
            self.word.push('e');
            return;
        }
    } // past_tense

    fn vowel_in_stem(&mut self) -> bool {
        for i in 0..self.j + 1 {
            if self.is_vowel(i) {
                return true;
            }
        }
        return false;
    }
    fn double_consonant(&mut self, position: usize) -> bool {
        if position < 1 {
            return false;
        }
        if self.word[position] != self.word[position - 1] {
            false
        } else {
            self.is_consonant(position - 1)
        }
    }

    fn is_vowel(&mut self, position: usize) -> bool {
        !self.is_consonant(position)
    }
    // Recursion!
    fn is_consonant(&mut self, position: usize) -> bool {
        let ch = self.word[position];
        match ch {
            'a' | 'e' | 'i' | 'o' | 'u' => false,
            'y' => {
                if position == 0 {
                    true
                } else {
                    !self.is_consonant(position - 1)
                }
            }
            _ => true,
        }
    } // is_consonant
}

enum DictEntry {
    Special { root: &'static str, exception: bool },
    Regular,
}
impl DictEntry {
    fn exception(&self) -> bool {
        match self {
            Self::Special { exception, .. } => *exception,
            _ => false,
        }
    }
}

static DICTIONARY: Lazy<HashMap<&str, DictEntry>> = Lazy::new(|| {
    let mut builder: HashMap<&str, DictEntry> = HashMap::default();
    // About this many exceptions:
    builder.reserve(30_000);

    for e in kstem_data::EXCEPTION_WORDS.iter() {
        let entry = DictEntry::Special {
            root: e,
            exception: true,
        };
        builder.insert(e, entry);
    }
    for (lhs, rhs) in kstem_data::DIRECT_CONFLATIONS.iter() {
        let entry = DictEntry::Special {
            root: rhs,
            exception: true,
        };
        builder.insert(lhs, entry);
    }
    for (nationality, country) in kstem_data::COUNTRY_NATIONALITY.iter() {
        let entry = DictEntry::Special {
            root: country,
            exception: true,
        };
        builder.insert(nationality, entry);
    }

    for entry in kstem_data::DICT_RAW.split_ascii_whitespace() {
        builder.insert(entry, DictEntry::Regular);
    }

    for entry in kstem_data::SUPPLEMENT_DICT {
        builder.insert(entry, DictEntry::Regular);
    }

    for entry in kstem_data::PROPER_NOUNS {
        builder.insert(entry, DictEntry::Regular);
    }

    builder
});

#[cfg(test)]
mod tests {
    // thanks sjh for the tests:
    use super::*;
    use crate::galago::tokenizer::tokenize_to_terms;

    const DOC: &str = r#"
        Call me Ishmael. Some years ago never mind how long precisely 
        having little or no money in my purse, and nothing particular to interest 
        me on shore, I thought I would sail about a little and see the watery part 
        of the world. It is a way I have of driving off the spleen and regulating 
        the circulation. Whenever I find myself growing grim about the mouth; 
        whenever it is a damp, drizzly November in my soul; whenever I find myself 
        involuntarily pausing before coffin warehouses, and bringing up the rear of 
        every funeral I meet; and especially whenever my hypos get such an upper 
        hand of me, that it requires a strong moral principle to prevent me from 
        deliberately stepping into the street, and methodically knocking people's 
        hats off then, I account it high time to get to sea as soon as I can. This 
        is my substitute for pistol and ball. With a philosophical flourish Cato 
        throws himself upon his sword; I quietly take to the ship. There is nothing 
        surprising in this. If they but knew it, almost all men in their degree, 
        some time or other, cherish very nearly the same feelings towards the ocean 
        with me.
    "#;

    const EXPECTED: &str = r#"
        call me ishmael some years ago never mind how 
        long precisely have little or no money in my
        purse and nothing particular to interest me on shore i
        thought i would sail about a little and see the
        watery part of the world it is a way i
        have of driving off the spleen and regulate the circulation
        whenever i find myself grow grim about the mouth whenever
        it is a damp drizzle november in my soul whenever
        i find myself involuntary pause before coffin warehouse and bring
        up the rear of every funeral i meet and especially
        whenever my hypo get such an upper hand of me
        that it require a strong moral principle to prevent me
        from deliberate step into the street and methodical knock people
        hat off then i account it high time to
        get to sea as soon as i can this is
        my substitute for pistol and ball with a philosophical flourish
        cato throw himself upon his sword i quiet take to
        the ship there is nothing surprising in this if they
        but knew it almost all men in their degree some
        time or other cherish very nearly the same feelings towards
        the ocean with me
    "#;

    #[test]
    fn test_a_book_about_a_fish() {
        let terms = tokenize_to_terms(DOC);
        let expected: Vec<&str> = EXPECTED.trim().split_ascii_whitespace().collect();

        for (lhs, rhs) in terms.iter().zip(expected.iter()) {
            if lhs != rhs {
                panic!("Stemmer TODO: {} -> {}", lhs, rhs);
            }
        }
    }
}
