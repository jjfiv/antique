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
    stem_it: bool,
    word: Vec<char>,
    lookup_buffer: String,
    original: &'t str,
    /// Index of final letter in stem (within word)
    j: usize,
}

pub fn stem(token: &str) -> String {
    let out = String::new();

    let mut state = KStemState {
        stem_it: true,
        // utf-32 vec: for ease of translation.
        word: Vec::new(),
        // utf-8 vec: for hashmap lookups.
        lookup_buffer: String::new(),
        original: token,
        j: 0,
    };
    state.stem()

    // if let Some(maybe_default) = DICTIONARY.get(token) {
    //     if let Some(entry) = maybe_default {
    //         if entry.exception {
    //             return token.into();
    //         } else {
    //             return entry.root.into();
    //         }
    //     }
    // }

    // out
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

    fn past_tense(&mut self) {}
}

enum DictEntry {
    Special { root: &'static str, exception: bool },
    Regular,
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
