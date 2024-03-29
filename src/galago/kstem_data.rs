pub(crate) const DICT_RAW: &str = include_str!("kstem.data");
pub(crate) const EXCEPTION_WORDS: &[&str] = &[
    "aide",
    "bathe",
    "caste",
    "cute",
    "dame",
    "dime",
    "doge",
    "done",
    "dune",
    "envelope",
    "gage",
    "grille",
    "grippe",
    "lobe",
    "mane",
    "mare",
    "nape",
    "node",
    "pane",
    "pate",
    "plane",
    "pope",
    "programme",
    "quite",
    "ripe",
    "rote",
    "rune",
    "sage",
    "severe",
    "shoppe",
    "sine",
    "slime",
    "snipe",
    "steppe",
    "suite",
    "swinge",
    "tare",
    "tine",
    "tope",
    "tripe",
    "twine",
];

pub(crate) const DIRECT_CONFLATIONS: &[(&str, &str)] = &[
    ("aging", "age"),
    ("going", "go"),
    ("goes", "go"),
    ("lying", "lie"),
    ("using", "use"),
    ("owing", "owe"),
    ("suing", "sue"),
    ("dying", "die"),
    ("tying", "tie"),
    ("vying", "vie"),
    ("aged", "age"),
    ("used", "use"),
    ("vied", "vie"),
    ("cued", "cue"),
    ("died", "die"),
    ("eyed", "eye"),
    ("hued", "hue"),
    ("iced", "ice"),
    ("lied", "lie"),
    ("owed", "owe"),
    ("sued", "sue"),
    ("toed", "toe"),
    ("tied", "tie"),
    ("does", "do"),
    ("doing", "do"),
    ("aeronautical", "aeronautics"),
    ("mathematical", "mathematics"),
    ("political", "politics"),
    ("metaphysical", "metaphysics"),
    ("cylindrical", "cylinder"),
    ("nazism", "nazi"),
    ("ambiguity", "ambiguous"),
    ("barbarity", "barbarous"),
    ("credulity", "credulous"),
    ("generosity", "generous"),
    ("spontaneity", "spontaneous"),
    ("unanimity", "unanimous"),
    ("voracity", "voracious"),
    ("fled", "flee"),
    ("miscarriage", "miscarry"),
    ("appendices", "appendix"),
    ("babysitting", "babysit"),
    ("bater", "bate"),
    ("belying", "belie"),
    ("bookshelves", "bookshelf"),
    ("bootstrapped", "bootstrap"),
    ("bootstrapping", "bootstrap"),
    ("checksummed", "checksum"),
    ("checksumming", "checksum"),
    ("crises", "crisis"),
    ("dwarves", "dwarf"),
    ("eerily", "eerie"),
    ("housewives", "housewife"),
    ("midwives", "midwife"),
    ("oases", "oasis"),
    ("parentheses", "parenthesis"),
    ("scarves", "scarf"),
    ("synopses", "synopsis"),
    ("syntheses", "synthesis"),
    ("taxied", "taxi"),
    ("testes", "testicle"),
    ("theses", "thesis"),
    ("thieves", "thief"),
    ("vortices", "vortex"),
    ("wharves", "wharf"),
    ("wolves", "wolf"),
    ("yourselves", "yourself"),
];

pub(crate) const COUNTRY_NATIONALITY: &[(&str, &str)] = &[
    ("afghan", "afghanistan"),
    ("african", "africa"),
    ("albanian", "albania"),
    ("algerian", "algeria"),
    ("american", "america"),
    ("andorran", "andorra"),
    ("angolan", "angola"),
    ("arabian", "arabia"),
    ("argentine", "argentina"),
    ("armenian", "armenia"),
    ("asian", "asia"),
    ("australian", "australia"),
    ("austrian", "austria"),
    ("azerbaijani", "azerbaijan"),
    ("azeri", "azerbaijan"),
    ("bangladeshi", "bangladesh"),
    ("belgian", "belgium"),
    ("bermudan", "bermuda"),
    ("bolivian", "bolivia"),
    ("bosnian", "bosnia"),
    ("botswanan", "botswana"),
    ("brazilian", "brazil"),
    ("british", "britain"),
    ("bulgarian", "bulgaria"),
    ("burmese", "burma"),
    ("californian", "california"),
    ("cambodian", "cambodia"),
    ("canadian", "canada"),
    ("chadian", "chad"),
    ("chilean", "chile"),
    ("chinese", "china"),
    ("colombian", "colombia"),
    ("croat", "croatia"),
    ("croatian", "croatia"),
    ("cuban", "cuba"),
    ("cypriot", "cyprus"),
    ("czechoslovakian", "czechoslovakia"),
    ("danish", "denmark"),
    ("egyptian", "egypt"),
    ("equadorian", "equador"),
    ("eritrean", "eritrea"),
    ("estonian", "estonia"),
    ("ethiopian", "ethiopia"),
    ("european", "europe"),
    ("fijian", "fiji"),
    ("filipino", "philippines"),
    ("finnish", "finland"),
    ("french", "france"),
    ("gambian", "gambia"),
    ("georgian", "georgia"),
    ("german", "germany"),
    ("ghanian", "ghana"),
    ("greek", "greece"),
    ("grenadan", "grenada"),
    ("guamian", "guam"),
    ("guatemalan", "guatemala"),
    ("guinean", "guinea"),
    ("guyanan", "guyana"),
    ("haitian", "haiti"),
    ("hawaiian", "hawaii"),
    ("holland", "dutch"),
    ("honduran", "honduras"),
    ("hungarian", "hungary"),
    ("icelandic", "iceland"),
    ("indonesian", "indonesia"),
    ("iranian", "iran"),
    ("iraqi", "iraq"),
    ("iraqui", "iraq"),
    ("irish", "ireland"),
    ("israeli", "israel"),
    ("italian", "italy"),
    ("jamaican", "jamaica"),
    ("japanese", "japan"),
    ("jordanian", "jordan"),
    ("kampuchean", "cambodia"),
    ("kenyan", "kenya"),
    ("korean", "korea"),
    ("kuwaiti", "kuwait"),
    ("lankan", "lanka"),
    ("laotian", "laos"),
    ("latvian", "latvia"),
    ("lebanese", "lebanon"),
    ("liberian", "liberia"),
    ("libyan", "libya"),
    ("lithuanian", "lithuania"),
    ("macedonian", "macedonia"),
    ("madagascan", "madagascar"),
    ("malaysian", "malaysia"),
    ("maltese", "malta"),
    ("mauritanian", "mauritania"),
    ("mexican", "mexico"),
    ("micronesian", "micronesia"),
    ("moldovan", "moldova"),
    ("monacan", "monaco"),
    ("mongolian", "mongolia"),
    ("montenegran", "montenegro"),
    ("moroccan", "morocco"),
    ("myanmar", "burma"),
    ("namibian", "namibia"),
    ("nepalese", "nepal"),
    //("netherlands", "dutch"),
    ("nicaraguan", "nicaragua"),
    ("nigerian", "nigeria"),
    ("norwegian", "norway"),
    ("omani", "oman"),
    ("pakistani", "pakistan"),
    ("panamanian", "panama"),
    ("papuan", "papua"),
    ("paraguayan", "paraguay"),
    ("peruvian", "peru"),
    ("portuguese", "portugal"),
    ("romanian", "romania"),
    ("rumania", "romania"),
    ("rumanian", "romania"),
    ("russian", "russia"),
    ("rwandan", "rwanda"),
    ("samoan", "samoa"),
    ("scottish", "scotland"),
    ("serb", "serbia"),
    ("serbian", "serbia"),
    ("siam", "thailand"),
    ("siamese", "thailand"),
    ("slovakia", "slovak"),
    ("slovakian", "slovak"),
    ("slovenian", "slovenia"),
    ("somali", "somalia"),
    ("somalian", "somalia"),
    ("spanish", "spain"),
    ("swedish", "sweden"),
    ("swiss", "switzerland"),
    ("syrian", "syria"),
    ("taiwanese", "taiwan"),
    ("tanzanian", "tanzania"),
    ("texan", "texas"),
    ("thai", "thailand"),
    ("tunisian", "tunisia"),
    ("turkish", "turkey"),
    ("ugandan", "uganda"),
    ("ukrainian", "ukraine"),
    ("uruguayan", "uruguay"),
    ("uzbek", "uzbekistan"),
    ("venezuelan", "venezuela"),
    ("vietnamese", "viet"),
    ("virginian", "virginia"),
    ("yemeni", "yemen"),
    ("yugoslav", "yugoslavia"),
    ("yugoslavian", "yugoslavia"),
    ("zambian", "zambia"),
    ("zealander", "zealand"),
    ("zimbabwean", "zimbabwe"),
];

pub(crate) const SUPPLEMENT_DICT: &[&str] = &[
    "aids",
    "applicator",
    "capacitor",
    "digitize",
    "electromagnet",
    "ellipsoid",
    "exosphere",
    "extensible",
    "ferromagnet",
    "graphics",
    "hydromagnet",
    "polygraph",
    "toroid",
    "superconduct",
    "backscatter",
    "connectionism",
];

pub(crate) const PROPER_NOUNS: &[&str] = &[
    "abrams",
    "achilles",
    "acropolis",
    "adams",
    "agnes",
    "aires",
    "alexander",
    "alexis",
    "alfred",
    "algiers",
    "alps",
    "amadeus",
    "ames",
    "amos",
    "andes",
    "angeles",
    "annapolis",
    "antilles",
    "aquarius",
    "archimedes",
    "arkansas",
    "asher",
    "ashly",
    "athens",
    "atkins",
    "atlantis",
    "avis",
    "bahamas",
    "bangor",
    "barbados",
    "barger",
    "bering",
    "brahms",
    "brandeis",
    "brussels",
    "bruxelles",
    "cairns",
    "camoros",
    "camus",
    "carlos",
    "celts",
    "chalker",
    "charles",
    "cheops",
    "ching",
    "christmas",
    "cocos",
    "collins",
    "columbus",
    "confucius",
    "conners",
    "connolly",
    "copernicus",
    "cramer",
    "cyclops",
    "cygnus",
    "cyprus",
    "dallas",
    "damascus",
    "daniels",
    "davies",
    "davis",
    "decker",
    "denning",
    "dennis",
    "descartes",
    "dickens",
    "doris",
    "douglas",
    "downs",
    "dreyfus",
    "dukakis",
    "dulles",
    "dumfries",
    "ecclesiastes",
    "edwards",
    "emily",
    "erasmus",
    "euphrates",
    "evans",
    "everglades",
    "fairbanks",
    "federales",
    "fisher",
    "fitzsimmons",
    "fleming",
    "forbes",
    "fowler",
    "france",
    "francis",
    "goering",
    "goodling",
    "goths",
    "grenadines",
    "guiness",
    "hades",
    "harding",
    "harris",
    "hastings",
    "hawkes",
    "hawking",
    "hayes",
    "heights",
    "hercules",
    "himalayas",
    "hippocrates",
    "hobbs",
    "holmes",
    "honduras",
    "hopkins",
    "hughes",
    "humphreys",
    "illinois",
    "indianapolis",
    "inverness",
    "iris",
    "iroquois",
    "irving",
    "isaacs",
    "italy",
    "james",
    "jarvis",
    "jeffreys",
    "jesus",
    "jones",
    "josephus",
    "judas",
    "julius",
    "kansas",
    "keynes",
    "kipling",
    "kiwanis",
    "lansing",
    "laos",
    "leeds",
    "levis",
    "leviticus",
    "lewis",
    "louis",
    "maccabees",
    "madras",
    "maimonides",
    "maldive",
    "massachusetts",
    "matthews",
    "mauritius",
    "memphis",
    "mercedes",
    "midas",
    "mingus",
    "minneapolis",
    "mohammed",
    "moines",
    "morris",
    "moses",
    "myers",
    "myknos",
    "nablus",
    "nanjing",
    "nantes",
    "naples",
    "neal",
    "netherlands",
    "nevis",
    "nostradamus",
    "oedipus",
    "olympus",
    "orleans",
    "orly",
    "papas",
    "paris",
    "parker",
    "pauling",
    "peking",
    "pershing",
    "peter",
    "peters",
    "philippines",
    "phineas",
    "pisces",
    "pryor",
    "pythagoras",
    "queens",
    "rabelais",
    "ramses",
    "reynolds",
    "rhesus",
    "rhodes",
    "richards",
    "robins",
    "rodgers",
    "rogers",
    "rubens",
    "sagittarius",
    "seychelles",
    "socrates",
    "texas",
    "thames",
    "thomas",
    "tiberias",
    "tunis",
    "venus",
    "vilnius",
    "wales",
    "warner",
    "wilkins",
    "williams",
    "wyoming",
    "xmas",
    "yonkers",
    "zeus",
    "frances",
    "aarhus",
    "adonis",
    "andrews",
    "angus",
    "antares",
    "aquinas",
    "arcturus",
    "ares",
    "artemis",
    "augustus",
    "ayers",
    "barnabas",
    "barnes",
    "becker",
    "bejing",
    "biggs",
    "billings",
    "boeing",
    "boris",
    "borroughs",
    "briggs",
    "buenos",
    "calais",
    "caracas",
    "cassius",
    "cerberus",
    "ceres",
    "cervantes",
    "chantilly",
    "chartres",
    "chester",
    "connally",
    "conner",
    "coors",
    "cummings",
    "curtis",
    "daedalus",
    "dionysus",
    "dobbs",
    "dolores",
    "edmonds",
];
