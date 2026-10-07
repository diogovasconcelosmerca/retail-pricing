"""Local FMCG reporting taxonomy; exact source-path rules, no trained model.

English labels follow the requested category list. Non-FMCG, Tobacco and
Plant-Based Alternatives keep observed merchandise out of misleading groups.
See docs/silver-data-quality.md for boundaries and validation limitations.
"""

import hashlib
import re
import unicodedata

MAPPING_VERSION = "fmcg_v3"

CATEGORY_LABELS = {
    "fresh_produce": "Fresh Produce",
    "meat_poultry_seafood": "Meat, Poultry & Seafood",
    "dairy_eggs": "Dairy & Eggs",
    "bakery": "Bakery",
    "chilled_prepared": "Chilled & Prepared Foods",
    "frozen": "Frozen Foods",
    "pantry_cooking": "Pantry & Cooking",
    "breakfast_cereals": "Breakfast & Cereals",
    "confectionery_snacks": "Confectionery & Snacks",
    "non_alcoholic_beverages": "Non-Alcoholic Beverages",
    "coffee_tea": "Coffee & Tea",
    "alcoholic_beverages": "Alcoholic Beverages",
    "baby_care": "Baby Care",
    "personal_care": "Personal Care & Hygiene",
    "beauty_cosmetics": "Beauty & Cosmetics",
    "household_care": "Household Care",
    "household_supplies": "Household Paper & Supplies",
    "pet_care": "Pet Care",
    "health_wellness": "Health & Wellness",
    "unclassified": "Other / Unclassified",
    "non_fmcg": "Non-FMCG / General Merchandise",
    "tobacco": "Tobacco",
    "plant_based_alternatives": "Plant-Based Alternatives",
}

MERCHANDISE_LABELS_NL = {
    "fresh_produce": (
        "Aardappel, groente, fruit",
        "Groenten en fruit",
        "Groenten & fruit",
        "Verse groenten en fruit",
        "Verse groenten",
        "Groente",
        "Vers fruit",
        "Fruit",
        "Aardappelen",
        "Aardappelproducten",
    ),
    "meat_poultry_seafood": (
        "Colruyt-beenhouwerij",
        "Colruyt-vlees",
        "Beenhouwerij",
        "Vlees",
        "Vers vlees",
        "Gevogelte en konijn",
        "Kip",
        "Varkensvlees",
        "Rundvlees",
        "Gehakt, worst, burgers",
        "Charcuterie",
        "Vleeswaren",
        "Vis",
        "Vis en zeevruchten",
        "Vis en schaaldieren",
        "Gerookte vis",  # fmcg_v2
    ),
    "dairy_eggs": (
        "Zuivel",
        "Zuivel en kaas",
        "Zuivel, eieren",
        "Zuivel, eieren, boter",
        "Melkproducten en kaas",
        "Kaas",
        "Kazen",
        "Melk/Melkdrank",
        "Yoghurt",
        "Yoghurt en kwark",
        "Boter",
        "Boter en margarine",
        "Eieren",
        "Verse eieren",  # fmcg_v3
        "Zuiveldranken",
        "Houdbare melk en zuivel",
        "Verse melk en zuiveldrank",
        "Kaas voor op brood",
        "Kaas voor tussendoor",
        "Kaas voor de maaltijd",
        "Vla, pap en toetjes",
    ),
    "bakery": (
        "Bakkerij",
        "Bakkerij en banket",
        "Brood en banket",
        "Brood & patisserie",
        "Brood",
        "Afbakbrood",
        "Voorverpakt brood",
        "Patisserie",
    ),
    "chilled_prepared": (
        "Bereide maaltijden",
        "Kant-en-klaar",
        "Traiteur & bereide maaltijden",
        "Salades, pizza, maaltijden",
        "Opwarmbare gerechten",
        "Verse kant-en-klaar maaltijden, salades",
        "Verse kant- en-klaar maaltijden",
        "Pizza",
        "Tapas, borrelhapjes",
        "Aperitiefhapjes",
    ),
    "frozen": (
        "Diepvriesgroenten",
        "Diepvriesvis",
        "Diepvriesmaaltijden/Pizza's/Snacks",
        "Diepvriesmaaltijden/Snacks",
        "IJs",
        "Roomijs",
        "IJs/Diepvriesdesserten",
        "Ijs & desserten",
        "Ijs & sorbets",
        "Diepvries",
        "Diepvries groenten, soepen en kruiden",
        "Diepvries natuurlijke groenten",
        "Diepvries snacks",
        "Diepvriesfrieten",
        "Diepvries pizza",
        "Diepvriesbroodjes",
        "Diepvries maaltijden",
        "Diepvries fruit",
        "Diepvries groenten",
        "Diepvries groente",
        "Diepvries vis",
        "Diepvriesvruchten",
        "Diepvriesgroenten- en fruit",
    ),
    "pantry_cooking": (
        "Deegwaren",
        "Pasta",
        "Rijst",
        "Pasta, rijst, noedels",
        "Pasta, rijst en andere zetmeelproducten",
        "Bloem/Meel/Bakken",
        "Zelf bakken",
        "Bakken",
        "Bakproducten",
        "Desserts, suiker & bloem",
        "Dessertbereidingen",
        "Kruiden",
        "Kruiden/ specerijen",
        "Kruiden, specerijen",
        "Peper/zout/kruiden/specerijen",
        "Olie en azijn",
        "Olie/Azijn/Vetten",
        "Sauzen",
        "Sausen (warm/koud)",
        "Koude sauzen",
        "Warme sauzen",
        "Sauzen, dressing, tafelzuren",
        "Bouillon/Smaakmakers",
        "Sauzen, smaakmakers & kookhulp",
        "Groenteconserven",
        "Groenten in conserve",
        "Fruitconserven",
        "Fruit in conserve en compote",
        "Vleesconserven",
        "Visconserven",
        "Vis in conserve",
        "Soepen",
        "Soepen & croutons",
        "Soep, soepverrijking",
        "Maaltijdpakketten, mixen",
        "Maaltijdboxen",
        "Zoetstof",
        "Zoetstoffen",
        "Suiker",
        "Suikers",
        "Bakmeel",
        "(Zelfrijzend) bakmeel",
        "Zoet broodbeleg",
        "Zoet boterhambeleg",
        "Pindakaas, hagelslag, jam",
        "Maaltijdmixen",
        "Maaltijdpakketten",
        "Pannenkoekenmix",
        "Pannenkoekenmeel",
        "Vruchtenconserven",
        "Dips, sauzen en vinaigrette",
        "Confituur",  # fmcg_v2
        "Honing",  # fmcg_v2
        "Tomatenconserven",  # fmcg_v2
        "Sauzen voor pasta & rijst",  # fmcg_v2
        "Specerijen & sauzen bio",  # fmcg_v2
        "Pasta en rijst",  # fmcg_v2
    ),
    "breakfast_cereals": (
        "Ontbijtgranen",
        "Muesli, cereals",
        "Ontbijtgranen, krokante , muesli",
        "Muesli",
        "Granola",
        "Havermout",
        "Cornflakes",
        "Muesli, cornflakes en granen",  # fmcg_v2
    ),
    "confectionery_snacks": (
        "Chips",
        "Chips/Borrelhapjes",
        "Chips / Borrelhapjes",
        "Chips & aperitief",
        "Chips en aperitief snacks",
        "Chips / Nootjes / Gezouten koekjes",
        "Noten",
        "Noten, pinda's",
        "Gedroogd fruit en noten",
        "Koeken",
        "Koek",
        "Koekjes",
        "Koeken & taarten",
        "Mueslirepen, biscuits, ontbijtkoek",
        "Beschuit, ontbijtkoek, knackebrod",
        "Beschuit/Toast/Crackers",
        "Beschuit/Toast",
        "Chocolade",
        "Snoep",
        "Snoepgoed",
        "Kauwgom en muntjes",
        "Paaseitjes",
        "Feestenchocolade",
        "Gedroogd fruit",
        "Noten & droge vruchten",
        "Ontbijtrepen",
        "Snoep en kauwgom",  # fmcg_v2
        "Noten en gedroogde vruchten",  # fmcg_v2
    ),
    "non_alcoholic_beverages": (
        "Alcoholvrije dranken",
        "Frisdrank",
        "Softdrinks",
        "Vruchtensappen & drank",
        "Sappen, dranken",
        "Fruitsappen",
        "Vers fruitsap",
        "Vruchten & groentensap",
        "Water",
        "Limonades",
        "IJsthee",
        "IJskoffie",
        "Dranken zonder alcohol",
        "Alcoholvrij bier",
        "Alcoholvrije wijnen & bubbels",
        "Alcoholvrije wijnen",
        "Alcoholvrije aperitieven",
        "Zonder alcohol",
        "Alcoholvrij",
        "Vruchtendranken",  # fmcg_v2
    ),
    "coffee_tea": (
        "Koffie",
        "Thee",
        "Thee & kruidenthee",
        "Kruidenthee",
        "Koffiebonen",
        "Gemalen koffie",
        "Koffiecapsules",
        "Oploskoffie",
        "Koffiecups",
        "Koffiepads",
        "Snelfilterkoffie",
        "Groene thee",
        "Zwarte thee",
        "Speciale thee",
        "Vruchtenthee",
        "Oploskoffie/Cichorei",
    ),
    "alcoholic_beverages": (
        "Alcoholische dranken",
        "Wijn",
        "Wijn & Bubbels",
        "Wijn en bubbels",
        "Wijn, bier, sterke drank",
        "Bier, sterke drank, aperitieven",
        "Wijnen",
        "Rode wijn",
        "Witte wijn",
        "Rosé wijn",
        "Wijn - Rode wijn",
        "Wijn   rode wijn",
        "Bier",
        "Bier - Speciaalbier",
        "Speciaalbier",
        "Alcohol",
        "Sterke drank",
        "Sterke dranken",
        "Sterkedrank/Alcohol",
        "Aperitieven & sterke drank",
        "Bubbels",
        "Schuimwijnen & champagnes",
        "Schuimwijn/Champagne",
        "Cava/Schuimwijn/Champagne",
        "Mousserende wijn",
    ),
    "baby_care": (
        "Babyvoeding",
        "Baby-voeding",
        "Baby maaltijden",
        "Zuigelingenmelk",
        "Opvolgmelk",
        "Groeimelk",
        "Luiers en verschoning",
        "Luiers & luierbroekjes",
        "Luiers",
        "Verzorging baby",
        "Babyverzorging",
    ),
    "personal_care": (
        "Tandpasta",  # fmcg_v3
        "Verzorging & hygiëne",
        "Hygiëne en verzorging",
        "Lichaamsverzorging",
        "Haarverzorging",
        "Mondhygiëne",
        "Mondverzorging",
        "Tandverzorging",
        "Persoonlijke hygiëne",
        "Intieme hygiëne",
        "Intieme hygiene",
        "Verzorging mannen",
        "Douche",
        "Douche, bad",
        "Deodorant",
        "Shampoo, conditioner",
        "Scheergerei/ontharing",  # fmcg_v2
    ),
    "beauty_cosmetics": (
        "Make-up",
        "Makeup",
        "Makeup and makeup accessoires",
        "Nagellak",
        "Manicure & nagels",
        "Parfumerie",
        "Damesparfum",
        "Herenparfum",
        "Parfum, geschenksets",
        "Geschenkverpakkingen parfumerie",
        "Nagelverzorging",
        "Nagels",
        "Haarkleuring",
        "Gezichtsverzorging",
        "Gelaatsverzorging",
        "Huidverzorging",
    ),
    "household_care": (
        "Onderhoud",
        "Onderhoud / Huishouden",
        "Onderhoud/Huishouden",
        "Onderhoudsproducten",
        "Schoonmaakproducten",
        "Schoonmaakmiddelen",
        "Wassen",
        "Wassen/Strijken",
        "Wasmiddelen",
        "Wasmiddelen, wasverzachters",
        "Verzorging van de was",
        "Toiletreinigers & verfrissers",
        "Wc-producten",
        "Afwas",
        "Vaatwasproducten",
        "Luchtverfrissers",
        "Luchtverfrissers & navullingen",
        "Kleding, schoenonderhoud",
    ),
    "household_supplies": (
        "Toiletpapier",
        "Toiletpapier, keukenpapier & zakdoeken",
        "Toiletpapier & doekjes",
        "Vuilniszakken",
        "Zakdoeken",
        "Keukenpapier",
        "Keukenrol",
        "Folies, zakjes & bakpapier",
        "Folies, zakjes and bakpapier",
        "Huishoudfolies, huishoudzakken",
        "Vershoudfolie",
        "Bakpapier",
        "Aluminiumfolie",
        "Diepvrieszakjes",
        "Diepvrieszakjes/Vershoudzakjes",
        "Zakdoeken in dozen",
        "Zakdoeken in etui",
        "Zakdoekjes/Brildoekjes",
        "Keukenfolie/aluminiumfolie",
        "Papierproducten",  # fmcg_v2
    ),
    "pet_care": (
        "Huisdieren",
        "Huisdier",
        "Dieren",
        "Dierenvoeding",
        "Honden",
        "Katten",
    ),
    "health_wellness": (
        "Vitamines",
        "Vitaminen",
        "Vitaminen/ mineralen",
        "Multivitamines",
        "Voedingssupplementen",
        "Vitamines & supplementen",
        "Voedingssupplementen & vitaminen",
        "Vitamines, medicijnen",
        "Pleisters/Kompressen",
    ),
    "non_fmcg": (
        "Koken, tafelen, vrije tijd",
        "Koken, tafelen, non-food",
        "Koken en tafelen",
        "Tafelen",
        "Wonen",
        "Wonen, slapen",
        "Huisdecoratie",
        "Boekhandel & Schrijfwaren",
        "Bureaumateriaal",
        "Kantoorartikelen",
        "Speelgoed & Vrijetijd",
        "Speelgoedcatalogus",
        "Textiel",
        "Textiel dames",
        "Textiel Mannen",
        "Baby-textiel",
        "Multimedia",
        "Huishoudelektro",
        "Huishoudtoestellen",
        "Keukentoestellen",
        "Keukengerei",
        "Keukengerei en accessoires",
        "Pannen en kookpotten",
        "Klussen & Tuin",
        "Cadeau- & GSM-kaarten",
        "Cadeaubonnen en herlaadkaarten",
        "Babykleding",
        "Damesmode",
        "Herenmode",
        "(Slow)Juicers (Fruitpers)",
        "Leesbrillen/Lenzen",
        "Koffiefilters",
        "Koffie en theemachines",
        "Filter-koffiezetapparaten",
    ),
    "tobacco": (
        "Tabak",
        "Rookwaren",
    ),
    "plant_based_alternatives": (
        "Vleesvervangers",
        "Vegetarisch, vegan, vleesvervangers",
        "Plantaardige dranken",
        "Andere plantaardige dranken",
        "Plantaardige yoghurt",
        "Sojadrank",
        "Plantaardige yoghurt naturel",
        "Plantaardig alternatief",  # fmcg_v3
    ),
}


def normalize_label(label: str) -> str:
    """Normalize matching keys only; original source labels are immutable."""
    return re.sub(r"\s+", " ", unicodedata.normalize("NFC", label).strip()).casefold()


ANCHORS = {}
for _code, _labels in MERCHANDISE_LABELS_NL.items():
    for _label in _labels:
        _key = normalize_label(_label)
        if _key in ANCHORS and ANCHORS[_key] != _code:
            raise ValueError(f"Conflicting FMCG rule: {_label}")
        ANCHORS[_key] = _code

PROMOTION = re.compile(
    r"\b(?:promo\w*|aanbieding\w*|acties?|recepten?|folder\w*)\b|\d+\s*\+\s*\d+"
)
AMBIGUOUS_NODES = {
    normalize_label(x)
    for x in (
        "Afslankingsproducten",
        "Dieet / Vitamines / Voedingssuplementen",
        "Dieetvoeding/Voedingssupplementen",
        "Aardappel (ovenschotel)",
    )
}
FOOD_GROUPS = {
    "fresh_produce",
    "meat_poultry_seafood",
    "dairy_eggs",
    "bakery",
    "chilled_prepared",
    "frozen",
    "pantry_cooking",
    "breakfast_cereals",
    "confectionery_snacks",
    "non_alcoholic_beverages",
    "coffee_tea",
    "alcoholic_beverages",
    "plant_based_alternatives",
}


def classify_path(
    path: list[str], language: str, shop: str
) -> tuple[set[str], set[str], str]:
    """Use explicit category-boundary precedence, then abstain on disagreement."""
    if language != "nl":
        return set(), set(), "unsupported_language"
    keys = [normalize_label(label) for label in path]
    if any(PROMOTION.search(key) for key in keys):
        return set(), set(), "excluded"
    if set(keys) & AMBIGUOUS_NODES:
        return set(), set(), "excluded"
    if "koffieverrijkers" in keys or "koffiemelk, filters, suiker" in keys:
        # A reviewed AH path places sweetener in Coffee navigation.
        if (
            shop == "ah"
            and keys[:3]
            == [
                normalize_label(x)
                for x in (
                    "Frisdrank, sappen, koffie, thee",
                    "Koffie",
                    "Koffieverrijkers",
                )
            ]
            and "zoetstof" in keys
        ):
            return {"pantry_cooking"}, {"ah_coffee_sweetener_v1"}, "matched"
        return set(), set(), "excluded"
    matched = {key for key in keys if key in ANCHORS}
    codes = {ANCHORS[key] for key in matched}
    rules = {
        "nl_label_" + hashlib.sha256(key.encode()).hexdigest()[:12] for key in matched
    }
    # Precedence applies within one path only. Different paths must still agree.
    overrides = [
        ("pet_care", FOOD_GROUPS | {"pet_care"}, "pet_use_over_ingredient"),
        (
            "baby_care",
            FOOD_GROUPS | {"baby_care", "personal_care"},
            "baby_use_over_ingredient",
        ),
        ("frozen", FOOD_GROUPS, "explicit_frozen_food"),
        (
            "plant_based_alternatives",
            {
                "plant_based_alternatives",
                "dairy_eggs",
                "meat_poultry_seafood",
                "non_alcoholic_beverages",
            },
            "explicit_plant_alternative",
        ),
        (
            "household_supplies",
            {"household_supplies", "household_care"},
            "paper_supplies_over_department",
        ),
        (
            "beauty_cosmetics",
            {"beauty_cosmetics", "personal_care"},
            "beauty_over_department",
        ),
        (
            "health_wellness",
            {"health_wellness", "personal_care"},
            "health_over_department",
        ),
        ("dairy_eggs", {"dairy_eggs", "bakery"}, "eggs_over_baking_department"),
    ]
    for winner, allowed, rule in overrides:
        if winner in codes and len(codes) > 1 and codes <= allowed:
            return {winner}, rules | {rule}, "matched"
    if set(keys) & {"gedroogd fruit", "noten & droge vruchten"} and codes <= {
        "fresh_produce",
        "confectionery_snacks",
    }:
        return (
            {"confectionery_snacks"},
            rules | {"dried_fruit_nuts_over_produce_department"},
            "matched",
        )
    if "vruchtenconserven" in keys and codes <= {"fresh_produce", "pantry_cooking"}:
        return (
            {"pantry_cooking"},
            rules | {"canned_fruit_over_produce_department"},
            "matched",
        )
    if "ontbijtrepen" in keys and codes <= {
        "breakfast_cereals",
        "confectionery_snacks",
    }:
        return {"confectionery_snacks"}, rules | {"cereal_bars_are_snacks"}, "matched"
    alcohol_free = {
        "alcoholvrij",
        "alcoholvrij bier",
        "alcoholvrije wijnen & bubbels",
        "alcoholvrije wijnen",
        "alcoholvrije aperitieven",
        "zonder alcohol",
    }
    if set(keys) & alcohol_free and codes <= {
        "alcoholic_beverages",
        "non_alcoholic_beverages",
    }:
        return {"non_alcoholic_beverages"}, rules | {"explicit_alcohol_free"}, "matched"
    return codes, rules, "matched" if codes else "no_match"


# Reviewed source contradictions, keyed by (daltix_id, shop, country). A product
# is withheld from classification while its source payload hash is the reviewed
# one; a changed payload triggers a fresh review.
ZERO_ALCOHOL_NAME_CONFLICT = (
    "Explicit zero-alcohol name conflicts with candidate alcoholic navigation."
)
COTTON_SWAB_MAKEUP_CONFLICT = (
    "Cotton-swab name is insufficiently supported by makeup navigation."
)

REVIEW_EXCLUSIONS = {
    ("0084706041d3a9cce6f003f620c931cebbf1fc85cd1928b33d880edd7071b733", "clp", "be"): {
        "reason": "Split-pea name does not establish fresh-produce storage.",
        "source_payload_hashes": [
            "024adabdf9df194240ac00fa685d9741183e0777905bdc90a1ba8d1f5220f83f"
        ],
    },
    ("00909cc0c1d8cfe59f2df5e4050ff6227eee77afae65aac7c70329b54062e0bc", "clp", "be"): {
        "reason": "Seaweed caviar-alternative name contradicts shellfish navigation.",
        "source_payload_hashes": [
            "eef4b2c7f596260aea20a77f695081b57612cd6960b47e2bb8189c246aa7f23d"
        ],
    },
    ("01b6fd43a920ba965e8e272d5a764d027c3a6b3116c72485f7b0e5160da5d7c1", "dll", "be"): {
        "reason": "Poppy-seed name contradicts breakfast-cereal navigation.",
        "source_payload_hashes": [
            "3a3c2d067780756592eef612cae65f62c8ec46f8528f95e5efac248766fad7e1"
        ],
    },
    ("040da69ba998f223ac562e903110ba2f102bc4a8c3f253df60aa7f14942e293b", "clp", "be"): {
        "reason": "Fajita-kit name does not establish a ready prepared meal.",
        "source_payload_hashes": [
            "5a5da40b3ef05307fdd61550058a5db30e3e15875d288c42b76dcc0181cc07c6"
        ],
    },
    (
        "07a1a889f10f3d4bec4603d6cabec99810577d87d1956a9107edd724f8d77565",
        "jumbo",
        "nl",
    ): {
        "reason": ZERO_ALCOHOL_NAME_CONFLICT,
        "source_payload_hashes": [
            "4bcdd18c2dd0914b10eb3063d53074ff838ae881fcabe6c24a28e7033744de57"
        ],
    },
    ("09671c3ecf4d121454355f541e1902144880fa47fcc8d500fb266c4c7b7085a9", "crf", "be"): {
        "reason": ZERO_ALCOHOL_NAME_CONFLICT,
        "source_payload_hashes": [
            "fd74604ac85835971d56f7c40f83ad127bf24606640dbf73f375040498728dbc"
        ],
    },
    ("10ed3ffc9e214be5c943f3e03f5f9d234f5ce7226bc8e807c438bbb57c45f2cd", "crf", "be"): {
        "reason": ZERO_ALCOHOL_NAME_CONFLICT,
        "source_payload_hashes": [
            "938e7eaaac2f393781009916e110ce46dea125a83c5a8997cb674923de4e964d"
        ],
    },
    ("1b02bc9f5cc1a672436c6ec3cd2bffa6065d90410fdd1e34331ecda630b65402", "crf", "be"): {
        "reason": ZERO_ALCOHOL_NAME_CONFLICT,
        "source_payload_hashes": [
            "938e7eaaac2f393781009916e110ce46dea125a83c5a8997cb674923de4e964d"
        ],
    },
    ("1c385fddd3adbf973da4f2417fd4b4dbb0aa7abbaee44ff908f912926271cb7f", "crf", "be"): {
        "reason": ZERO_ALCOHOL_NAME_CONFLICT,
        "source_payload_hashes": [
            "80feb085a6fe94b098eda361c5c9ff1901ba2f2acdc5180adfff7c852f163c99"
        ],
    },
    (
        "21aba07007260c074bb1f55dd98611b11aaee3174c5caeb7e89328d05e7c4883",
        "jumbo",
        "nl",
    ): {
        "reason": ZERO_ALCOHOL_NAME_CONFLICT,
        "source_payload_hashes": [
            "34570b39ab904cfe72cfd7a9c48c9d97bfbb8b535bd19fb2e6e2982242062fed"
        ],
    },
    (
        "257758866340bba813a45d7dbb073bc5bd77f55be2dc0f112232c91f5a11a1ad",
        "jumbo",
        "nl",
    ): {
        "reason": ZERO_ALCOHOL_NAME_CONFLICT,
        "source_payload_hashes": [
            "4bcdd18c2dd0914b10eb3063d53074ff838ae881fcabe6c24a28e7033744de57"
        ],
    },
    ("27dfade77d6bbe7c1aabfedc45795c3cc355086f7065a8cd6e2de7d2792029a5", "crf", "be"): {
        "reason": ZERO_ALCOHOL_NAME_CONFLICT,
        "source_payload_hashes": [
            "8dfa41b37237fdbbd3de6b8682ff1f52b762399c7b4c5d8abbdff999b6ef0549"
        ],
    },
    ("28f6401ff87012c20bf5d68b8c7cb560f87373597a02b2f4cbd1c1b7f373bc24", "crf", "be"): {
        "reason": ZERO_ALCOHOL_NAME_CONFLICT,
        "source_payload_hashes": [
            "938e7eaaac2f393781009916e110ce46dea125a83c5a8997cb674923de4e964d",
            "e67ebeb77269772b71b3b1c3cbdfeb7c4aef3f88e17005fbc2fff46f2e5723dd",
        ],
    },
    ("2e55e1958dd6422295a547f4f799911bca20ea36c345de99c7f5d06a6691bf54", "crf", "be"): {
        "reason": ZERO_ALCOHOL_NAME_CONFLICT,
        "source_payload_hashes": [
            "fd74604ac85835971d56f7c40f83ad127bf24606640dbf73f375040498728dbc"
        ],
    },
    (
        "2ece92854f38dc757bc08994d83e93677521e4f353249a6409c5f52e4e4b0a16",
        "aldi",
        "be",
    ): {
        "reason": COTTON_SWAB_MAKEUP_CONFLICT,
        "source_payload_hashes": [
            "b3cf273f2cd773976399d5ffbb2835a27cf1e1dbba6ac795bec1b6e506ea9418"
        ],
    },
    ("3f1c343d80fca2f78c79812bc2829313575996b384826b77a95bc1ec8114e14f", "crf", "be"): {
        "reason": ZERO_ALCOHOL_NAME_CONFLICT,
        "source_payload_hashes": [
            "a47b8fe216ed65fb94cc70147c7c7649c4bb037bd0200c854a007cb9edcb7174"
        ],
    },
    ("40b3317064805a026dbe0e56b6dbd3d7595923f2aee3b22c1252e708c297c189", "crf", "be"): {
        "reason": ZERO_ALCOHOL_NAME_CONFLICT,
        "source_payload_hashes": [
            "c32550aafff8da05c493da4ad1e3f32de144b0e23195b6682fd15b439f0a2348"
        ],
    },
    (
        "4205b6a73772778531b18b5faaec52d17feb690f5cc59f2898053f980514f944",
        "jumbo",
        "nl",
    ): {
        "reason": ZERO_ALCOHOL_NAME_CONFLICT,
        "source_payload_hashes": [
            "2788a1c1f01a1022c1a23e5aab6ed2e87df282d0428b18f16c246db076aab734"
        ],
    },
    ("433a290de001308bec7390b9d6597039be1ffc7e6cfbfa248a07e6a145904eb3", "ah", "nl"): {
        "reason": "Meal-box name contradicts the iced-tea source path.",
        "source_payload_hashes": [
            "1f10cf1e049b4d92c52d5f4df9c9fd8757b9c85d1632a4fde61d6ad3b50bd74a"
        ],
    },
    (
        "44beecc73e29dc6e682245fe73c60a90ba0b6e879d51c5eaf5c4429fe8192806",
        "jumbo",
        "nl",
    ): {
        "reason": ZERO_ALCOHOL_NAME_CONFLICT,
        "source_payload_hashes": [
            "ad1b5f462f6076977ae3ccbd7bbe724398341d3fe9d4903195f97c2b22a9c9ea"
        ],
    },
    ("4d63c2d7821e703d56b85ba4a5597e41675e69938e7f325e9ea8cea0bd744a2a", "crf", "be"): {
        "reason": ZERO_ALCOHOL_NAME_CONFLICT,
        "source_payload_hashes": [
            "a47b8fe216ed65fb94cc70147c7c7649c4bb037bd0200c854a007cb9edcb7174"
        ],
    },
    (
        "5b7ed980fd224f2dbeaca6b5a1c2bbd7f5edc9700317d9545d671fda354d4647",
        "aldi",
        "be",
    ): {
        "reason": COTTON_SWAB_MAKEUP_CONFLICT,
        "source_payload_hashes": [
            "b6d32f9f925a1877a3bd2432bb76745b360c33490ffd98527e9381c8fda3c31a"
        ],
    },
    (
        "5e746033e0a66577c9f83e58d666add1aade651fd92623a6ebaa3221399a90e3",
        "jumbo",
        "nl",
    ): {
        "reason": ZERO_ALCOHOL_NAME_CONFLICT,
        "source_payload_hashes": [
            "b2c44a29d3dc46af1acc660003e4641fa98f958a18b4dd548b25c1424353db7c"
        ],
    },
    ("6576e0afe8fb03381ec3ff20b93793d80c1872aa6aad6470f806a4ef79339e50", "crf", "be"): {
        "reason": ZERO_ALCOHOL_NAME_CONFLICT,
        "source_payload_hashes": [
            "06ad15f8804bc7ef77a8bff2baeac44e6d3e636de529f56e7f8c23b4265f24cf"
        ],
    },
    ("6ae022602c4de7fd135e8d0aa3562679987d96a0d9c89569bfae40e575368d47", "crf", "be"): {
        "reason": ZERO_ALCOHOL_NAME_CONFLICT,
        "source_payload_hashes": [
            "4dc2e9427f8580b0dbeb125454912272ae829770071de48be517fa2454a0f740"
        ],
    },
    ("6faea0d34beaa9901a9c459da56c267ec48f977d82f3d5cb16dac9e4e5847ce8", "crf", "be"): {
        "reason": ZERO_ALCOHOL_NAME_CONFLICT,
        "source_payload_hashes": [
            "10810b217af9cfbeae1300dd0a2cd6462c13d04d0f6d3a6ef2ce54788781b8b3",
            "fd74604ac85835971d56f7c40f83ad127bf24606640dbf73f375040498728dbc",
        ],
    },
    ("7eb5d6b6099781d622ae50ad05244e6bd2f8caedc1cb309bfbd6957a7bcb5941", "crf", "be"): {
        "reason": ZERO_ALCOHOL_NAME_CONFLICT,
        "source_payload_hashes": [
            "fd74604ac85835971d56f7c40f83ad127bf24606640dbf73f375040498728dbc"
        ],
    },
    ("7fa52ecebc789d9e46bc3545ab08329c6542fb15fa315d60777ae246505e301b", "crf", "be"): {
        "reason": ZERO_ALCOHOL_NAME_CONFLICT,
        "source_payload_hashes": [
            "fd74604ac85835971d56f7c40f83ad127bf24606640dbf73f375040498728dbc"
        ],
    },
    ("7fd96836569ccf07575c2215e6df953cc8e5b0d0dd9db685a822d82d6da22426", "crf", "be"): {
        "reason": ZERO_ALCOHOL_NAME_CONFLICT,
        "source_payload_hashes": [
            "720b34ae0369b69783605f785d84d85fc97adb720d40fa29e7a07ae38902e127"
        ],
    },
    (
        "8c2bfcf3ad081c9c6c595423139fae96a7fbb81b1a7b7d509ea1defc154a075f",
        "jumbo",
        "nl",
    ): {
        "reason": ZERO_ALCOHOL_NAME_CONFLICT,
        "source_payload_hashes": [
            "a43e4bdc72b7f6456cf45885740b39483bad517636f210f3f4bc7b0220ba91a2"
        ],
    },
    ("8d0d2b20ef8ef74a67fde0ef225578f9fb50278a97379dd43a5ae7bd4cbd43dd", "crf", "be"): {
        "reason": ZERO_ALCOHOL_NAME_CONFLICT,
        "source_payload_hashes": [
            "301d1823748997784e43cc71842c89ddacb6061397f9dd50650319b90caef020",
            "4dc2e9427f8580b0dbeb125454912272ae829770071de48be517fa2454a0f740",
        ],
    },
    ("8fc74788f66b7de154dc0b04d258aa8d6816841debdfbb4eafa82a3b5e44593f", "clp", "be"): {
        "reason": ZERO_ALCOHOL_NAME_CONFLICT,
        "source_payload_hashes": [
            "84265417386b58a4ce389bc7658e12270978e4163017f9005457ad9326e1e418"
        ],
    },
    ("ae1c1dda0cccb99fb8c6ebc3dc14f36fd8eeb404bbd72e99cb296e13069cbe29", "clp", "be"): {
        "reason": ZERO_ALCOHOL_NAME_CONFLICT,
        "source_payload_hashes": [
            "7ad8ae957188e502cb430ac814d9895276ea20dd34b537a59a803c497954bcdc"
        ],
    },
    ("aea59bbd9eb9bfa3d48ab370df753adb5fa88d2cd368025f5d4b9a9b4b7e3f0e", "crf", "be"): {
        "reason": ZERO_ALCOHOL_NAME_CONFLICT,
        "source_payload_hashes": [
            "8dfa41b37237fdbbd3de6b8682ff1f52b762399c7b4c5d8abbdff999b6ef0549"
        ],
    },
    ("af8d5c23fde47687b0d66fa99b12c4e8e5dfdc393e60d247dacfa4d278349cb8", "crf", "be"): {
        "reason": ZERO_ALCOHOL_NAME_CONFLICT,
        "source_payload_hashes": [
            "1bc7f2c445b89e5bbee6de48e0e41cb5217e07cca21bf86b9169ab0f4f600d34"
        ],
    },
    ("b1eaeffd0afc640be54674ce1afdbb4ffdce17e98de8e6bfd1dba0b832c5de8e", "dll", "be"): {
        "reason": ZERO_ALCOHOL_NAME_CONFLICT,
        "source_payload_hashes": [
            "3a1921bfb9d06e26c249014d241e284ac135f56b07940c46c0d42ab484c90319"
        ],
    },
    (
        "b4c365926e6e913767c6b9f369b77b1df53bbcb67dfea460188113633bf8aca9",
        "jumbo",
        "nl",
    ): {
        "reason": ZERO_ALCOHOL_NAME_CONFLICT,
        "source_payload_hashes": [
            "4bcdd18c2dd0914b10eb3063d53074ff838ae881fcabe6c24a28e7033744de57"
        ],
    },
    ("b6ac4d3ed0406ebd3d78315b73f7642cb1b00ef7ec6c8aa035824dfdd0f5ff03", "clp", "be"): {
        "reason": ZERO_ALCOHOL_NAME_CONFLICT,
        "source_payload_hashes": [
            "f6050bf0f1d57c5a5021c50799af41d3f594b6d77ed66de8f4a9fb825226ebcb"
        ],
    },
    ("bef437be68a2a3c7dcac7230c9fb9dcea0373431c40c0e127435ac052c65f49f", "crf", "be"): {
        "reason": ZERO_ALCOHOL_NAME_CONFLICT,
        "source_payload_hashes": [
            "fa1bb237e43ad46e450fbca2173374d6758e3c194f521d6395eeba07360a4ac1"
        ],
    },
    ("c2c32193922f420e1d7e12ef52da06425434b3644f481ad977dbb73b45842af7", "crf", "be"): {
        "reason": ZERO_ALCOHOL_NAME_CONFLICT,
        "source_payload_hashes": [
            "5e02b89c524c40a8a2345d21cde1fcbefad967db3b55ff8c5d2af66b5be4a9d2"
        ],
    },
    ("c2ece85dcaf92ba776be98c09a7caeabc307039b194d2781fe5a1e977cd6a9d2", "crf", "be"): {
        "reason": ZERO_ALCOHOL_NAME_CONFLICT,
        "source_payload_hashes": [
            "53bac8498ceb4731a8ab8f5f0f11dddfd758b2024c2198f8e8dadc3541fb2b0e"
        ],
    },
    ("c92f8457e8db5eefad874dabb3f3e3c95a706c957ccd45dd40d959bddb472b0b", "crf", "be"): {
        "reason": ZERO_ALCOHOL_NAME_CONFLICT,
        "source_payload_hashes": [
            "938e7eaaac2f393781009916e110ce46dea125a83c5a8997cb674923de4e964d"
        ],
    },
    ("cba8754778d38b4bbaecbfdaf076507da9444495157e8ca36162380899b2e157", "clp", "be"): {
        "reason": ZERO_ALCOHOL_NAME_CONFLICT,
        "source_payload_hashes": [
            "84265417386b58a4ce389bc7658e12270978e4163017f9005457ad9326e1e418"
        ],
    },
    ("cef770f896b24fba52ea2491ce2b75154c7dc0b2ba866b033c4e7333d91d9b12", "crf", "be"): {
        "reason": ZERO_ALCOHOL_NAME_CONFLICT,
        "source_payload_hashes": [
            "fd74604ac85835971d56f7c40f83ad127bf24606640dbf73f375040498728dbc"
        ],
    },
    ("d6695f313fb4ecb5accd0f469aa5e04f87625f2c472f229a039c491f57a73d5a", "crf", "be"): {
        "reason": ZERO_ALCOHOL_NAME_CONFLICT,
        "source_payload_hashes": [
            "1bc7f2c445b89e5bbee6de48e0e41cb5217e07cca21bf86b9169ab0f4f600d34"
        ],
    },
    (
        "e1dba94b923e27926992b3d6b3fcfb876af3b7f72e4d1230a49f1da1f740321c",
        "aldi",
        "be",
    ): {
        "reason": ZERO_ALCOHOL_NAME_CONFLICT,
        "source_payload_hashes": [
            "697304e5971f28310f7e059ce8f22b8aaf28c2cc6ee3581052f2a269b807f795"
        ],
    },
    (
        "e6c258f080ac3825c279cdebc503f683a65a8c7fe0365b13f2420d88583ef98f",
        "jumbo",
        "nl",
    ): {
        "reason": ZERO_ALCOHOL_NAME_CONFLICT,
        "source_payload_hashes": [
            "4bcdd18c2dd0914b10eb3063d53074ff838ae881fcabe6c24a28e7033744de57"
        ],
    },
    ("e7c37a1a6b1b5ffbff2a5551ce3910ae5055f7826f4ee7b50e7b8847c8c2c7d6", "crf", "be"): {
        "reason": ZERO_ALCOHOL_NAME_CONFLICT,
        "source_payload_hashes": [
            "10810b217af9cfbeae1300dd0a2cd6462c13d04d0f6d3a6ef2ce54788781b8b3",
            "fd74604ac85835971d56f7c40f83ad127bf24606640dbf73f375040498728dbc",
        ],
    },
    (
        "ee09dbc961dbc5321d417a650efaf2bbfab567ef89c0f88ec16d35b7336077cf",
        "jumbo",
        "nl",
    ): {
        "reason": ZERO_ALCOHOL_NAME_CONFLICT,
        "source_payload_hashes": [
            "2788a1c1f01a1022c1a23e5aab6ed2e87df282d0428b18f16c246db076aab734"
        ],
    },
    ("eedfa04edf8f1a98c59dc6bc146124f16db439907fd62b9064339db30d97afcc", "crf", "be"): {
        "reason": ZERO_ALCOHOL_NAME_CONFLICT,
        "source_payload_hashes": [
            "10810b217af9cfbeae1300dd0a2cd6462c13d04d0f6d3a6ef2ce54788781b8b3"
        ],
    },
    ("f0cc5d580aae2191ddac0b4e8213482c18bf4098b132fbf17896937ab727d48a", "crf", "be"): {
        "reason": ZERO_ALCOHOL_NAME_CONFLICT,
        "source_payload_hashes": [
            "ea3be51270813519dea059612c151022c3567ab1ae2318a75cedc6083aec2673"
        ],
    },
    ("f20ac2283511848f9e1a74107dbd47ded77e8fa9b72ffa4a5090bdb0528bace6", "dll", "be"): {
        "reason": ZERO_ALCOHOL_NAME_CONFLICT,
        "source_payload_hashes": [
            "be60235d6d838a8a0f5c82c7b5f28618c737afc6e2d7635400ae137fdf9169ad"
        ],
    },
}
