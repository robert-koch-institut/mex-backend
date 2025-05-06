from typing import cast

import pytest

from mex.common.models import AnyExtractedModel
from tests.ingest.test_main import Payload


@pytest.fixture
def post_payload(artificial_extracted_items: list[AnyExtractedModel]) -> Payload:
    items = [model.model_dump() for model in artificial_extracted_items]
    return cast("Payload", {"items": items})


def test_bla(post_payload: Payload) -> None:
    assert post_payload == {
        "items": [
            {
                "hadPrimarySource": "00000000000000",
                "identifierInPrimarySource": "PrimarySource-2516530558",
                "version": None,
                "alternativeTitle": [],
                "contact": [],
                "description": [],
                "documentation": [],
                "locatedAt": [],
                "title": [],
                "unitInCharge": [],
                "entityType": "ExtractedPrimarySource",
                "identifier": "bpFBLhGNhn7hr3Azo1Z45g",
                "stableTargetId": "bduBnQ5GcBhRgTGXBRwQ2m",
            },
            {
                "hadPrimarySource": "bduBnQ5GcBhRgTGXBRwQ2m",
                "identifierInPrimarySource": "PrimarySource-2516530559",
                "version": None,
                "alternativeTitle": [],
                "contact": [],
                "description": [],
                "documentation": [],
                "locatedAt": [],
                "title": [],
                "unitInCharge": [],
                "entityType": "ExtractedPrimarySource",
                "identifier": "dOHucCUVfcF3erEQjllCi5",
                "stableTargetId": "goPgHfOzReSP60a3GKPBOr",
            },
            {
                "hadPrimarySource": "goPgHfOzReSP60a3GKPBOr",
                "identifierInPrimarySource": "Organization-991028314",
                "officialName": [
                    {
                        "value": "Vorbei gerade reiten hinter Welt klettern Freude. Sieben Meer sieht Jahr Name.",
                        "language": "de",
                    },
                    {
                        "value": "Leute Blume aus böse. Doch schicken nennen hier nennen kurz Flasche Minute. Schenken dafür war. Sonst Glück gar dir Abend. Zeit Sommer letzte Bruder offen. Nimmt dick Sommer Meer springen mal ab. Laut da Hase zur Tante fest. Wagen Wissen bis laufen Beispiel. Sonntag das lachen mich Freude von ins. Wichtig so schlecht Schwester holen.",
                        "language": "de",
                    },
                ],
                "alternativeName": [
                    {
                        "value": "Fallen gleich natürlich fast fahren. Schule Bauer Bein antworten. Beispiel fertig Sache essen davon will. Nach fiel Küche hören Boden richtig haben. Krank sein schaffen zum braun Hunger. Stunde böse Eis Mann den einfach. Fisch werfen kann viel Milch baden überall. Zeigen Wetter vergessen. Hören las vier Weihnachten. Herr zur Freude Stadt. Fünf schnell seit Stadt rot laufen einmal.",
                        "language": "de",
                    },
                    {
                        "value": "Setzen Baum genau baden Land wer Mama bauen. Bett einige nein schauen Ferien danach warum. Nacht auch Bild rennen Mutter. Wiese erklären schön fest hinter sagen.",
                        "language": "de",
                    },
                ],
                "geprisId": ["https://gepris.dfg.de/gepris/institution/79911"],
                "gndId": [
                    "https://d-nb.info/gnd/13542-7",
                    "https://d-nb.info/gnd/9808412-4",
                ],
                "isniId": [],
                "rorId": ["https://ror.org/34k8qnb74", "https://ror.org/64k0qnb05"],
                "shortName": [
                    {
                        "value": "Schuh Monate da wissen schlimm deshalb arbeiten. Name deshalb darin gelb weg stellen. Weg erzählen Fisch. Haben Monat vor Zeitung. Wetter fertig Dorf Stunde. Wahr andere setzen ging.",
                        "language": "de",
                    },
                    {
                        "value": "Dir schreiben Herz Arzt können. Tot kochen gehen stellen fangen. Richtig holen Fußball. Zehn Fußball Pferd holen. Gibt anfangen vorbei frei nah zum Arzt. Jung sein gerade nie Licht hier nach Frage. Ab Wetter Maus Schüler sie sagen. Jeder wo wer oben rufen darauf. Letzte rund gesund schnell Minutenmir. In wir scheinen Weihnachten nächste. Hängen Uhr gehen erzählen stark. Reich Kopf kann essen Küche Weg stellen drei.",
                        "language": "de",
                    },
                ],
                "viafId": [
                    "https://viaf.org/viaf/134332003",
                    "https://viaf.org/viaf/769367632",
                ],
                "wikidataId": ["https://www.wikidata.org/entity/Q870831"],
                "entityType": "ExtractedOrganization",
                "identifier": "b1JKYoj6FWS6rVC3RBSWlA",
                "stableTargetId": "bAl5wuuzs5f5borMwYpgEJ",
            },
            {
                "hadPrimarySource": "bduBnQ5GcBhRgTGXBRwQ2m",
                "identifierInPrimarySource": "Organization-991028315",
                "officialName": [
                    {
                        "value": "Tisch was ja weinen kaufen dir viel kennen. Uns Fuß springen lassen fest. Besser fünf fröhlich Geschichte Meer dem. Heute dick essen ihr gibt. Ihr erschrecken in weil Nase Schnee verstehen. Herr aber Haare so verlieren wohl immer. Stunde weg laut früh laufen Himmel Sache. Sehen unser wer Zug man weinen. Mutter anfangen Pferd dazu kommen baden Herr es. Gleich Wagen Fußball Woche in um. Alt Meer überall gut Rad später rechnen. Wo er am.",
                        "language": "de",
                    },
                    {
                        "value": "Lachen dabei Wissen Kopf. Sonntag drehen nie sind dafür. Zahl Buch anfangen genau. Hoch schwimmen Berg schlimm oft bringen.",
                        "language": "de",
                    },
                ],
                "alternativeName": [],
                "geprisId": [
                    "https://gepris.dfg.de/gepris/institution/18506",
                    "https://gepris.dfg.de/gepris/institution/65726",
                ],
                "gndId": [
                    "https://d-nb.info/gnd/7694531-4",
                    "https://d-nb.info/gnd/7996507-5",
                ],
                "isniId": [],
                "rorId": ["https://ror.org/80s8v3h13"],
                "shortName": [
                    {
                        "value": "Kein also Garten hängen fiel rufen heiß. Mama ist sieht gibt schreien fröhlich Flasche. Sechs gerade sie Woche. Eis sich Fuß natürlich. Nennen Zug Uhr geben Hand gegen Schule. Früher Zeitung Minute schnell. Packen sitzen Brot allein. Kurz kam einfach bald wird. Dabei beim Leben nach schlagen dich tief.",
                        "language": "de",
                    },
                    {
                        "value": "Danach Sonne nie steigen Familie. Herr weit geben natürlich Jahr mögen Baum. Wissen erst erzählen richtig. Fest Milch Ding ich. Können Apfel gab führen versuchen beißen Fenster Freude. Neben suchen endlich dafür fallen deshalb Freude denken. Draußen gehören sind mögen.",
                        "language": "de",
                    },
                ],
                "viafId": [],
                "wikidataId": [
                    "https://www.wikidata.org/entity/Q488771",
                    "https://www.wikidata.org/entity/Q659401",
                ],
                "entityType": "ExtractedOrganization",
                "identifier": "bMcCdUQAMRrEXEgV0K6rcp",
                "stableTargetId": "mmJ3BAHHgUz5berBCEzD5",
            },
            {
                "hadPrimarySource": "bduBnQ5GcBhRgTGXBRwQ2m",
                "identifierInPrimarySource": "OrganizationalUnit-3284134756",
                "parentUnit": None,
                "name": [
                    {
                        "value": "Rechnen wenig ein Nase Onkel Leben Blume. Ich Geschichte Bruder doch ihm Lehrer. Tier mich Klasse gerade Gesicht zeigen. Vier trinken ab voll kommen Ball. Hart sofort offen sollen erklären mich wichtig. Wahr lassen Angst ohne Stunde schwarz. Milch Milch Boden zwischen weiß.",
                        "language": "de",
                    }
                ],
                "alternativeName": [],
                "email": ["selim77@example.org", "jrohleder@example.com"],
                "shortName": [
                    {
                        "value": "Glück stehen freuen Abend sehr und. Überall leben deshalb Meer Gott Milch mein Klasse.",
                        "language": "de",
                    }
                ],
                "unitOf": [],
                "website": [
                    {"language": "en", "title": "Weinhage", "url": "http://eberth.org/"}
                ],
                "entityType": "ExtractedOrganizationalUnit",
                "identifier": "0lRoItjJ1EmL1PpazC8ml",
                "stableTargetId": "dGhJF5yUUbBLw8Pe50A7zr",
            },
            {
                "hadPrimarySource": "goPgHfOzReSP60a3GKPBOr",
                "identifierInPrimarySource": "OrganizationalUnit-3284134757",
                "parentUnit": None,
                "name": [
                    {
                        "value": "Vom zeigen bekommen später Freude oben.",
                        "language": "de",
                    },
                    {
                        "value": "Verlieren versuchen Herr gern Schüler sollen. Arbeiten Bein ohne wer über sieht jung. Sollen schreien Brief draußen. Weil aus stellen gar her. Fiel ihm schicken eigentlich. Bett vom ob Feuer. Denken kochen fliegen Frage Affe suchen Weg scheinen. Bekommen wissen zurück Geburtstag. Schlecht klein gegen überall verlieren Buch.",
                        "language": "de",
                    },
                ],
                "alternativeName": [
                    {
                        "value": "Zimmer rennen früher. Vier springen Zeitung weiter sieben Uhr Weihnachten. Nie Haare Opa ob scheinen. Oft weil plötzlich gelb. Apfel Hase dauern weiter gesund ihr. Davon Meer Zeit Mama Welt. Fragen trinken las Tag. Suchen kurz Berg so Familie schauen. Zehn suchen hat steigen als Maus.",
                        "language": "de",
                    },
                    {
                        "value": "Nase traurig wir Buch Fenster. Ob wirklich gewinnen. Fahrrad beide treffen weiter gegen ich dem wissen. Öffnen dafür Ferien Maus Himmel. Sache sie wissen immer sie die. Reiten Uhr nächste wir Fußball schlecht. Kann wo verstecken Brief schlafen. Weihnachten Platz Baum früher zehn wo treffen. Rufen Pferd nennen am. Fiel darauf Mutter Bein Sonne erst. Tier Fenster spielen stehen nennen. Versuchen denn das Flasche dunkel. Dauern halten stehen. Gestern dunkel bald sie lustig sieht Land springen. Nehmen Kind Fahrrad nur sehr weit richtig. Klettern wünschen Baum lange Hund.",
                        "language": "de",
                    },
                ],
                "email": ["fbriemer@example.com", "carsteneberth@example.com"],
                "shortName": [
                    {
                        "value": "Nichts Brief schreiben verlieren rechnen sonst sechs. Jeder warten ihn tun holen. Genau genau Frage wohl. Bein Tür Boden brauchen sich tief mit. Wohnung Monate Sache ihm von wird ins wenig. Zehn neun Haare. Letzte die las Vater gut mit dafür. Müssen Tante unter ziehen tot. Schauen mit nimmt rennen Schluss Angst früh Sommer. Gut eigentlich denken rennen Beispiel sicher. Blume mal her.",
                        "language": "de",
                    },
                    {
                        "value": "Heiß sechs sprechen Mädchen. Wald die so baden. Fröhlich rot laufen kann. Essen Licht groß da scheinen. Laufen rund aus hinter neu acht gefährlich es. Sonne gab danach Lehrerin. Durch Herr schreien leicht Uhr Tag. Hoch sie Junge kochen. Vielleicht müssen müde schwarz. Gab hängen Zahl dumm Wissen kochen. Blau unter fiel natürlich Luft hinter. Sieht Fahrrad spät Berg Onkel. Leute weinen gut sehr tragen Tisch Ferien. Frage dumm dick Sohn. Durch Spiel legen so.",
                        "language": "de",
                    },
                ],
                "unitOf": ["mmJ3BAHHgUz5berBCEzD5", "bAl5wuuzs5f5borMwYpgEJ"],
                "website": [
                    {"language": None, "title": None, "url": "https://blumel.com/"}
                ],
                "entityType": "ExtractedOrganizationalUnit",
                "identifier": "eUuEsAmXovLw9PUvGg5Ltf",
                "stableTargetId": "fKzLLO4cRXadKyY4PWvnZk",
            },
            {
                "hadPrimarySource": "goPgHfOzReSP60a3GKPBOr",
                "identifierInPrimarySource": "ContactPoint-4181830114",
                "email": ["dbender@example.com", "cosimo19@example.net"],
                "entityType": "ExtractedContactPoint",
                "identifier": "gHnnw0FYJB8PSjbxL6lBPp",
                "stableTargetId": "fLbEhIUrS6GbsQZh6DCdsq",
            },
            {
                "hadPrimarySource": "goPgHfOzReSP60a3GKPBOr",
                "identifierInPrimarySource": "ContactPoint-4181830115",
                "email": [
                    "mendesibilla@example.net",
                    "marioschleich@example.com",
                    "verabolander@example.com",
                ],
                "entityType": "ExtractedContactPoint",
                "identifier": "erXhEA7rnnl2htWq4GXq8g",
                "stableTargetId": "d0uKlX2GlXKqxkRdcyj7Q2",
            },
            {
                "hadPrimarySource": "bduBnQ5GcBhRgTGXBRwQ2m",
                "identifierInPrimarySource": "Person-639677827",
                "affiliation": ["bAl5wuuzs5f5borMwYpgEJ", "bAl5wuuzs5f5borMwYpgEJ"],
                "email": ["hpruschke@example.net"],
                "familyName": ["hin ihn"],
                "fullName": [],
                "givenName": ["Musik"],
                "isniId": ["https://isni.org/isni/5192546291486528"],
                "memberOf": [
                    "fKzLLO4cRXadKyY4PWvnZk",
                    "dGhJF5yUUbBLw8Pe50A7zr",
                    "fKzLLO4cRXadKyY4PWvnZk",
                ],
                "orcidId": [],
                "entityType": "ExtractedPerson",
                "identifier": "db4NCcHwAdeZ0FuJvZ2D4A",
                "stableTargetId": "buz8FH8lnXEHVVMKqR1sWz",
            },
            {
                "hadPrimarySource": "goPgHfOzReSP60a3GKPBOr",
                "identifierInPrimarySource": "Person-639677828",
                "affiliation": [],
                "email": [],
                "familyName": [],
                "fullName": ["davon nicht Herr", "dort"],
                "givenName": ["kann Auge ihr"],
                "isniId": ["https://isni.org/isni/7383473597746886"],
                "memberOf": [],
                "orcidId": ["https://orcid.org/8141-2478-2613-7506"],
                "entityType": "ExtractedPerson",
                "identifier": "cF6RR7tv5CLd9SKJgi3kpr",
                "stableTargetId": "ddTKOu0dKtmUsy6SoGjxBC",
            },
            {
                "hadPrimarySource": "bduBnQ5GcBhRgTGXBRwQ2m",
                "identifierInPrimarySource": "Consent-2933388007",
                "hasConsentStatus": "https://mex.rki.de/item/consent-status-2",
                "hasDataSubject": "buz8FH8lnXEHVVMKqR1sWz",
                "isIndicatedAtTime": "1998-02-16T09:59:31Z",
                "hasConsentType": None,
                "entityType": "ExtractedConsent",
                "identifier": "bdEAuftGbvMz9ZlwTXQzC2",
                "stableTargetId": "h4Pnp6CMxPMUdCNGTSEac",
            },
            {
                "hadPrimarySource": "goPgHfOzReSP60a3GKPBOr",
                "identifierInPrimarySource": "Consent-2933388008",
                "hasConsentStatus": "https://mex.rki.de/item/consent-status-1",
                "hasDataSubject": "buz8FH8lnXEHVVMKqR1sWz",
                "isIndicatedAtTime": "1996-08-27T06:35:42Z",
                "hasConsentType": None,
                "entityType": "ExtractedConsent",
                "identifier": "hKh3lm6FcnBcbfveYMziY1",
                "stableTargetId": "d8wkfwsnaBIixxnRTAz4qE",
            },
            {
                "hadPrimarySource": "bduBnQ5GcBhRgTGXBRwQ2m",
                "identifierInPrimarySource": "AccessPlatform-3427526317",
                "technicalAccessibility": "https://mex.rki.de/item/technical-accessibility-2",
                "endpointDescription": None,
                "endpointType": "https://mex.rki.de/item/api-type-2",
                "endpointURL": {
                    "language": "de",
                    "title": "Hellwig",
                    "url": "https://dorschner.org/",
                },
                "alternativeTitle": [
                    {
                        "value": "Ab Haare möglich einige rund Luft. Bein lustig letzte zusammen Minute Himmel. Auge Hilfe helfen sieht halten Bein Feuer. Brot Name sofort davon Haare gleich Pferd Tür.",
                        "language": "de",
                    }
                ],
                "contact": [
                    "fLbEhIUrS6GbsQZh6DCdsq",
                    "fLbEhIUrS6GbsQZh6DCdsq",
                    "fLbEhIUrS6GbsQZh6DCdsq",
                ],
                "description": [
                    {
                        "value": "Warm gern Geld hoch Tisch. Fliegen sitzen einmal halbe spät vier ging. Stunde Junge holen kalt. Gleich einige schlimm Haus Straße freuen.",
                        "language": "de",
                    }
                ],
                "landingPage": [
                    {
                        "language": "en",
                        "title": "Schleich",
                        "url": "http://www.speer.info/",
                    },
                    {"language": None, "title": None, "url": "https://www.martin.com/"},
                    {"language": None, "title": None, "url": "http://lubs.com/"},
                ],
                "title": [
                    {
                        "value": "Wohl denken nimmt später. Darauf Ferien plötzlich. Heute Mädchen nichts schenken wo heißen. Müde mal hoch Rad. Wollen anfangen einige ließ Welt. Frei Frau kaufen über. Schluss überall er dazu bekommen. Weg Katze blau schicken. Rund schwarz mich ganz gefährlich Ding. Stelle letzte dann er. Fertig legen Hand müde. Suchen Milch nun bis gelb überall Schüler. Ihm ich müssen darauf gern bekommen krank. Gott Wagen spät Papa ein Zahl jetzt. Tür ließ gehen Dorf.",
                        "language": "de",
                    }
                ],
                "unitInCharge": ["fKzLLO4cRXadKyY4PWvnZk", "dGhJF5yUUbBLw8Pe50A7zr"],
                "entityType": "ExtractedAccessPlatform",
                "identifier": "b2xN00S6tkXGieaPuQpJCC",
                "stableTargetId": "fMg1xaQX7nNCka2Dqyek8d",
            },
            {
                "hadPrimarySource": "goPgHfOzReSP60a3GKPBOr",
                "identifierInPrimarySource": "AccessPlatform-3427526318",
                "technicalAccessibility": "https://mex.rki.de/item/technical-accessibility-1",
                "endpointDescription": None,
                "endpointType": None,
                "endpointURL": None,
                "alternativeTitle": [
                    {"value": "Aus Pferd nach rechnen.", "language": "de"},
                    {
                        "value": "Lehrerin offen gab. Las suchen schlafen geben beißen. Dir ihr Kopf Glück. Genau Schüler selbst viel. Dauern heiß Monate Geschenk krank freuen. Den baden wissen vier im. Tier Wiese sechs einmal. Lehrerin lieb Lehrerin gelb. Bleiben hinein klein. Essen Haare baden legen Wald. Fünf Affe Buch davon Freude hat Hunger. Hund möglich Eltern. Gab hier kennen mich trinken oben noch. Ziehen hinein stellen gleich. So endlich Leute leicht Auge dein. Suchen Loch deshalb was dem wieder. Dorf zwei sich Geburtstag glauben tun. Leben Musik lachen also gibt böse im. Stellen sehen schlagen anfangen lachen.",
                        "language": "de",
                    },
                ],
                "contact": [],
                "description": [
                    {
                        "value": "Müde schreiben einmal. Gerade dich schlimm ihm spielen wer. Schuh ließ Monate überall. Lustig gerade dumm weiß glücklich haben Spiel.",
                        "language": "de",
                    }
                ],
                "landingPage": [
                    {
                        "language": "de",
                        "title": "Holsten",
                        "url": "https://hanel.info/",
                    },
                    {
                        "language": None,
                        "title": "Keudel",
                        "url": "https://ruppersberger.net/",
                    },
                    {"language": "en", "title": "Kaul", "url": "http://budig.com/"},
                ],
                "title": [
                    {
                        "value": "Tun fröhlich Monate fröhlich böse. Fest baden weil Familie Fenster Brief gibt Küche. Aber Milch wie hören. Lange er wünschen Geschichte plötzlich weit bleiben. Fehlen sich verkaufen arbeiten werden im und. Über hoch Lehrer. Vorbei Affe Mädchen Buch zum. Klasse Pferd Luft warten dazu. Kurz Eis dabei Dorf klettern fragen einige. Steigen Mädchen sofort dunkel verstehen Wasser. Liegen bis Sache einfach den gestern. Acht heute kaufen freuen gibt. See alle reich Schluss bei damit gehören hängen.",
                        "language": "de",
                    },
                    {
                        "value": "Haus fahren Wohnung lange Klasse. Helfen schreiben weiß sofort Stück trinken Sommer schenken. Von Bild kaufen schreien besser Mutter bei merken. Erde Maus dort verstecken kann schenken schön. Blume Leute Teller Kind lieb klein Bauer. Schaffen gelb an Frage Abend etwas bekommen. Gott beide öffnen Wiese Bein Wiese.",
                        "language": "de",
                    },
                ],
                "unitInCharge": ["dGhJF5yUUbBLw8Pe50A7zr"],
                "entityType": "ExtractedAccessPlatform",
                "identifier": "c3RJPuZSONOCvTUBu7EVyO",
                "stableTargetId": "d8VQbH5SiRKVnloMLdZpFf",
            },
            {
                "hadPrimarySource": "bduBnQ5GcBhRgTGXBRwQ2m",
                "identifierInPrimarySource": "Distribution-1589096615",
                "accessRestriction": "https://mex.rki.de/item/access-restriction-2",
                "issued": "2010-08",
                "accessService": None,
                "license": "https://mex.rki.de/item/license-1",
                "mediaType": "https://mex.rki.de/item/mime-type-17",
                "modified": None,
                "title": [
                    {
                        "value": "Mama treffen Finger groß früher Winter. Apfel richtig nur will. Feuer Bein nein. Neu schreien Auto sicher Hund Fenster etwas. Den zwischen Fenster wenig. Geschenk neu sagen Geschichte gefährlich denken richtig. Ihn früher mit neben selbst. Offen ja Lehrer Zeit. Stark Geschenk im deshalb her so. Feuer Familie verstehen denn kommen Bauer ihn. Mit davon fiel erklären plötzlich werden. Blume sehen einmal Freund schaffen nicht. Einigen Sommer sehen Monat. Erst weinen gegen weit es einmal sicher. Welt Schnee schreien hoch fest Gesicht führen. Man Hand frei Licht Lehrerin braun Zug. Sommer Geschichte Erde genau aus vier sicher. Frei Wiese Nacht. Garten Ende fast mögen finden Milch.",
                        "language": "de",
                    },
                    {
                        "value": "Zehn Pferd leben drehen stellen drehen sechs. Einfach warten zwei Wohnung Luft uns bekommen. Sonne ihn unten. Bein dich etwas klein Baum. Weiß richtig als Mama ihm also See Mann. Also Tier schaffen Tag weiter. Wenn Luft Feuer ob. Tisch freuen hoch helfen hängen. Fiel schnell gefährlich tun Bett zwei Rad. Boden Herr Minute.",
                        "language": "de",
                    },
                ],
                "accessURL": [],
                "downloadURL": [
                    {"language": None, "title": None, "url": "https://www.berger.com/"}
                ],
                "entityType": "ExtractedDistribution",
                "identifier": "vBdpgnvdeiBy0rmripyR3",
                "stableTargetId": "bhGmpHzOtg1XnQsLIBe3KQ",
            },
            {
                "hadPrimarySource": "bduBnQ5GcBhRgTGXBRwQ2m",
                "identifierInPrimarySource": "Distribution-1589096616",
                "accessRestriction": "https://mex.rki.de/item/access-restriction-2",
                "issued": "1997-11",
                "accessService": None,
                "license": None,
                "mediaType": None,
                "modified": None,
                "title": [
                    {
                        "value": "Heute sicher steigen schlecht besser sieben. Ball Junge uns Glück sonst wohnen. Zeigen gesund Onkel Stadt kennen fangen. Wird jeder schenken richtig bis nämlich Straße. Bild die schwer Onkel. Gern Buch aber gelb Hand Freund. Blau jeder mit Licht. Anfangen haben Boden neu. Vielleicht allein ihn spät sehen Monat. Möglich drehen Herz dem gefährlich. Denn draußen früher Geburtstag später hart wirklich. Lehrer nächste klettern dazu acht hinter unser Herr. Kam jeder treffen rechnen nein gefährlich. Wissen er gerade Zahl zeigen ganz Schnee leben. Später gesund fährt stark hat wieder er. Eigentlich sonst anfangen sehr wirklich erzählen Straße.",
                        "language": "de",
                    }
                ],
                "accessURL": [
                    {"language": None, "title": None, "url": "http://herrmann.com/"},
                    {"language": None, "title": None, "url": "https://meister.info/"},
                ],
                "downloadURL": [
                    {"language": None, "title": None, "url": "http://reising.org/"},
                    {"language": None, "title": None, "url": "http://www.sontag.com/"},
                    {
                        "language": "en",
                        "title": "Schmidtke",
                        "url": "https://atzler.com/",
                    },
                ],
                "entityType": "ExtractedDistribution",
                "identifier": "gz6NxdOVmyoQAClgPbifGD",
                "stableTargetId": "hSHtj1xmDjYCs4B5iMk2pW",
            },
            {
                "hadPrimarySource": "goPgHfOzReSP60a3GKPBOr",
                "identifierInPrimarySource": "BibliographicResource-2035724763",
                "accessRestriction": "https://mex.rki.de/item/access-restriction-1",
                "doi": "http://dx.doi.org/98.25071/4611",
                "edition": "groß kochen",
                "issue": "langsam braun",
                "issued": "2006",
                "license": None,
                "pages": None,
                "publicationPlace": None,
                "publicationYear": None,
                "repositoryURL": None,
                "section": "Fenster Tag",
                "volume": None,
                "volumeOfSeries": "Angst sieben",
                "creator": [
                    "ddTKOu0dKtmUsy6SoGjxBC",
                    "ddTKOu0dKtmUsy6SoGjxBC",
                    "buz8FH8lnXEHVVMKqR1sWz",
                ],
                "title": [
                    {
                        "value": "Stunde Maus hat Ende. Fressen vielleicht machen Papa. Himmel hinein dabei Haare klettern dumm schaffen Stunde.",
                        "language": "de",
                    },
                    {
                        "value": "Damit gehen ein Schluss Leute immer. Müssen ins laufen Maus.",
                        "language": "de",
                    },
                ],
                "abstract": [
                    {
                        "value": "Einige unser ob vier. Lassen Eltern heute. Lesen legen schlafen ihr uns laut Bild. Stellen fahren Apfel. Maus drehen klein sie Sonntag. Leute müde freuen mein zwei mehr sofort. Merken zusammen bringen turnen singen geben. Kennen grün tot. Vier damit tief erzählen setzen schlecht. Oma draußen dem erschrecken erschrecken Baum gut fährt. Drehen zu gar müde Woche lieb Weg. Dazu singen ihm Baum. Vogel darin Vogel Fisch schwer. Sommer schauen leben nach Freund. Her Berg Stelle sonst Hunger singen.",
                        "language": "de",
                    }
                ],
                "alternateIdentifier": ["nass Abend"],
                "alternativeTitle": [
                    {
                        "value": "Hoch zehn trinken legen Gesicht führen merken. Mädchen laufen Gott Schiff. Aus offen und. Daran verstehen kam. Zwei ja öffnen. Wald gegen vor vorbei. Reich Fisch heißen weil. Bald offen sehr so.",
                        "language": "de",
                    },
                    {
                        "value": "Ab Land davon Familie. Affe Schnee tot zurück ins davon. Fast bleiben Tag Brot. Wahr dein spielen sitzen davon zwischen. Bein Musik noch dort. See oft dich ihm schlagen viel. Wasser dem daran seit. Wünschen gerade dem Fuß bin Minute. Sechs fünf wird anfangen Vater vorbei erschrecken nie.",
                        "language": "de",
                    },
                    {
                        "value": "Plötzlich Stadt Stelle fest hier. Leicht nein machen Zimmer zurück. Dauern viel mögen hat Schiff Sommer. Also Bein frei auch. Wissen hoch für Schwester. Ins Hund Papa Maus dick bin. Licht erklären Tante Mann.",
                        "language": "de",
                    },
                ],
                "bibliographicResourceType": [],
                "contributingUnit": [
                    "dGhJF5yUUbBLw8Pe50A7zr",
                    "dGhJF5yUUbBLw8Pe50A7zr",
                    "dGhJF5yUUbBLw8Pe50A7zr",
                ],
                "distribution": [],
                "editor": [],
                "editorOfSeries": ["buz8FH8lnXEHVVMKqR1sWz"],
                "isbnIssn": ["alle dort leise"],
                "journal": [
                    {
                        "value": "Singen von Seite damit. Glas Jahr Luft. Schwester nach besser Klasse Hand. Versuchen draußen Ding spielen waschen schaffen dafür. Abend versuchen allein aus Hase wer dürfen. Minute Welt sicher.",
                        "language": "de",
                    },
                    {
                        "value": "Finger wenig Geschichte trinken möglich danach. Dürfen suchen Zug. Bein Kopf führen unter gesund Minute. Heiß ganz Stadt. Feuer Haus Zeit über sprechen Seite merken stellen. Acht ich Fenster groß. Woche ab rennen. Brot mal fertig Leben Baum. Schwer Boden werden Wasser. Ihn Wasser klettern sind nah lesen packen. Bekommen Mädchen jung wünschen fertig frei. Wetter hinein Tür gesund Kind Fenster.",
                        "language": "de",
                    },
                ],
                "keyword": [],
                "language": [],
                "publisher": ["mmJ3BAHHgUz5berBCEzD5"],
                "subtitle": [
                    {
                        "value": "Kommen verlieren bleiben Tante gehen. Auch um gewinnen Blume Finger halbe lieb scheinen. Tragen dunkel Woche verstecken Minutenmir. Fisch kommen suchen dem allein turnen zum. Endlich Hund schlafen Winter. Name Frau Tier an. Endlich plötzlich Teller Blume. Daran wird baden werfen jetzt sehen.",
                        "language": "de",
                    },
                    {
                        "value": "Schiff dumm essen einige Fußball Wald. Nur nächste Polizei. Ihm Milch Baum Fußball dein er war. Fisch öffnen mein alt Herz Schwester langsam. Gott Mutter Gesicht fröhlich anfangen nennen Angst. Bauer zwei mal Sonntag überall. Brief rennen freuen Tisch. Sonne Wissen müssen zum. Haben uns unter durch nehmen Tag. Welt Angst schreiben Wissen Musik Musik.",
                        "language": "de",
                    },
                ],
                "titleOfBook": [
                    {
                        "value": "Arzt bauen nicht am lange ging nur. Glück durch Stück voll. An nass wenn spielen. Viel Wort helfen wird hinter bringen gefährlich. Ziehen Seite Junge Papa. Rad dunkel zeigen Schluss wichtig stehen fallen. Tier heraus selbst Garten dürfen ab schreiben müde. Apfel nicht wer zehn fahren Mutter will.",
                        "language": "de",
                    },
                    {
                        "value": "Fertig zum drehen war. Traurig Sache sein glauben. Freude Fisch Bein. Stehen sechs Auto kalt immer. War Leben Herr Mensch.",
                        "language": "de",
                    },
                ],
                "titleOfSeries": [
                    {
                        "value": "Bekommen unter Garten Geschenk Woche bekommen drei. Doch Berg wird Zimmer gleich zur. Schuh jeder reich nie jung Ball gibt. Ich Minute Wetter Meer richtig. Fertig baden möglich uns dick. Abend blau treffen oder langsam braun.",
                        "language": "de",
                    },
                    {
                        "value": "Zimmer lachen dafür Stadt können. Blume mein einfach war heiß. Auge kalt fressen und Mutter dich schnell. Bis Gott mehr Zeit. Fangen Baum Hilfe los Zahl schlagen will müde.",
                        "language": "de",
                    },
                ],
                "entityType": "ExtractedBibliographicResource",
                "identifier": "dVSHeS2XCpHlb9henfHYTe",
                "stableTargetId": "gmb9sqCNaQKu2SGQJV3Xr4",
            },
            {
                "hadPrimarySource": "bduBnQ5GcBhRgTGXBRwQ2m",
                "identifierInPrimarySource": "BibliographicResource-2035724764",
                "accessRestriction": "https://mex.rki.de/item/access-restriction-2",
                "doi": None,
                "edition": None,
                "issue": None,
                "issued": None,
                "license": None,
                "pages": None,
                "publicationPlace": "über",
                "publicationYear": None,
                "repositoryURL": {
                    "language": "en",
                    "title": "Borner",
                    "url": "https://politz.org/",
                },
                "section": None,
                "volume": None,
                "volumeOfSeries": None,
                "creator": ["ddTKOu0dKtmUsy6SoGjxBC"],
                "title": [
                    {
                        "value": "Nimmt arbeiten früher denken nimmt Schluss Arzt. Schlimm Auto lachen Freund heißen gleich. Geld unten versuchen schlafen Zeitung. Dazu Zeitung kann trinken Freund. Nichts bleiben kam Welt hören lassen. Vogel traurig liegen dir Geld lesen dazu. Wagen Angst Eis Uhr und Fisch klein. Schuh früh treffen lesen Opa daran. Feuer laut kaufen. Nass gut Dorf waschen sind schlecht fünf. Baden verlieren spät essen Fahrrad Familie. Freund ich am spät sonst. Lachen davon dumm. Fallen nimmt wirklich. Ins her grün Flasche krank.",
                        "language": "de",
                    },
                    {
                        "value": "Acht überall zehn uns. Polizei nimmt Geburtstag Buch beide zusammen Papa. Frei schwer was Geburtstag dich. Weg Tag Stück neun richtig. Sechs reiten über Frau. Hängen setzen Name öffnen Tisch glücklich. Schlafen packen Land wir halten. Unter unser vorbei eigentlich braun. Sicher sehen Monat weiß. Reich Fahrrad fallen vom in warm. Frei Garten verkaufen können Vogel Name. Beißen warten leicht Vogel Herz Geburtstag Ball verstecken. Zwischen glücklich zurück was. Denken vielleicht eigentlich Bett ging zurück genau. Aber früh sollen drehen. Leben Sonne blau nur. Hunger Zahl heute Arzt er helfen.",
                        "language": "de",
                    },
                ],
                "abstract": [
                    {
                        "value": "Offen Fisch Schluss. Genau Brot lernen gehören beim aus. Klein Loch Luft jetzt schlagen. Glücklich Weihnachten Hand einigen.",
                        "language": "de",
                    },
                    {
                        "value": "Fallen zur es an lernen. Hat Rad arbeiten vorbei. Spiel oben nein Auto Buch. Auto dafür Katze. Merken lachen dick Schnee. Affe in bringen Loch oben schreiben immer weg. Schüler damit dabei Bruder endlich er fressen. Zeitung halten ich doch merken weiter schlagen.",
                        "language": "de",
                    },
                ],
                "alternateIdentifier": [],
                "alternativeTitle": [],
                "bibliographicResourceType": [
                    "https://mex.rki.de/item/bibliographic-resource-type-3",
                    "https://mex.rki.de/item/bibliographic-resource-type-2",
                    "https://mex.rki.de/item/bibliographic-resource-type-13",
                ],
                "contributingUnit": [],
                "distribution": ["hSHtj1xmDjYCs4B5iMk2pW"],
                "editor": ["buz8FH8lnXEHVVMKqR1sWz"],
                "editorOfSeries": [],
                "isbnIssn": ["jeder sieht", "brauchen"],
                "journal": [],
                "keyword": [
                    {
                        "value": "Wagen Boden Tisch ein Hund. Fröhlich hier verkaufen Stunde Polizei Straße dabei. Zug unter Brot weiter. Stück scheinen eigentlich Herr halten Angst schlafen. Las laut warten kam. Frei gerade Schuh scheinen. See fallen wohnen von Loch schlagen nämlich. Rot Fisch groß schauen hier uns. Was nun alle Wiese lesen kurz Lehrer. Bekommen Leben Feuer zeigen. Letzte Milch heraus. Besser Katze müde blau Lehrerin. Danach Lehrerin reich Brot ihn mit. Trinken Stein wünschen. Warum Musik reich klein Glück. Tot unter ging. Neun schön Hase Kind braun richtig Abend halbe. Merken helfen Bruder Wasser Leben. Buch noch einigen wie hinein klein. Schön Buch ihr Wetter und.",
                        "language": "de",
                    }
                ],
                "language": ["https://mex.rki.de/item/language-3"],
                "publisher": [],
                "subtitle": [
                    {
                        "value": "Schlimm dunkel Tisch Brot. Nie Gesicht wahr immer nimmt Schluss Finger hoch. Zeit Kind davon ihn schon sind. Unten merken gerade setzen Minute. Er oder ganz groß sagen. Lernen Schnee Gesicht dafür. Musik Name wird schwer schaffen waschen. Fünf Vater Tante Luft andere denn Sache. Freuen stark zum Schiff der. Schlimm Garten so Apfel. Bringen voll sich vier sie Monate fertig. Schluss aus Herz im stehen. Zahl Ball sieben Erde voll viel. Turnen weiß Schuh Polizei so merken. Ende sehen Eltern kein zum bekommen Wort selbst. Heraus Jahr mit das. Unter aber krank Winter auch wichtig nennen spielen. Nächste rot leben müde halten Stadt Geschenk fahren. Loch stark dir mein. Blume Fahrrad essen ging Hand etwas.",
                        "language": "de",
                    },
                    {
                        "value": "Dafür groß fest. Wenn fiel auch warm Herz stellen dabei fragen. Andere ließ Spaß lieb letzte. Auge weg Eis lieb Wissen draußen wohnen. Man offen erzählen ohne essen schlafen um. Maus schreien ziehen sprechen plötzlich. Sieht dick klettern ging. Bekommen dürfen Flasche Beispiel. Weiter hören kennen hängen gern. Schaffen Mann Pferd nämlich sofort Nase. Halten leise gab schlafen. Bis Sonne Junge hinter richtig lachen man dein. Stellen vergessen Garten Onkel Geburtstag warm. Wohnung alle vor den auf müde. Maus Wissen gehen sind weil Gott. Weinen verkaufen denken einfach nächste fliegen. Eigentlich Stunde verlieren verkaufen. Kochen mich ganz Bein braun sehen laut. Halten sagen Fenster fünf Beispiel Eltern. Kennen nächste gehen Nase Mutter Hase gern.",
                        "language": "de",
                    },
                    {
                        "value": "Genau stark Schwester. Fuß Hand schenken ging langsam Katze. Laufen böse plötzlich Bild fährt. Warum Ende Seite offen. Was vergessen Oma Tier. Hängen neben weinen Milch mein überall Küche sprechen. Hin warum natürlich verstehen davon leise. Hoch leise rot Vater. Schiff sofort den dir Mädchen Mutter von der. Wir auch Sommer fehlen Freude Fenster rund. Weit hart können Klasse aus Zug gestern.",
                        "language": "de",
                    },
                ],
                "titleOfBook": [
                    {
                        "value": "Hat reich um liegen her weinen. Hängen tot Gott Stein warm acht Wald. Dort besser Platz es einige hart. Rot ihr gibt dabei. Lange neben Polizei ihm fiel darin nehmen. Ohne Arzt müde bauen.",
                        "language": "de",
                    },
                    {
                        "value": "Wohnung neu Musik alt Schüler rot führen. Essen nie Küche las. Turnen neun Stelle tragen ein. Freude sehen glücklich Kind neben fast hier. Also Luft weiter Schiff. Nichts durch Weihnachten neun. Lernen damit las dauern nicht tun Wasser.",
                        "language": "de",
                    },
                ],
                "titleOfSeries": [
                    {
                        "value": "Jahr Winter las sonst gut suchen weit. Braun davon Land fehlen versuchen führen vorbei. Fragen aber sieben acht los Loch. Reich schlagen gern hart die rechnen. Frei Mann Mutter nun zwei. So ihn Stadt las heiß Minutenmir. Zusammen nach spielen hoch dem.",
                        "language": "de",
                    },
                    {
                        "value": "Stadt wo Brief letzte stark Stück nur. Doch Zahl aber. Treffen Küche neun blau.",
                        "language": "de",
                    },
                    {
                        "value": "Bett Arzt heute zur nur los. Bauer spielen dunkel legen alle verkaufen wie ließ. Arbeit neu liegen fehlen drehen. Hand reiten ziehen. Über spät haben hinter. Er als schreien wer mich sehr Monat Name. Wünschen Fuß nie Eltern wie müssen.",
                        "language": "de",
                    },
                ],
                "entityType": "ExtractedBibliographicResource",
                "identifier": "dlldhKisTmp7vsZoPEcI49",
                "stableTargetId": "dGKrGTUAjyDSGiNh7Kzaox",
            },
            {
                "hadPrimarySource": "bduBnQ5GcBhRgTGXBRwQ2m",
                "identifierInPrimarySource": "Activity-2405477151",
                "contact": [
                    "buz8FH8lnXEHVVMKqR1sWz",
                    "buz8FH8lnXEHVVMKqR1sWz",
                    "ddTKOu0dKtmUsy6SoGjxBC",
                ],
                "responsibleUnit": ["fKzLLO4cRXadKyY4PWvnZk", "fKzLLO4cRXadKyY4PWvnZk"],
                "title": [
                    {
                        "value": "Schicken Wasser vielleicht las fragen heute stehen. Scheinen fast gern wohnen liegen. Schreiben fressen für. Schaffen traurig Berg wieder Land haben. Freund Wissen Schiff weiter Lehrerin er selbst. Erschrecken Erde Fisch nämlich Ding. Danach besser wohnen Leben. Sonne mehr auf.",
                        "language": "de",
                    },
                    {
                        "value": "Gerade weg wahr müssen früher ein Sonne Licht. Fahrrad nach Ball Stunde lange mehr. Grün viel sprechen klein dann ließ gern. Alle erzählen nah. Hart viel einige fressen Freund Wohnung. Hat Lehrerin erzählen schreiben heißen verstecken. Kennen möglich zu schlafen Tier. Suchen Minutenmir verstehen Ende Stunde. Küche Affe in dazu. Ließ sechs stark gefährlich Musik Mädchen hoch schlimm. Essen fliegen uns brauchen. Frau Hand mit Teller Gott ihm. Vogel wünschen endlich müssen Rad dunkel. Halbe schaffen laut setzen warum. Nur Kopf wohnen voll Papa wichtig. Oma möglich vergessen beißen Dorf fangen. Reich schwer nichts hinter warten spät.",
                        "language": "de",
                    },
                    {
                        "value": "Zahl ihn einfach an sehr rot wir. Erzählen hoch brauchen will. Mich anfangen hoch Sonne bin fressen die. Gelb Finger fünf offen dauern uns. Arbeit halbe wenig. Krank vom Wiese mich dazu Wissen Meer. Nennen ohne Opa Musik Luft Papa gelb grün. Fangen bleiben gelb ihm die alle Luft. Gott kurz Affe werden. Geburtstag Sonntag Straße nennen stellen merken einigen. Junge Fisch schwimmen plötzlich turnen fahren Bett. Fest Sonne weiter Tante schlafen fünf sie Teller. Einigen Bein dein böse Klasse Kind. Leben Geschichte dick zeigen an Zeit führen darauf. Sehr Fenster dich legen glücklich Geld. Über Kopf Stein gibt unten Woche bauen.",
                        "language": "de",
                    },
                    {
                        "value": "Können Eis unser tot tun dann sonst. Hier ziehen Finger öffnen endlich. Los Zug Küche schicken. Will dir sein sieben mich. Fliegen packen endlich warm. Zeitung Ferien jung als Stelle. Danach steigen Affe auf Stadt lachen nennen heißen. Fisch leicht schlafen gleich.",
                        "language": "de",
                    },
                ],
                "abstract": [
                    {
                        "value": "Darauf fehlen Zug stehen. Tag kalt Eis schlecht halten dazu. Laufen Junge Garten. Einmal einige grün Seite früher Tür Welt. Doch Mama kein tragen mehr Klasse sprechen. Herr einige alt verstehen sprechen fest sechs.",
                        "language": "de",
                    }
                ],
                "activityType": ["https://mex.rki.de/item/activity-type-6"],
                "alternativeTitle": [
                    {
                        "value": "Da Luft beim um das von. Buch Eis Apfel Pferd Himmel. Lustig schreiben Frage hat nie Beispiel wie. Reiten wichtig Tür kurz packen. Mit Winter ihr heiß schwarz treffen. Mal selbst um besser Essen. Braun sehen Mama einmal lustig Fisch offen. Monat mit sieht Sonntag. Halten klettern ruhig Wald draußen eigentlich Hund. Rennen man neun Wiese Weihnachten mich. Unter ihn Onkel trinken Gesicht Feuer. Essen Sonntag Wetter fliegen Wort kaufen hoch.",
                        "language": "de",
                    },
                    {
                        "value": "Also dazu warum acht zur essen gerade Spiel.",
                        "language": "de",
                    },
                ],
                "documentation": [
                    {"language": "de", "title": "Mies", "url": "https://hering.com/"}
                ],
                "end": ["2014-09"],
                "externalAssociate": [],
                "funderOrCommissioner": [],
                "fundingProgram": ["stehen"],
                "involvedPerson": ["ddTKOu0dKtmUsy6SoGjxBC"],
                "involvedUnit": [],
                "isPartOfActivity": [],
                "publication": ["gmb9sqCNaQKu2SGQJV3Xr4", "dGKrGTUAjyDSGiNh7Kzaox"],
                "shortName": [
                    {
                        "value": "Schaffen über Zeit. Müssen bleiben jeder stark deshalb glauben Land sieben. Holen bis leise spielen dauern dazu Monat. Hinter möglich Monate über Luft Feuer Freude. Gesicht werfen bleiben sagen ins grün. Gott reiten Spaß drehen gut traurig.",
                        "language": "de",
                    },
                    {
                        "value": "Kam werfen Fenster schlafen. Sohn verlieren da Ball. Wetter voll tragen allein Schluss da kennen an. Sicher es merken singen mich. Seit den schon alt Name rufen Auge drei. Er schnell fröhlich Kopf wissen. Himmel fröhlich verlieren richtig. Alt Vogel Rad fehlen. Letzte schwarz weil lassen nennen blau. Leicht klettern Wetter am Finger. Gehen leben sieben alt Schiff halbe gegen. Früher bei fragen Hilfe überall zwei wirklich. Familie glücklich hinein dein seit. Wort nun offen fangen fest dick Zeit. Gewinnen mehr erschrecken dem mehr Stunde vielleicht gelb. Weiter jeder weit Spaß reiten Mutter dir. Mit Wissen danach Geschenk. Sehr Sonntag wahr ihn. Gegen sieht traurig Opa sich dick. Buch einfach Sommer nun freuen.",
                        "language": "de",
                    },
                ],
                "start": ["2003"],
                "succeeds": [],
                "theme": ["https://mex.rki.de/item/theme-23"],
                "website": [
                    {"language": None, "title": None, "url": "http://www.hecker.biz/"}
                ],
                "entityType": "ExtractedActivity",
                "identifier": "hJZ8MT6KQ4C0mreUzssjIX",
                "stableTargetId": "hrIu1JNRXLP07KFtl3aAU2",
            },
            {
                "hadPrimarySource": "goPgHfOzReSP60a3GKPBOr",
                "identifierInPrimarySource": "Activity-2405477152",
                "contact": ["dGhJF5yUUbBLw8Pe50A7zr"],
                "responsibleUnit": ["fKzLLO4cRXadKyY4PWvnZk"],
                "title": [
                    {
                        "value": "Hängen auf selbst Herr genau nun. Nein lustig vorbei stark Loch kennen tun. Wagen finden Maus. Mit liegen ohne. Einmal kalt war. Kochen ins nun Ball ließ. Kopf plötzlich bleiben leicht. Springen Winter Schnee dazu. Gesicht andere Klasse Wasser Wasser Wald. Fisch schlimm weinen stellen. Blume Schule Onkel bis. Sehr ziehen nun doch Milch Auge. Weiß Mutter richtig dich.",
                        "language": "de",
                    },
                    {
                        "value": "Immer gelb sonst. Gab eigentlich weg wollen Abend Sonntag allein. Verstecken mein aber Wald fertig natürlich Land. Kann neun acht Wald Fußball oft.",
                        "language": "de",
                    },
                    {
                        "value": "Deshalb reich arbeiten verlieren. Und Geburtstag wer Auto im dürfen sagen. Rot schon müde Musik Zahl sind Hand arbeiten. Gesicht weg tragen Sohn unten.",
                        "language": "de",
                    },
                ],
                "abstract": [
                    {
                        "value": "Gewinnen ihn erzählen erst Bauer. Sein oft zurück lassen schlecht Wiese zurück. Ja Vater Frau müssen oben.",
                        "language": "de",
                    }
                ],
                "activityType": [
                    "https://mex.rki.de/item/activity-type-6",
                    "https://mex.rki.de/item/activity-type-6",
                    "https://mex.rki.de/item/activity-type-6",
                ],
                "alternativeTitle": [],
                "documentation": [
                    {
                        "language": None,
                        "title": "Drubin",
                        "url": "http://www.liebelt.biz/",
                    },
                    {
                        "language": None,
                        "title": "Killer",
                        "url": "http://www.gutknecht.com/",
                    },
                ],
                "end": ["2012-08"],
                "externalAssociate": ["mmJ3BAHHgUz5berBCEzD5", "mmJ3BAHHgUz5berBCEzD5"],
                "funderOrCommissioner": [],
                "fundingProgram": ["Wetter lange", "rechnen"],
                "involvedPerson": [],
                "involvedUnit": ["fKzLLO4cRXadKyY4PWvnZk", "fKzLLO4cRXadKyY4PWvnZk"],
                "isPartOfActivity": [],
                "publication": [],
                "shortName": [
                    {
                        "value": "Fährt antworten zum offen. Möglich Himmel nein vier. Schuh gerade Apfel ihn Lehrerin. Heißen turnen gegen lustig dunkel. Stadt springen Welt kann andere las. Die Glück lassen vor Geld gesund Spaß. Traurig Winter scheinen davon. Klettern Schwester los.",
                        "language": "de",
                    },
                    {"value": "Schlagen nein blau.", "language": "de"},
                ],
                "start": ["1996-02"],
                "succeeds": [],
                "theme": ["https://mex.rki.de/item/theme-22"],
                "website": [
                    {
                        "language": "de",
                        "title": "Fischer",
                        "url": "http://www.killer.net/",
                    },
                    {
                        "language": "de",
                        "title": "Faust",
                        "url": "http://www.schmidtke.info/",
                    },
                ],
                "entityType": "ExtractedActivity",
                "identifier": "eILTyL773cUMXWj3gPHEeg",
                "stableTargetId": "esX9nd1l7S6hjNzB0Ow47c",
            },
            {
                "hadPrimarySource": "goPgHfOzReSP60a3GKPBOr",
                "identifierInPrimarySource": "Resource-2676315731",
                "accessRestriction": "https://mex.rki.de/item/access-restriction-1",
                "accrualPeriodicity": "https://mex.rki.de/item/frequency-2",
                "created": "1997-03-07T20:39:39Z",
                "doi": "https://doi.org/95.4538/1792-1717.ES.4773.32.94.5625577",
                "hasPersonalData": None,
                "license": None,
                "maxTypicalAge": 7556,
                "minTypicalAge": 5711,
                "modified": None,
                "sizeOfDataBasis": None,
                "temporal": None,
                "wasGeneratedBy": "esX9nd1l7S6hjNzB0Ow47c",
                "contact": ["fLbEhIUrS6GbsQZh6DCdsq"],
                "theme": ["https://mex.rki.de/item/theme-23"],
                "title": [
                    {
                        "value": "Mensch Ferien rot besser wie müssen. Dabei nennen gestern braun Katze gelb hängen. Böse Tier Spiel Herr legen richtig bleiben tot. Weihnachten Opa ab der deshalb Mama Himmel Nacht. Erschrecken vorbei Geburtstag Stelle schenken. Schön Welt zwei Name nun Leute. Verlieren bei Weg zehn. Schreien traurig Hand. Vier Ferien schreien hinein Geschenk. Hängen jung es dumm dem Minutenmir. Uns die über Sache dazu beißen. Denn war rennen. Bin Pferd ihr klein. Wie oben Klasse ist tragen hier Familie ist. Nacht Vater fertig. Freund Wetter nach Apfel Arzt lassen Haus. Las schlimm denken seit zum zum springen Kopf. Luft noch finden ohne.",
                        "language": "de",
                    },
                    {
                        "value": "Genau bald Gott springen kurz Licht kurz selbst. Haare Zimmer Garten draußen Schnee neu. Sehr Musik bei Küche drehen.",
                        "language": "de",
                    },
                    {
                        "value": "Anfangen lassen darin Berg Stadt Flasche nächste früher.",
                        "language": "de",
                    },
                    {
                        "value": "Nichts weiter sich richtig. Tante frei schreiben lassen Stunde vielleicht. Zum zehn nein traurig und sitzen. Spaß fertig kam packen erzählen selbst. Ball Freude tragen bald Kopf Land.",
                        "language": "de",
                    },
                ],
                "unitInCharge": ["fKzLLO4cRXadKyY4PWvnZk", "fKzLLO4cRXadKyY4PWvnZk"],
                "accessPlatform": [],
                "alternativeTitle": [
                    {
                        "value": "Wort Schiff auch wenn packen wohnen. Noch rechnen früh fröhlich Stück Frage. Hin Spiel sind selbst natürlich Eltern gleich schlecht. Genau alle Affe Bein. Braun Brot kaufen fahren führen Eis. Ob bauen nein. Wo packen Schule danach verstehen. Her Sonne Polizei suchen. Wasser Schnee fliegen krank. Packen werden Fuß ja. Fragen spät Fuß bei Himmel öffnen. Wohnen Freude kam Leben wichtig. Hinein plötzlich Buch mehr jeder nur Bruder kam. Brot beißen möglich Flasche kaufen. Deshalb Papa wieder rufen freuen schon weil. Abend Schule Spaß lange Hunger sehr tief.",
                        "language": "de",
                    }
                ],
                "anonymizationPseudonymization": [
                    "https://mex.rki.de/item/anonymization-pseudonymization-1"
                ],
                "conformsTo": ["müde", "wirklich hart glauben", "gehören hängen bauen"],
                "contributingUnit": [
                    "fKzLLO4cRXadKyY4PWvnZk",
                    "dGhJF5yUUbBLw8Pe50A7zr",
                ],
                "contributor": ["buz8FH8lnXEHVVMKqR1sWz", "buz8FH8lnXEHVVMKqR1sWz"],
                "creator": ["buz8FH8lnXEHVVMKqR1sWz", "ddTKOu0dKtmUsy6SoGjxBC"],
                "description": [
                    {
                        "value": "Kalt einmal arbeiten andere über fertig Himmel Stein. Leicht Zahl tief zehn also. Bauen stehen auf damit Tante rund. Wir vom Maus sicher Fenster beißen selbst. Reich können wenn aus schreiben dauern dumm. Weil traurig braun. Drei neben tot Bruder Lehrerin Stunde fahren suchen. Gleich um sich Hund dann Schiff. Dein und Stelle Sonne nehmen glauben dann. Sohn drehen heiß dort. Wetter auch Brief Vater nun.",
                        "language": "de",
                    }
                ],
                "distribution": ["bhGmpHzOtg1XnQsLIBe3KQ"],
                "documentation": [],
                "externalPartner": ["mmJ3BAHHgUz5berBCEzD5", "mmJ3BAHHgUz5berBCEzD5"],
                "hasLegalBasis": [],
                "icd10code": ["Feuer Wissen", "nächste", "Arzt nicht ein"],
                "instrumentToolOrApparatus": [],
                "isPartOf": [],
                "keyword": [],
                "language": [],
                "loincId": ["https://loinc.org/LA40312-7"],
                "meshId": [],
                "method": [
                    {
                        "value": "Flasche nächste warum Kind Brot bald. Durch stark lassen dafür waschen sechs nehmen. Immer sieht weg fast böse fangen. Sich vom her fahren können. Ball jung Essen nimmt dürfen helfen klettern. Oma ziehen Erde darin schlimm. Fährt da krank immer. Fahren Garten Ding Stelle wohl weinen.",
                        "language": "de",
                    }
                ],
                "methodDescription": [
                    {
                        "value": "Lachen kommen als Jahr von Finger jung Fahrrad. Wissen Welt weg im Auge zwei Seite lernen. Mama lange Straße traurig Kind fest. Sitzen Schluss hören etwas halten fest überall. Langsam gehen schreien. Sieht andere Klasse Spiel vor hängen steigen. Offen schreien ab oft hier schlimm. Apfel ist Freund wissen mein Stelle stehen. Ob glauben wieder lieb. Warten da hat nämlich hat dunkel. Schuh Monate spielen dem Glas Meer Rad. Vom schreien sitzen sollen Geschichte weg lernen ins. Beide groß laufen.",
                        "language": "de",
                    }
                ],
                "populationCoverage": [
                    {
                        "value": "Haus schaffen Schluss Zimmer Wald einigen war blau. Weg gab Wohnung böse früher schnell. Dich erklären Woche an im schicken wieder. Groß Katze Platz ganz bei vom fehlen. Platz ihm glücklich plötzlich gesund nehmen schnell. Katze gehen dauern steigen dumm Frau lassen. Spaß aus haben klein draußen. Schwer fünf Zeitung uns. Doch damit zwischen müssen. Frei einfach Musik Arbeit gern Gott. Kein ziehen man dann andere wieder.",
                        "language": "de",
                    },
                    {
                        "value": "Ding zeigen Mensch letzte Polizei Garten. Zeigen so zwischen rund Stunde. Gleich da lange auch scheinen damit. Hund unser kaufen. Gesund nimmt Eltern lustig.",
                        "language": "de",
                    },
                ],
                "publication": [],
                "publisher": ["bAl5wuuzs5f5borMwYpgEJ"],
                "qualityInformation": [
                    {
                        "value": "Licht bauen rund hoch schwimmen ja hier allein. Haare Zimmer vor blau sein Katze. Suchen gab Tag zeigen ins Wasser. Um neu denken gegen. Zurück beim wollen sofort blau ich Welt. Dunkel kommen Mädchen Welt er Tür. Minute auf trinken um gehen. Sicher fallen Stein die gefährlich Papa Opa. Reich was im Leben. Angst Sonne Leben Zeit neu. Nah Mutter voll oft Land Schluss. Helfen Seite also Frau. Bauer Geschichte Wagen dazu Dorf früh immer weiß. Auto ganz packen Berg Eltern oft. Neu draußen Gesicht gern.",
                        "language": "de",
                    }
                ],
                "resourceCreationMethod": [
                    "https://mex.rki.de/item/resource-creation-method-6",
                    "https://mex.rki.de/item/resource-creation-method-6",
                ],
                "resourceTypeGeneral": [
                    "https://mex.rki.de/item/resource-type-general-18"
                ],
                "resourceTypeSpecific": [],
                "rights": [
                    {
                        "value": "Bringen die Fahrrad einigen. Nur Tür lustig Mama Milch sofort dabei. Wenig damit dauern also grün schlimm dem. Warum neu Sonne glücklich schwarz Angst. Fahrrad verstehen denken Familie. Erzählen Zug ist Sommer. Besser spielen sofort auf. Apfel richtig nehmen kurz. Auf gefährlich alle brauchen daran. Erschrecken Weg einfach Haare Blume unter wird wo. Haare verstecken spät nun lernen Bein. Sommer mal schwimmen wünschen dumm so aber sehen. Wer verkaufen lachen schnell einfach. Eigentlich haben Herr essen.",
                        "language": "de",
                    }
                ],
                "spatial": [],
                "stateOfDataProcessing": [
                    "https://mex.rki.de/item/data-processing-state-1"
                ],
                "entityType": "ExtractedResource",
                "identifier": "feF2TTopzIO8ybp0gwiBwD",
                "stableTargetId": "dZ0czpfY8eWT7bmIoW91Xr",
            },
            {
                "hadPrimarySource": "goPgHfOzReSP60a3GKPBOr",
                "identifierInPrimarySource": "Resource-2676315732",
                "accessRestriction": "https://mex.rki.de/item/access-restriction-2",
                "accrualPeriodicity": "https://mex.rki.de/item/frequency-2",
                "created": "2012-02-24T21:48:05Z",
                "doi": "http://dx.doi.org/73.86463/1752",
                "hasPersonalData": "https://mex.rki.de/item/personal-data-1",
                "license": "https://mex.rki.de/item/license-1",
                "maxTypicalAge": None,
                "minTypicalAge": 9675,
                "modified": None,
                "sizeOfDataBasis": "glücklich",
                "temporal": "1999-07-12",
                "wasGeneratedBy": "esX9nd1l7S6hjNzB0Ow47c",
                "contact": [
                    "d0uKlX2GlXKqxkRdcyj7Q2",
                    "d0uKlX2GlXKqxkRdcyj7Q2",
                    "d0uKlX2GlXKqxkRdcyj7Q2",
                    "d0uKlX2GlXKqxkRdcyj7Q2",
                ],
                "theme": [
                    "https://mex.rki.de/item/theme-11",
                    "https://mex.rki.de/item/theme-11",
                ],
                "title": [
                    {
                        "value": "Antworten sich turnen Lehrerin. Zeit Herz schenken. Gegen oben sollen rot. Sonst Stein Erde rot leben frei Gott. Merken Woche Apfel jeder Welt heute gerade. Kann arbeiten durch richtig spät war. Leben dazu schwimmen laut. Mögen leben Minute Meer. Land verstecken lesen hin legen Zeit nächste. Sonntag fünf bei neben darin hat. Anfangen warum glücklich braun Gott halten Haare rot.",
                        "language": "de",
                    },
                    {
                        "value": "Nehmen weiter beim gehen gleich. Maus arbeiten rennen. Mögen Weihnachten freuen Schnee natürlich zur. Möglich Gesicht braun laut schlimm erschrecken. Leise Sonne Glück nur Hase Junge Lehrer. Unser Mann für Auto. Hund Familie sie Blume unter brauchen glauben.",
                        "language": "de",
                    },
                ],
                "unitInCharge": [
                    "fKzLLO4cRXadKyY4PWvnZk",
                    "dGhJF5yUUbBLw8Pe50A7zr",
                    "fKzLLO4cRXadKyY4PWvnZk",
                    "dGhJF5yUUbBLw8Pe50A7zr",
                ],
                "accessPlatform": ["d8VQbH5SiRKVnloMLdZpFf", "d8VQbH5SiRKVnloMLdZpFf"],
                "alternativeTitle": [
                    {
                        "value": "Kind Sonne eigentlich alt schreien vielleicht nach. Rot treffen Winter freuen packen. Verlieren weinen Wiese sehr Baum er kommen fest. Unten ihm viel weiter Onkel. Freuen für doch schlagen Ferien fertig. Eis finden tun Kopf. Zwei in weinen Erde früh Angst steigen. Rennen durch verlieren wünschen laufen. Bauen Wasser gerade. Vogel Weg hören Hase rennen. Hoch uns unten Ferien. Selbst wieder los turnen wo wieder Flasche.",
                        "language": "de",
                    }
                ],
                "anonymizationPseudonymization": [],
                "conformsTo": ["hinein Tier sofort", "fertig wirklich ja"],
                "contributingUnit": [],
                "contributor": ["ddTKOu0dKtmUsy6SoGjxBC"],
                "creator": ["buz8FH8lnXEHVVMKqR1sWz", "ddTKOu0dKtmUsy6SoGjxBC"],
                "description": [],
                "distribution": [
                    "hSHtj1xmDjYCs4B5iMk2pW",
                    "bhGmpHzOtg1XnQsLIBe3KQ",
                    "hSHtj1xmDjYCs4B5iMk2pW",
                ],
                "documentation": [
                    {"language": None, "title": None, "url": "https://seidel.com/"}
                ],
                "externalPartner": ["mmJ3BAHHgUz5berBCEzD5"],
                "hasLegalBasis": [
                    {
                        "value": "Vogel verstehen Hilfe weiter hinein Zimmer Frage. Will sofort geben versuchen. Wenig Oma Wagen Sonntag. Steigen früh Bett spät Leben setzen. Schön vorbei tragen treffen schon lesen. In sein merken gesund Schnee Sommer. Ohne Stein dazu Hund. Gehen müssen da Fisch Schwester sonst Rad das. Schlafen Stein Geschenk lange selbst frei schlagen tun.",
                        "language": "de",
                    },
                    {
                        "value": "Ihr ein vorbei wirklich. Fallen Wissen werfen Polizei offen weinen Zeitung. Frei verkaufen baden nämlich neun. Bleiben Haus Wissen damit. Jung wer Klasse schaffen danach Schwester einmal. Für legen dauern verstehen neun. Sehr einmal eigentlich Stein Angst. Ferien kennen Finger Name Polizei.",
                        "language": "de",
                    },
                ],
                "icd10code": ["wahr"],
                "instrumentToolOrApparatus": [
                    {
                        "value": "Setzen Frage brauchen Bauer ging Ferien. Über wir zehn neu sind aus. Lernen Herz Zimmer sagen davon. Auch dürfen drei Leben gleich kurz bleiben. Jetzt frei genau zu leicht Zeitung fährt. Wetter führen vorbei nun Sommer fressen. Jung dort setzen Leben wo Minutenmir.",
                        "language": "de",
                    },
                    {
                        "value": "Deshalb fast arbeiten Frau Schuh. Dafür waschen legen kam zusammen Monate. Leben unter Vogel daran Boden voll eigentlich. Singen treffen Musik rufen. Name Zeit lustig möglich im. Finden Lehrerin Boden gewinnen. Schicken schicken hinein neben Woche. Dick Bild Tür richtig Klasse wohnen. Rennen gibt Auge oben Kind Wissen überall weinen.",
                        "language": "de",
                    },
                    {
                        "value": "Stück führen Straße. Merken nicht Blume Frage. Zahl vier bleiben verlieren Dorf. Fünf sitzen reich dann leben jetzt.",
                        "language": "de",
                    },
                ],
                "isPartOf": [],
                "keyword": [],
                "language": ["https://mex.rki.de/item/language-2"],
                "loincId": ["https://loinc.org/LA16592-6"],
                "meshId": [],
                "method": [
                    {
                        "value": "Machen Polizei bin kurz Bett Stück gefährlich. Schiff gefährlich Fisch ja. Auto Berg Auge Buch ist Boden rot. Tier halbe Nase sonst also sicher kommen. Unten Auge im Schuh. Nass Klasse Sache oder Freude zehn.",
                        "language": "de",
                    },
                    {
                        "value": "Besser Zug Frau schaffen. Man aber damit dumm Land schlimm. Sehr gegen Tier nächste schlimm wünschen schauen suchen. Darauf als da also. Versuchen erzählen Polizei Rad jung.",
                        "language": "de",
                    },
                ],
                "methodDescription": [
                    {
                        "value": "In damit Weihnachten sich Katze lassen alt. Nase Erde mögen Uhr baden acht ohne. Zu vergessen setzen nennen Minutenmir ganz. Holen lieb sehen will weil Vogel fehlen. Machen in gern beide. Warten damit oben plötzlich kein Junge. Gab Eis Gott fehlen. Nass schlafen werden verlieren schwimmen. Dauern gar schwer wollen Uhr schaffen selbst. Stunde unter oben führen sind rund das. Ding Leben Stelle wirklich legen.",
                        "language": "de",
                    }
                ],
                "populationCoverage": [],
                "publication": [],
                "publisher": ["bAl5wuuzs5f5borMwYpgEJ", "bAl5wuuzs5f5borMwYpgEJ"],
                "qualityInformation": [
                    {
                        "value": "Familie Monate laufen zwischen Stadt Spaß Spaß mit. Verlieren von nichts Brot. Wetter Junge klein. Ziehen später noch versuchen. Treffen auf Bruder ohne. Weiter Ding gar kam lesen. Die Hase tragen daran. Kalt ihr sonst finden Teller. Für drei Freude schwimmen ihr ohne. Dich so in fangen Stück.",
                        "language": "de",
                    }
                ],
                "resourceCreationMethod": [
                    "https://mex.rki.de/item/resource-creation-method-7"
                ],
                "resourceTypeGeneral": [
                    "https://mex.rki.de/item/resource-type-general-13"
                ],
                "resourceTypeSpecific": [],
                "rights": [
                    {
                        "value": "Bei Sohn richtig Pferd Weihnachten Bauer. Zu Stunde vorbei Zeitung immer warm schreiben von. Weit Affe Fußball machen wer schreiben. Platz warten oder nämlich kann groß tief. Sprechen wahr bin Maus Himmel Schüler. Loch mehr essen Licht. Los sofort doch Angst stellen. Frau Tür gehen ins lesen Seite unser Frau. Haben Auto rechnen darauf Geburtstag setzen Kind. Tag zusammen Mutter waschen oft.",
                        "language": "de",
                    }
                ],
                "spatial": [
                    {
                        "value": "Schwimmen Loch Finger gewinnen. Eis springen Klasse der ist ihm Blume ist. Land kaufen heiß lustig. Monat springen von Nase Opa über schlecht Wissen. Treffen denn darin schwer das Schuh. Damit es Küche sieht wirklich heiß Woche. Verlieren böse heiß gab Luft damit gehen helfen. Laufen Stein denken natürlich Auto. Warten Essen tragen laut daran. Bein Auge langsam Geld Kind gleich. Lachen sechs nie Sommer lernen kochen. Rund deshalb sein bleiben oder oder Hilfe. Gleich aus Sache kann Fuß blau.",
                        "language": "de",
                    }
                ],
                "stateOfDataProcessing": [],
                "entityType": "ExtractedResource",
                "identifier": "g0CSnlxrWg97NhCfd7oisy",
                "stableTargetId": "8ee5zhCJ8v9InQQLnqhm7",
            },
            {
                "hadPrimarySource": "bduBnQ5GcBhRgTGXBRwQ2m",
                "identifierInPrimarySource": "VariableGroup-1476619723",
                "containedBy": ["dZ0czpfY8eWT7bmIoW91Xr"],
                "label": [
                    {
                        "value": "Wasser danach will eigentlich alt stark finden. Katze Tante nie Hund Auge vielleicht an Junge. Andere machen Herz Blume zu mehr. Denn Frage bleiben gehören hören. Bauer Wort eigentlich Schwester. Himmel Geschichte ihn gestern her. Bald fest nennen Ferien. Sitzen hier dafür Baum Kopf hören lassen.",
                        "language": "de",
                    }
                ],
                "entityType": "ExtractedVariableGroup",
                "identifier": "haiomJ29I0XKjjsaTBbFrU",
                "stableTargetId": "bVMB6gaKPdubOMJTqRpJlO",
            },
            {
                "hadPrimarySource": "goPgHfOzReSP60a3GKPBOr",
                "identifierInPrimarySource": "VariableGroup-1476619724",
                "containedBy": ["dZ0czpfY8eWT7bmIoW91Xr", "8ee5zhCJ8v9InQQLnqhm7"],
                "label": [
                    {
                        "value": "Kaufen Vater leicht später denken mehr. Später Maus ins weiß. Auf Tier Wort ob reich draußen Baum. Früh Flasche schenken überall unten es unser. Aus sich Musik gefährlich hinter. Tür sehen überall Wagen. Noch Wohnung Geschenk schicken glücklich. Zu Schule früh rot Gesicht. Kochen neu also schlimm plötzlich. Nimmt aber Affe. Ihn waschen schreien Sommer. Bild vorbei rechnen Meer was Tisch Katze. Haare Tier tun halten.",
                        "language": "de",
                    }
                ],
                "entityType": "ExtractedVariableGroup",
                "identifier": "grkoZoTq1EYRLTQzGaEHqa",
                "stableTargetId": "hxfWD0ULIkgW4gXyB10ck9",
            },
            {
                "hadPrimarySource": "bduBnQ5GcBhRgTGXBRwQ2m",
                "identifierInPrimarySource": "Variable-4015597000",
                "codingSystem": None,
                "dataType": "wissen Eltern",
                "label": [
                    {"value": "Eis Tisch oder Tante Fahrrad zu.", "language": "de"}
                ],
                "usedIn": ["8ee5zhCJ8v9InQQLnqhm7", "dZ0czpfY8eWT7bmIoW91Xr"],
                "belongsTo": ["bVMB6gaKPdubOMJTqRpJlO"],
                "description": [
                    {
                        "value": "Richtig Mutter schicken Mutter beim. Sieht darin Wiese verstecken. Sein früher Vater an zeigen scheinen nur. Wald ab ja schaffen Rad verlieren Gesicht. Früh schenken krank hart Sohn war. Aus wenn Mama gern hin. Bild möglich andere setzen Zimmer ins.",
                        "language": "de",
                    },
                    {
                        "value": "Dort Geld offen braun langsam. Nehmen offen Glas.",
                        "language": "de",
                    },
                    {
                        "value": "Lachen wollen Uhr traurig. Stelle wirklich sprechen ob Sonne offen Nacht. Ziehen wenig heraus warum. Später bei Stück Ball. Offen beim scheinen Monat Garten Herz brauchen sitzen. Vier stehen oder Kind Kopf. Wahr nämlich Glück Winter später Klasse. Natürlich Buch allein Schwester nehmen unser.",
                        "language": "de",
                    },
                ],
                "valueSet": ["ist laufen", "offen kein einigen"],
                "entityType": "ExtractedVariable",
                "identifier": "feuskumuuW8TEzAMStaU7o",
                "stableTargetId": "hpt4K8wJxnjFo6AM5PQ7kI",
            },
            {
                "hadPrimarySource": "goPgHfOzReSP60a3GKPBOr",
                "identifierInPrimarySource": "Variable-4015597001",
                "codingSystem": None,
                "dataType": None,
                "label": [
                    {
                        "value": "Geld war sprechen der gelb glücklich. Zeigen ließ rennen denn fertig tragen. Versuchen in Ende er weit stark glücklich. Schnee Zug neun gibt Nacht wünschen nächste. Gewinnen Bruder vorbei weiter Lehrer wer. Schluss krank essen langsam sonst. Tante her holen trinken acht alle. Antworten dafür ganz Eltern Nase Sonntag. Ziehen endlich fünf. War Schüler Glück Minutenmir dürfen. Durch frei bringen. Herr stehen da will. Klein heiß heißen drei machen. Wald Wagen beim denn werfen stellen Eis heraus. Geschenk Jahr Opa wer Sommer nass. Garten deshalb Finger ohne Fußball darin letzte. Warm erzählen Bild können rufen beide letzte singen.",
                        "language": "de",
                    },
                    {
                        "value": "Kopf Gesicht helfen Schnee schwimmen legen Freund. Packen steigen Rad erst waschen Kind. Hinter gehören warten möglich will. Frage bin sitzen Garten Loch fast. Kopf ob ja. Jetzt Haus Lehrerin früh andere kam Freude. Hand Bild unten Licht. Stark Wetter braun beißen. Mensch Glas schauen. Gegen Tisch Schiff früher Schuh. Überall sitzen sicher weiß. Spaß fährt groß. Wissen traurig Sohn allein reich zusammen. Vier fangen See erzählen.",
                        "language": "de",
                    },
                    {
                        "value": "Stark Sohn See kochen Stein genau einigen Spaß. Erzählen kennen Monat helfen nach das. Leicht richtig haben. Immer schlafen Hunger springen Maus. Nass Mädchen Apfel dem Zimmer öffnen. Ohne nicht nimmt Vogel zu erklären hin. Hier lachen gewinnen Wort packen gehen drei. Dürfen tun nicht anfangen endlich weiß hier. Fenster richtig Beispiel Bauer ließ. Gesund weiter hin zusammen.",
                        "language": "de",
                    },
                ],
                "usedIn": [
                    "8ee5zhCJ8v9InQQLnqhm7",
                    "dZ0czpfY8eWT7bmIoW91Xr",
                    "8ee5zhCJ8v9InQQLnqhm7",
                    "dZ0czpfY8eWT7bmIoW91Xr",
                ],
                "belongsTo": ["hxfWD0ULIkgW4gXyB10ck9"],
                "description": [
                    {
                        "value": "Früher acht denken beide holen Tante lustig. Wagen kochen nach setzen für wahr klein. Hin treffen vergessen ihr eigentlich. Seite klettern weiter einfach mögen hinein wissen fest. Früh oft ganz wenig. Weit ihm Sommer dann Haus. Lernen spät ich dort. Lesen kommen bekommen Schnee singen.",
                        "language": "de",
                    }
                ],
                "valueSet": ["vier Ende", "Arzt alle"],
                "entityType": "ExtractedVariable",
                "identifier": "ZjAPIDwREni1VmPOIdDBm",
                "stableTargetId": "hyF76OwzrFaZScxJZLNX17",
            },
        ]
    }
