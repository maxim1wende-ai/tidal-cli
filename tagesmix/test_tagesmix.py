"""Rauchtest für tagesmix.py – ausführen mit: python3 tagesmix/test_tagesmix.py"""
import json, os, subprocess, sys, tempfile, unittest, datetime as dt

HIER = os.path.dirname(os.path.abspath(__file__))
SKRIPT = os.path.join(HIER, "tagesmix.py")
sys.path.insert(0, HIER)
import tagesmix  # noqa: E402


def lauf(*args, cwd):
    r = subprocess.run([sys.executable, SKRIPT, *args], cwd=cwd, capture_output=True, text=True)
    if r.returncode != 0:
        raise AssertionError(f"tagesmix.py {' '.join(args)} scheiterte:\n{r.stderr}")
    return r.stdout


class TagesmixTest(unittest.TestCase):
    def test_kategorien_und_ids(self):
        self.assertEqual(tagesmix.cats_on(dt.date(2026, 11, 22)),
                         ["Sport", "Soul & Funk", "Indie & Alternative", "Essen mit Gästen"])
        self.assertEqual(tagesmix.cats_on(dt.date(2026, 11, 23)),
                         ["Hard Rock & Metal", "Pop seit 2000", "Party · Oldies", "Jazz & Swing"])
        self.assertEqual(tagesmix.docid("2026-11-23", "Sport"), "79738876-sport")
        self.assertEqual(tagesmix.slug("Soul & Funk"), "soul-und-funk")

    def test_ablauf_offen_plan_eintrag(self):
        with tempfile.TemporaryDirectory() as tmp:
            mixdir = os.path.join(tmp, "export", "data", "tagesmix", "mixe")
            os.makedirs(mixdir)
            gestern = tagesmix.docid("2026-11-23", "Sport")
            json.dump({"datum": "2026-11-23", "kategorie": "Sport", "slug": "sport",
                       "ansatz": "standard", "abwechslung": "kern",
                       "songsStamm": 15, "songsAbwechslung": 5,
                       "stammIds": [str(i) for i in range(1, 16)],
                       "abwechslungIds": [str(i) for i in range(16, 21)],
                       "playlistGroesse": 60},
                      open(os.path.join(mixdir, gestern + ".json"), "w", encoding="utf-8"))

            offen = json.loads(lauf("offen", "--export", "export", "--heute", "2026-11-24", cwd=tmp))
            self.assertEqual([m["id"] for m in offen], [gestern])
            self.assertEqual(len(offen[0]["trackIds"].split(",")), 20)

            json.dump({gestern: [["3", "Song A – Künstler"], ["17", "Song B – Künstler"]]},
                      open(os.path.join(tmp, "treffer.json"), "w", encoding="utf-8"))
            out = lauf("plan", "--export", "export", "--heute", "2026-11-24", "--treffer", "treffer.json",
                       "--kat", "Soul & Funk=40", "--kat", "Sport", "--out", "out", cwd=tmp)
            self.assertIn("AUSWERTUNG 2026-11-23 Sport: 2 Treffer (Stamm 1, Abwechslung 1)", out)
            plan = json.load(open(os.path.join(tmp, "out", "plan.json"), encoding="utf-8"))
            self.assertEqual([p["kategorie"] for p in plan], ["Soul & Funk", "Sport"])
            self.assertEqual(plan[0]["playlistGroesse"], 40)
            writes = json.load(open(os.path.join(tmp, "out", "writes_auswertungen.json"), encoding="utf-8"))
            self.assertEqual([w["doc_id"] for w in writes], [gestern])

            zeile = json.loads(lauf("eintrag", "--plan", "out/plan.json", "--kat", "Soul & Funk", "--playlist", "P1",
                                    "--stamm", "1,2,3", "--abw", "4", "--out", "out", cwd=tmp))
            self.assertEqual(zeile["collection"], "data/tagesmix/mixe")
            self.assertEqual(zeile["doc_id"], tagesmix.docid("2026-11-24", "Soul & Funk"))
            doc = json.load(open(zeile["file_path"], encoding="utf-8"))
            self.assertEqual((doc["songsStamm"], doc["songsAbwechslung"], doc["playlistId"]), (3, 1, "P1"))


if __name__ == "__main__":
    unittest.main(verbosity=2)
